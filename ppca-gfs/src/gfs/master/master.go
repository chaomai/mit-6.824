package master

import (
	"net"
	"net/rpc"
	"time"

	"gfs"

	log "github.com/Sirupsen/logrus"
)

// Master Server struct
type Master struct {
	address    gfs.ServerAddress // master server address
	serverRoot string            // path to metadata storage
	l          net.Listener
	shutdown   chan struct{}

	nm  *namespaceManager
	cm  *chunkManager
	csm *chunkServerManager
}

// NewAndServe starts a master and returns the pointer to it.
func NewAndServe(address gfs.ServerAddress, serverRoot string) *Master {
	m := &Master{
		address:    address,
		serverRoot: serverRoot,
		shutdown:   make(chan struct{}),
	}

	m.nm = newNamespaceManager()
	m.cm = newChunkManager()
	m.csm = newChunkServerManager()

	rpcs := rpc.NewServer()
	rpcs.Register(m)
	l, e := net.Listen("tcp", string(m.address))
	if e != nil {
		log.Fatal("listen error:", e)
		log.Exit(1)
	}
	m.l = l

	// RPC Handler
	go func() {
		for {
			select {
			case <-m.shutdown:
				return
			default:
			}
			conn, err := m.l.Accept()
			if err == nil {
				go func() {
					rpcs.ServeConn(conn)
					conn.Close()
				}()
			} else {
				log.Fatal("accept error:", err)
				log.Exit(1)
			}
		}
	}()

	// Background Task
	go func() {
		ticker := time.Tick(gfs.BackgroundInterval)
		for {
			select {
			case <-m.shutdown:
				return
			default:
			}
			<-ticker

			err := m.BackgroundActivity()
			if err != nil {
				log.Fatal("Background error ", err)
			}
		}
	}()

	log.Infof("Master is running now. addr = %v, root path = %v", address, serverRoot)

	return m
}

// Shutdown shuts down master
func (m *Master) Shutdown() {
	close(m.shutdown)
}

// BackgroundActivity does all the background activities:
// dead chunkserver handling, garbage collection, stale replica detection, etc
func (m *Master) BackgroundActivity() error {
	return nil
}

// RPCHeartbeat is called by chunkserver to let the master know that a chunkserver is alive.
// Lease extension request is included.
func (m *Master) RPCHeartbeat(args gfs.HeartbeatArg, reply *gfs.HeartbeatReply) error {
	m.csm.Heartbeat(args.Address)

	for _, v := range args.LeaseExtensions {
		if err := m.cm.ExtendLease(v, args.Address); err != nil {
			return err
		}
	}

	return nil
}

// RPCGetPrimaryAndSecondaries returns lease holder and secondaries of a chunk.
// If no one holds the lease currently, grant one.
func (m *Master) RPCGetPrimaryAndSecondaries(args gfs.GetPrimaryAndSecondariesArg, reply *gfs.GetPrimaryAndSecondariesReply) error {
	l, err := m.cm.GetLeaseHolder(args.Handle)

	if err != nil {
		log.Errorf("RPCGetPrimaryAndSecondaries, err[%s]", err)
		return err
	}

	log.Infof("RPCGetPrimaryAndSecondaries, lease[%+v]", l)
	reply.Primary = l.Primary
	reply.Secondaries = l.Secondaries
	reply.Expire = l.Expire
	reply.Version = l.Version

	return nil
}

// RPCGetReplicas is called by client to find all chunkservers that hold the chunk.
func (m *Master) RPCGetReplicas(args gfs.GetReplicasArg, reply *gfs.GetReplicasReply) error {
	loc, err := m.cm.GetReplicas(args.Handle)
	if err != nil {
		log.Errorf("RPCGetReplicas, err[%s]", err)
		return err
	}

	for _, e := range loc.GetAll() {
		addr := e.(gfs.ServerAddress)
		reply.Locations = append(reply.Locations, addr)
	}
	return nil
}

// RPCCreateFile is called by client to create a new file
func (m *Master) RPCCreateFile(args gfs.CreateFileArg, replay *gfs.CreateFileReply) error {
	return m.nm.Create(args.Path)
}

// RPCMkdir is called by client to make a new directory
func (m *Master) RPCMkdir(args gfs.MkdirArg, replay *gfs.MkdirReply) error {
	return m.nm.Mkdir(args.Path)
}

// RPCGetFileInfo is called by client to get file information
func (m *Master) RPCGetFileInfo(args gfs.GetFileInfoArg, reply *gfs.GetFileInfoReply) error {
	return nil
}

// RPCGetChunkHandle returns the chunk handle of (path, index).
// If the requested index is bigger than the number of chunks of this path by exactly one, create one.
func (m *Master) RPCGetChunkHandle(args gfs.GetChunkHandleArg, reply *gfs.GetChunkHandleReply) error {
	if err := m.nm.Create(args.Path); err == nil || err == gfs.ErrFileExists {
		log.Infof("RPCGetChunkHandle, file[%s], err[%s]", args.Path, err)
	} else {
		return err
	}

	dirParts, leafName, err := m.nm.dirAndLeafName(args.Path)
	if err != nil {
		return err
	}

	parentNode, err := m.nm.lockParents(dirParts)
	if err != nil {
		return err
	}

	defer m.nm.unlockParents(dirParts)

	parentNode.RLock()
	defer parentNode.RUnlock()

	fileNode, ok := parentNode.children[leafName]
	if !ok {
		log.Errorf("RPCGetChunkHandle, file[%s], err[%s]", args.Path, gfs.ErrFileNotExists)
		return gfs.ErrFileNotExists
	}

	if fileNode.isDir {
		log.Errorf("RPCGetChunkHandle, path[%s], err[%s]", args.Path, gfs.ErrPathIsNotFile)
		return gfs.ErrPathIsNotFile
	}

	fileNode.Lock()
	defer fileNode.Unlock()

	if int64(args.Index) < fileNode.chunks {
		ch, err := m.cm.GetChunk(args.Path, args.Index)

		if err != nil {
			log.Errorf("RPCGetChunkHandle, err[%s]", err)
			return err
		}

		reply.Handle = ch
		return nil
	} else if int64(args.Index) > fileNode.chunks {
		log.Errorf("RPCGetChunkHandle, err[%s]", gfs.ErrDiscontinuousChunk)
		return gfs.ErrDiscontinuousChunk
	}

	fileNode.chunks++

	servers, err := m.csm.ChooseServers(gfs.DefaultNumReplicas)
	if err != nil {
		log.Errorf("RPCGetChunkHandle, err[%s]", err)
		return err
	}

	handle, err := m.cm.CreateChunk(args.Path, servers)
	if err != nil {
		log.Errorf("RPCGetChunkHandle, err[%s]", err)
		return err
	}

	if err := m.csm.AddChunk(servers, handle); err != nil {
		log.Errorf("RPCGetChunkHandle, err[%s]", err)
		return err
	}

	reply.Handle = handle

	return nil
}

// RPCList returns list of files under path.
func (m *Master) RPCList(args gfs.ListArg, reply *gfs.ListReply) error {
	pathInfo, err := m.nm.List(args.Path)
	reply.Files = pathInfo
	return err
}
