package master

import (
	"net"
	"net/rpc"
	"time"

	"gfs"
	"gfs/util"

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

	log.Infof("master[%v], Master is running now. addr = %v, root path = %v", address, address, serverRoot)

	return m
}

// Shutdown shuts down master
func (m *Master) Shutdown() {
	close(m.shutdown)
}

// BackgroundActivity does all the background activities:
// dead chunkserver handling, garbage collection, stale replica detection, etc
func (m *Master) BackgroundActivity() (err error) {
	// dead chunkserver and reReplication
	deadServers := m.csm.DetectDeadServers()
	log.Infof("BackgroundActivity, deadServers[%v]", deadServers)
	return

	for _, deadAddr := range deadServers {
		var deadCSHandles []gfs.ChunkHandle
		deadCSHandles, err = m.csm.RemoveServer(deadAddr)
		if err != nil {
			log.Errorf("BackgroundActivity, err[%v]", err)
			return
		}

		for _, handle := range deadCSHandles {
			var curReplicas []gfs.ServerAddress
			curReplicas, err = m.cm.removeServerFromLocation(handle, deadAddr)
			if err != nil {
				log.Errorf("BackgroundActivity, err[%v]", err)
				return
			}

			var from, to gfs.ServerAddress
			from, to, err = m.csm.ChooseReReplication(handle, curReplicas)
			if err == gfs.ErrNoNeedForReReplication {
				log.Warnf("BackgroundActivity, err[%v]", err)
				continue
			} else if err != nil {
				log.Errorf("BackgroundActivity, err[%v]", err)
				return
			}

			// copy replica
			rpcArgs := gfs.SendCopyArg{Handle: handle, Address: to}
			rpcReply := new(gfs.SendCopyReply)

			err = util.Call(from, "ChunkServer.RPCSendCopy", rpcArgs, rpcReply)
			if err != nil {
				log.Errorf("BackgroundActivity, call err[%v]", err)
				return
			} else if rpcReply.Error != nil {
				err = rpcReply.Error
				log.Errorf("BackgroundActivity, err[%v]", err)
				return
			}

			err = m.cm.RegisterReplica(handle, to)
			if err != nil {
				log.Errorf("BackgroundActivity, err[%v]", err)
				return
			}
		}
	}

	return
}

// RPCHeartbeat is called by chunkserver to let the master know that a chunkserver is alive.
// Lease extension request is included.
func (m *Master) RPCHeartbeat(args gfs.HeartbeatArg, reply *gfs.HeartbeatReply) error {
	log.Infof("master[%v], RPCHeartbeat, args[%+v]", m.address, args)

	m.csm.Heartbeat(args.Address)

	for _, v := range args.LeaseExtensions {
		err := m.cm.ExtendLease(v, args.Address)
		if err != nil {
			reply.Error = err
			return nil
		}
	}

	return nil
}

// RPCGetPrimaryAndSecondaries returns lease holder and secondaries of a chunk.
// If no one holds the lease currently, grant one.
func (m *Master) RPCGetPrimaryAndSecondaries(args gfs.GetPrimaryAndSecondariesArg, reply *gfs.GetPrimaryAndSecondariesReply) error {
	log.Infof("master[%v], RPCGetPrimaryAndSecondaries, args[%+v]", m.address, args)

	l, err := m.cm.GetLeaseHolder(args.Handle)

	if err != nil {
		reply.Error = err
		log.Errorf("master[%v], RPCGetPrimaryAndSecondaries, err[%v]", m.address, err)
		return nil
	}

	log.Infof("master[%v], RPCGetPrimaryAndSecondaries, lease[%+v]", m.address, l)
	reply.Primary = l.Primary
	reply.Secondaries = l.Secondaries
	reply.Expire = l.Expire
	reply.Version = l.Version

	return nil
}

// RPCGetReplicas is called by client to find all chunkservers that hold the chunk.
func (m *Master) RPCGetReplicas(args gfs.GetReplicasArg, reply *gfs.GetReplicasReply) error {
	log.Infof("master[%v], RPCGetReplicas, args[%+v]", m.address, args)

	loc, err := m.cm.GetReplicas(args.Handle)
	if err != nil {
		reply.Error = err
		log.Errorf("master[%v], RPCGetReplicas, err[%v]", m.address, err)
		return nil
	}

	for _, e := range loc.GetAll() {
		addr := e.(gfs.ServerAddress)
		reply.Locations = append(reply.Locations, addr)
	}

	return nil
}

// RPCCreateFile is called by client to create a new file
func (m *Master) RPCCreateFile(args gfs.CreateFileArg, replay *gfs.CreateFileReply) error {
	log.Infof("master[%v], RPCCreateFile, args[%+v]", m.address, args)

	replay.Error = m.nm.Create(args.Path)
	return nil
}

// RPCMkdir is called by client to make a new directory
func (m *Master) RPCMkdir(args gfs.MkdirArg, replay *gfs.MkdirReply) error {
	log.Infof("master[%v], RPCMkdir, args[%+v]", m.address, args)

	replay.Error = m.nm.Mkdir(args.Path)
	return nil
}

// RPCGetFileInfo is called by client to get file information
func (m *Master) RPCGetFileInfo(args gfs.GetFileInfoArg, reply *gfs.GetFileInfoReply) error {
	log.Infof("master[%v], RPCGetFileInfo, args[%+v]", m.address, args)

	isDir, length, chunks, err := m.nm.GetFileInfo(args.Path)
	reply.IsDir = isDir
	reply.Length = length
	reply.Chunks = chunks
	reply.Error = err

	return nil
}

// RPCGetChunkHandle returns the chunk handle of (path, index).
// If the requested index is bigger than the number of chunks of this path by exactly one, create one.
func (m *Master) RPCGetChunkHandle(args gfs.GetChunkHandleArg, reply *gfs.GetChunkHandleReply) error {
	log.Infof("master[%v], RPCGetChunkHandle, args[%+v]", m.address, args)

	dirParts, leafName, err := m.nm.dirAndLeafName(args.Path)
	if err != nil {
		reply.Error = err
		return nil
	}

	parentNode, err := m.nm.lockParents(dirParts, true)
	if err != nil {
		reply.Error = err
		return nil
	}

	defer m.nm.unlockParents(dirParts, true)

	fileNode, ok := parentNode.children[leafName]
	if !ok {
		log.Errorf("master[%v], RPCGetChunkHandle, file[%s], err[%v]", m.address, args.Path, gfs.ErrFileNotExists)
		reply.Error = gfs.ErrFileNotExists
		return nil
	}

	if fileNode.isDir {
		log.Errorf("master[%v], RPCGetChunkHandle, path[%s], err[%v]", m.address, args.Path, gfs.ErrPathIsNotFile)
		reply.Error = gfs.ErrPathIsNotFile
		return nil
	}

	fileNode.Lock()
	defer fileNode.Unlock()

	if args.Index < gfs.ChunkIndex(fileNode.chunks) {
		ch, err := m.cm.GetChunk(args.Path, args.Index)

		if err != nil {
			log.Errorf("master[%v], RPCGetChunkHandle, err[%v]", m.address, err)
			reply.Error = err
			return nil
		}

		reply.Handle = ch
		return nil
	} else if args.Index > gfs.ChunkIndex(fileNode.chunks) {
		log.Errorf("master[%v], RPCGetChunkHandle, err[%v]", m.address, gfs.ErrCreateDiscontinuousChunk)
		reply.Error = gfs.ErrCreateDiscontinuousChunk
		return nil
	}

	fileNode.chunks++

	servers, err := m.csm.ChooseServers(gfs.DefaultNumReplicas)
	if err != nil {
		log.Errorf("master[%v], RPCGetChunkHandle, err[%v]", m.address, err)
		reply.Error = err
		return nil
	}

	handle, err := m.cm.CreateChunk(args.Path, servers)
	if err != nil {
		log.Errorf("master[%v], RPCGetChunkHandle, err[%v]", m.address, err)
		reply.Error = err
		return nil
	}

	if err := m.csm.AddChunk(servers, handle); err != nil {
		log.Errorf("master[%v], RPCGetChunkHandle, err[%v]", m.address, err)
		reply.Error = err
		return nil
	}

	reply.Handle = handle

	return nil
}

// RPCList returns list of files under path.
func (m *Master) RPCList(args gfs.ListArg, reply *gfs.ListReply) error {
	log.Infof("master[%v], RPCList, args[%+v]", m.address, args)

	pathInfo, err := m.nm.List(args.Path)
	reply.Files = pathInfo
	reply.Error = err
	return nil
}
