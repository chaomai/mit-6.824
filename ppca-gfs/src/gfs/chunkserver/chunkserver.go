package chunkserver

import (
	"net"
	"net/rpc"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"gfs"
	"gfs/util"

	log "github.com/Sirupsen/logrus"
)

// ChunkServer struct
type ChunkServer struct {
	sync.RWMutex
	address    gfs.ServerAddress // chunkserver address
	master     gfs.ServerAddress // master address
	serverRoot string            // path to data storage
	l          net.Listener
	shutdown   chan struct{}
	dead       bool // set to true if server is shutdown

	dl                     *downloadBuffer                // expiring download buffer
	pendingLeaseExtensions *util.ArraySet                 // pending lease extension
	chunk                  map[gfs.ChunkHandle]*chunkInfo // chunk information
}

type Mutation struct {
	mtype   gfs.MutationType
	version gfs.ChunkVersion
	data    []byte
	offset  gfs.Offset
}

type chunkInfo struct {
	sync.RWMutex
	length  gfs.Offset
	version gfs.ChunkVersion // version number of the chunk in disk
	// newestVersion gfs.ChunkVersion               // allocated newest version number
	// mutations map[gfs.ChunkVersion]*Mutation // mutation buffer
	checksum []gfs.ChunkCheckSum
}

// NewAndServe starts a chunkserver and return the pointer to it.
func NewAndServe(addr, masterAddr gfs.ServerAddress, serverRoot string) *ChunkServer {
	cs := &ChunkServer{
		address:                addr,
		shutdown:               make(chan struct{}),
		master:                 masterAddr,
		serverRoot:             serverRoot,
		dl:                     newDownloadBuffer(gfs.DownloadBufferExpire, gfs.DownloadBufferTick),
		pendingLeaseExtensions: new(util.ArraySet),
		chunk:                  make(map[gfs.ChunkHandle]*chunkInfo),
	}
	rpcs := rpc.NewServer()
	rpcs.Register(cs)
	l, e := net.Listen("tcp", string(cs.address))
	if e != nil {
		log.Fatal("listen error:", e)
		log.Exit(1)
	}
	cs.l = l

	// RPC Handler
	go func() {
		for {
			select {
			case <-cs.shutdown:
				return
			default:
			}
			conn, err := cs.l.Accept()
			if err == nil {
				go func() {
					rpcs.ServeConn(conn)
					conn.Close()
				}()
			} else {
				// if chunk server is dead, ignores connection error
				if !cs.dead {
					log.Fatal(err)
				}
			}
		}
	}()

	// Heartbeat
	go func() {
		for {
			select {
			case <-cs.shutdown:
				return
			default:
			}

			pe := cs.pendingLeaseExtensions.GetAllAndClear()
			le := make([]gfs.ChunkHandle, len(pe))
			for i, v := range pe {
				le[i] = v.(gfs.ChunkHandle)
			}

			args := &gfs.HeartbeatArg{
				Address:         addr,
				LeaseExtensions: le,
			}

			if err := util.Call(cs.master, "Master.RPCHeartbeat", args, nil); err != nil {
				log.Fatal("heartbeat rpc error ", err)
				log.Exit(1)
			}

			time.Sleep(gfs.HeartbeatInterval)
		}
	}()

	log.Infof("ChunkServer is now running. addr = %v, root path = %v, master addr = %v", addr, serverRoot, masterAddr)

	return cs
}

// Shutdown shuts the chunkserver down
func (cs *ChunkServer) Shutdown() {
	if !cs.dead {
		log.Warningf("ChunkServer %v shuts down", cs.address)
		cs.dead = true
		close(cs.shutdown)
		cs.l.Close()
	}
}

// RPCGrantLease is called by mater.
// It increment version of a chunk when master grant lease.
func (cs *ChunkServer) RPCGrantLease(args gfs.GrantLeaseArg, reply *gfs.GrantLeaseReply) error {
	log.Infof("RPCGrantLease, addr[%s], args[%+v]", cs.address, args)

	cs.RLock()
	defer cs.RUnlock()
	if _, ok := cs.chunk[args.Handle]; !ok {
		log.Errorf("RPCGrantLease, err[%s]", gfs.ErrNoSuchHandle)
		return gfs.ErrNoSuchHandle
	}

	info := cs.chunk[args.Handle]
	info.Lock()
	defer info.Unlock()

	if info.version > args.Version {
		log.Warnf("RPCGrantLease, master chunk version[%s], cs chunk version[%s], err[%s]", args.Version, info.version, gfs.ErrStaleVersionAtMaster)
		reply.NewestVersion = info.version
		return gfs.ErrStaleVersionAtMaster
	}

	log.Infof("RPCGrantLease, chunk version[%s] assigned", args.Version)
	info.version = args.Version

	// TODO
	// write version to disk

	return nil
}

// RPCGetChunks is called by mater.
// It return all chunks and chunks' version.
func (cs *ChunkServer) RPCGetChunks(args gfs.GetChunksArg, reply *gfs.GetChunksReply) error {
	cs.RLock()
	defer cs.RUnlock()

	for k, v := range cs.chunk {
		reply.Chunks = append(reply.Chunks, gfs.CSChunkInfo{Handle: k, Length: v.length, Version: v.version})
	}

	return nil
}

// RPCPushDataAndForward is called by client.
// It saves client pushed data to memory buffer and forward to all other replicas.
// Returns a DataID which represents the index in the memory buffer.
func (cs *ChunkServer) RPCPushDataAndForward(args gfs.PushDataAndForwardArg, reply *gfs.PushDataAndForwardReply) error {
	log.Infof("RPCPushDataAndForward, addr[%s], args[%+v]", cs.address, args)

	dataID := args.DataID
	if dataID == nil {
		p := cs.dl.New(args.Handle)
		dataID = &p
	}

	cs.dl.Set(*dataID, args.Data)

	forwardTo := args.ForwardTo

	if len(forwardTo) > 0 {
		rpcArgs := gfs.PushDataAndForwardArg{Handle: args.Handle, DataID: dataID, Data: args.Data, ForwardTo: forwardTo[1:]}
		rpcReply := new(gfs.PushDataAndForwardReply)
		err := util.Call(forwardTo[0], "ChunkServer.RPCPushDataAndForward", rpcArgs, rpcReply)
		if err != nil {
			log.Errorf("RPCPushDataAndForward, err[%s]", err)
			return err
		}
	}

	reply.DataID = *dataID

	return nil
}

// RPCForwardData is called by another replica who sends data to the current memory buffer.
// TODO: This should be replaced by a chain forwarding.
func (cs *ChunkServer) RPCForwardData(args gfs.ForwardDataArg, reply *gfs.ForwardDataReply) error {
	return nil
}

// RPCCreateChunk is called by master to create a new chunk given the chunk handle.
func (cs *ChunkServer) RPCCreateChunk(args gfs.CreateChunkArg, reply *gfs.CreateChunkReply) error {
	log.Infof("RPCCreateChunk, addr[%s], args[%+v]", cs.address, args)
	cs.Lock()
	defer cs.Unlock()

	if _, ok := cs.chunk[args.Handle]; ok {
		log.Errorf("RPCCreateChunk, addr[%s], err[%s]", cs.address, gfs.ErrChunkExists)
		return gfs.ErrChunkExists
	}

	chunkPath := path.Join(cs.serverRoot, strconv.FormatInt(int64(args.Handle), 10))
	fp, err := os.OpenFile(chunkPath, os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		log.Errorf("RPCCreateChunk, addr[%s], err[%+v]", cs.address, err)
		return err
	}

	defer fp.Close()

	log.Infof("RPCCreateChunk, addr[%s], create[%s]", cs.address, chunkPath)

	cs.chunk[args.Handle] = new(chunkInfo)

	return nil
}

// RPCReadChunk is called by client, read chunk data and return
func (cs *ChunkServer) RPCReadChunk(args gfs.ReadChunkArg, reply *gfs.ReadChunkReply) error {
	log.Infof("RPCReadChunk, addr[%s], args[%+v]", cs.address, args)

	cs.RLock()
	defer cs.RUnlock()
	if _, ok := cs.chunk[args.Handle]; !ok {
		log.Errorf("RPCReadChunk, err[%s]", gfs.ErrNoSuchHandle)
		return gfs.ErrNoSuchHandle
	}

	info := cs.chunk[args.Handle]
	info.RLock()
	defer info.RUnlock()

	if args.Offset > info.length {
		log.Errorf("RPCReadChunk, err[%s]", gfs.ErrReadExceedFileSize)
		return gfs.ErrReadExceedFileSize
	}

	chunkPath := path.Join(cs.serverRoot, strconv.FormatInt(int64(args.Handle), 10))
	fp, err := os.OpenFile(chunkPath, os.O_RDONLY, 0644)
	if err != nil {
		log.Errorf("RPCReadChunk, err[%s]", err)
		return err
	}
	defer fp.Close()

	reply.Data = make([]byte, args.Length)
	reply.Length = args.Length
	n, err := fp.ReadAt(reply.Data, int64(args.Offset))

	if err != nil {
		log.Errorf("RPCReadChunk, err[%s]", err)
		return err
	}

	if n != args.Length {
		log.Errorf("RPCReadChunk, err[%s]", gfs.ErrReadIncomplete)
		return gfs.ErrReadIncomplete
	}

	log.Infof("RPCReadChunk, read[%s] data[%v] bufferLen[%v]", chunkPath, reply.Data, args.Length)

	return nil
}

// RPCWriteChunk is called by client
// applies chunk write to itself (primary) and asks secondaries to do the same.
func (cs *ChunkServer) RPCWriteChunk(args gfs.WriteChunkArg, reply *gfs.WriteChunkReply) error {
	log.Infof("RPCWriteChunk, addr[%s], args[%+v]", cs.address, args)

	retErrCh := make(chan error, gfs.DefaultNumReplicas)
	rpcArgs := gfs.ApplyMutationArg{Mtype: gfs.MutationWrite, Version: args.Version, DataID: args.DataID, Offset: args.Offset}

	go func() {
		err := cs.applyMutation(rpcArgs)
		if err != nil {
			log.Errorf("RPCWriteChunk, err[%s]", err)
		}

		retErrCh <- err
	}()

	for _, v := range args.Secondaries {
		go func(addr gfs.ServerAddress) {
			rpcReply := new(gfs.ApplyMutationReply)
			err := util.Call(addr, "ChunkServer.RPCApplyMutation", rpcArgs, rpcReply)
			if err != nil {
				log.Errorf("RPCWriteChunk, err[%s]", err)
			}

			retErrCh <- err
		}(v)
	}

	for i := 0; i < gfs.DefaultNumReplicas; i++ {
		if err := <-retErrCh; err != nil {
			log.Errorf("RPCWriteChunk, err[%s]", err)
			return err
		}
	}

	return nil
}

// RPCAppendChunk is called by client to apply atomic record append.
// The length of data should be within max append size.
// If the chunk size after appending the data will excceed the limit,
// pad current chunk and ask the client to retry on the next chunk.
func (cs *ChunkServer) RPCAppendChunk(args gfs.AppendChunkArg, reply *gfs.AppendChunkReply) error {
	return nil
}

func (cs *ChunkServer) applyMutation(args gfs.ApplyMutationArg) error {
	switch args.Mtype {
	case gfs.MutationWrite:
		return cs.applyWrite(args.DataID, args.Version, args.Offset)
	case gfs.MutationAppend:
		return cs.applyAppend(args.DataID, args.Version)
	default:
		return nil
	}
}

func (cs *ChunkServer) applyWrite(dataID gfs.DataBufferID, version gfs.ChunkVersion, offset gfs.Offset) error {
	data, ok := cs.dl.Get(dataID)
	if !ok {
		log.Errorf("applyWrite, err[%s]", gfs.ErrNoSuchDataID)
		return gfs.ErrNoSuchDataID
	}

	dataLen := len(data)
	if int64(offset)+int64(dataLen) > gfs.MaxChunkSize {
		log.Errorf("applyWrite, err[%s]", gfs.ErrWriteExceedChunkSize)
		return gfs.ErrWriteExceedChunkSize
	}

	cs.RLock()
	defer cs.RUnlock()
	if _, ok := cs.chunk[dataID.Handle]; !ok {
		log.Errorf("applyWrite, err[%s]", gfs.ErrNoSuchHandle)
		return gfs.ErrNoSuchHandle
	}

	info := cs.chunk[dataID.Handle]
	info.Lock()
	defer info.Unlock()

	if info.version != version {
		log.Errorf("applyWrite, err[%s]", gfs.ErrStaleVersionAtChunkServer)
		return gfs.ErrStaleVersionAtChunkServer
	}

	info.length = gfs.Offset(int64(info.length) + int64(dataLen))

	chunkPath := path.Join(cs.serverRoot, strconv.FormatInt(int64(dataID.Handle), 10))
	fp, err := os.OpenFile(chunkPath, os.O_WRONLY, 0644)
	if err != nil {
		log.Errorf("applyWrite, err[%s]", err)
		return err
	}
	defer fp.Close()

	log.Infof("applyWrite, write[%s] data[%v] offset[%v]", chunkPath, data, offset)

	n, err := fp.WriteAt(data, int64(offset))
	if err != nil {
		log.Errorf("applyWrite, err[%s]", err)
		return err
	}

	if n != dataLen {
		err = gfs.ErrWriteIncomplete
		log.Errorf("applyWrite, err[%s]", err)
		return err
	}

	return nil
}

func (cs *ChunkServer) applyAppend(dataID gfs.DataBufferID, version gfs.ChunkVersion) error {
	data, ok := cs.dl.Get(dataID)
	if !ok {
		log.Errorf("applyAppend, err[%s]", gfs.ErrNoSuchDataID)
		return gfs.ErrNoSuchDataID
	}

	cs.RLock()
	defer cs.RUnlock()
	if _, ok := cs.chunk[dataID.Handle]; !ok {
		log.Errorf("applyAppend, err[%s]", gfs.ErrNoSuchHandle)
		return gfs.ErrNoSuchHandle
	}

	info := cs.chunk[dataID.Handle]
	info.Lock()
	defer info.Unlock()

	if info.version != version {
		log.Errorf("applyAppend, err[%s]", gfs.ErrStaleVersionAtChunkServer)
		return gfs.ErrStaleVersionAtChunkServer
	}

	dataLen := len(data)
	if int64(info.length)+int64(dataLen) > gfs.MaxChunkSize {
		log.Errorf("applyAppend, err[%s]", gfs.ErrWriteExceedChunkSize)
		return gfs.ErrWriteExceedChunkSize
	}

	return nil
}

// RPCApplyMutation is called by primary to apply mutations
func (cs *ChunkServer) RPCApplyMutation(args gfs.ApplyMutationArg, reply *gfs.ApplyMutationReply) error {
	log.Infof("RPCApplyMutation, addr[%s], args[%+v]", cs.address, args)

	err := cs.applyMutation(args)
	if err != nil {
		log.Errorf("RPCApplyMutation, err[%s]", err)
		return err
	}

	return nil
}

// RPCSendCopy is called by master, send the whole copy to given address
func (cs *ChunkServer) RPCSendCopy(args gfs.SendCopyArg, reply *gfs.SendCopyReply) error {
	return nil
}

// RPCApplyCopy is called by another replica
// rewrite the local version to given copy data
func (cs *ChunkServer) RPCApplyCopy(args gfs.ApplyCopyArg, reply *gfs.ApplyCopyReply) error {
	return nil
}
