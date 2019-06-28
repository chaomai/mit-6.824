package chunkserver

import (
	"io"
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

			if errx := util.Call(cs.master, "Master.RPCHeartbeat", args, nil); errx != nil {
				log.Fatal("heartbeat rpc error ", errx)
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
		reply.Error = gfs.ErrNoSuchHandle
		log.Errorf("RPCGrantLease, err[%v]", reply.Error)
		return nil
	}

	info := cs.chunk[args.Handle]
	info.Lock()
	defer info.Unlock()

	if info.version > args.Version {
		reply.Error = gfs.ErrStaleVersionAtMaster
		log.Warnf("RPCGrantLease, master chunk version[%v], cs chunk version[%v], err[%v]", args.Version, info.version, reply.Error)
		reply.NewestVersion = info.version
		return nil
	}

	log.Infof("RPCGrantLease, chunk version[%v] assigned", args.Version)
	info.version = args.Version

	// TODO write version to disk

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
	log.Infof("RPCPushDataAndForward, addr[%s], handle[%v], dataID[%v], forwardTo[%v]", cs.address, args.Handle, args.DataID, args.ForwardTo)

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
		if errx := util.Call(forwardTo[0], "ChunkServer.RPCPushDataAndForward", rpcArgs, rpcReply); errx != nil {
			log.Errorf("RPCPushDataAndForward, call err[%v]", errx)
			rpcReply.Error = errx
			return nil
		} else if rpcReply.Error != nil {
			log.Errorf("RPCPushDataAndForward, err[%v]", rpcReply.Error)
			return nil
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
		reply.Error = gfs.ErrChunkExists
		log.Errorf("RPCCreateChunk, addr[%s], err[%v]", cs.address, reply.Error)
		return nil
	}

	chunkPath := path.Join(cs.serverRoot, strconv.FormatInt(int64(args.Handle), 10))
	fp, err := os.OpenFile(chunkPath, os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		reply.Error = gfs.NewErrorf("open file err[%v]", err)
		log.Errorf("RPCCreateChunk, addr[%s], err[%v]", cs.address, reply.Error)
		return nil
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
		reply.Error = gfs.ErrNoSuchHandle
		log.Errorf("RPCReadChunk, err[%v]", reply.Error)
		return nil
	}

	info := cs.chunk[args.Handle]
	info.RLock()
	defer info.RUnlock()

	if args.Offset > info.length {
		reply.Error = gfs.ErrReadExceedFileSize
		log.Errorf("RPCReadChunk, err[%v]", reply.Error)
		return nil
	}

	chunkPath := path.Join(cs.serverRoot, strconv.FormatInt(int64(args.Handle), 10))
	fp, err := os.OpenFile(chunkPath, os.O_RDONLY, 0644)
	if err != nil {
		reply.Error = gfs.NewErrorf("open file err[%v]", err)
		log.Errorf("RPCReadChunk, err[%v]", reply.Error)
		return nil
	}
	defer fp.Close()

	reply.Data = make([]byte, args.Length)
	reply.Length = args.Length
	n, err := fp.ReadAt(reply.Data, int64(args.Offset))

	reply.Length = n

	if err == io.EOF {
		reply.Error = gfs.ErrReadEOF
		log.Warnf("RPCReadChunk, err[%v]", reply.Error)
		return nil
	} else if err != nil {
		reply.Error = gfs.NewErrorf("read file err[%v]", err)
		log.Errorf("RPCReadChunk, err[%v]", reply.Error)
		return nil
	}

	if n != args.Length {
		reply.Error = gfs.ErrReadIncomplete
		log.Errorf("RPCReadChunk, err[%v]", reply.Error)
		return nil
	}

	log.Infof("RPCReadChunk, read[%s], bufferLen[%v]", chunkPath, args.Length)

	return nil
}

// RPCWriteChunk is called by client
// applies chunk write to itself (primary) and asks secondaries to do the same.
func (cs *ChunkServer) RPCWriteChunk(args gfs.WriteChunkArg, reply *gfs.WriteChunkReply) error {
	log.Infof("RPCWriteChunk, addr[%s], args[%+v]", cs.address, args)

	cs.RLock()
	defer cs.RUnlock()
	if _, ok := cs.chunk[args.DataID.Handle]; !ok {
		reply.Error = gfs.ErrNoSuchHandle
		log.Errorf("RPCWriteChunk, err[%v]", reply.Error)
		return nil
	}

	info := cs.chunk[args.DataID.Handle]
	info.Lock()
	defer info.Unlock()

	retErrCh := make(chan error, gfs.DefaultNumReplicas)
	rpcArgs := gfs.ApplyMutationArg{Mtype: gfs.MutationWrite, Version: args.Version, DataID: args.DataID, Offset: args.Offset}

	go func() {
		_, err := cs.applyMutation(rpcArgs, info)
		if err != nil {
			log.Errorf("RPCWriteChunk, err[%v]", err)
		}

		retErrCh <- err
	}()

	for _, v := range args.Secondaries {
		go func(addr gfs.ServerAddress) {
			rpcReply := new(gfs.ApplyMutationReply)
			if errx := util.Call(addr, "ChunkServer.RPCApplyMutation", rpcArgs, rpcReply); errx != nil {
				log.Errorf("RPCWriteChunk, call err[%v]", errx)
				retErrCh <- errx
				return
			} else if rpcReply.Error != nil {
				log.Errorf("RPCWriteChunk, err[%v]", rpcReply.Error)
				retErrCh <- reply.Error
				return
			} else {
				retErrCh <- nil
			}
		}(v)
	}

	for i := 0; i < gfs.DefaultNumReplicas; i++ {
		if err := <-retErrCh; err != nil {
			reply.Error = err
			log.Errorf("RPCWriteChunk, err[%v]", reply.Error)
			return nil
		}
	}

	return nil
}

// RPCAppendChunk is called by client to apply atomic record append.
// The length of data should be within max append size.
// If the chunk size after appending the data will excceed the limit,
// pad current chunk and ask the client to retry on the next chunk.
func (cs *ChunkServer) RPCAppendChunk(args gfs.AppendChunkArg, reply *gfs.AppendChunkReply) error {
	log.Infof("RPCAppendChunk, addr[%s], args[%+v]", cs.address, args)

	cs.RLock()
	defer cs.RUnlock()
	if _, ok := cs.chunk[args.DataID.Handle]; !ok {
		reply.Error = gfs.ErrNoSuchHandle
		log.Errorf("RPCAppendChunk, err[%v]", reply.Error)
		return nil
	}

	info := cs.chunk[args.DataID.Handle]
	info.Lock()
	defer info.Unlock()

	retErrCh := make(chan error, gfs.DefaultNumReplicas)
	rpcArgs := gfs.ApplyMutationArg{Mtype: gfs.MutationAppend, Version: args.Version, DataID: args.DataID}

	var offset gfs.Offset = 0

	// go
	func() {
		var err error
		offset, err = cs.applyMutation(rpcArgs, info)
		if err != nil {
			log.Errorf("RPCAppendChunk, err[%v]", err)
		}

		retErrCh <- err
	}()

	for _, v := range args.Secondaries {
		go func(addr gfs.ServerAddress) {
			rpcReply := new(gfs.ApplyMutationReply)
			if errx := util.Call(addr, "ChunkServer.RPCApplyMutation", rpcArgs, rpcReply); errx != nil {
				log.Errorf("RPCAppendChunk, call err[%v]", errx)
				retErrCh <- errx
				return
			} else if rpcReply.Error != nil {
				log.Errorf("RPCAppendChunk, err[%v]", rpcReply.Error)
				retErrCh <- reply.Error
				return
			} else {
				retErrCh <- nil
			}
		}(v)
	}

	for i := 0; i < gfs.DefaultNumReplicas; i++ {
		if err := <-retErrCh; err != nil {
			reply.Error = err
			log.Errorf("RPCAppendChunk, err[%v]", reply.Error)
			return nil
		}
	}

	reply.Offset = offset

	return nil
}

func (cs *ChunkServer) applyMutation(args gfs.ApplyMutationArg, info *chunkInfo) (offset gfs.Offset, err error) {
	switch args.Mtype {
	case gfs.MutationWrite:
		return cs.applyWrite(args, info)
	case gfs.MutationAppend:
		return cs.applyAppend(args, info)
	case gfs.MutationPad:
		return cs.applyPad(args, info)
	default:
		return
	}
}

func (cs *ChunkServer) applyWrite(args gfs.ApplyMutationArg, info *chunkInfo) (offset gfs.Offset, err error) {
	data, ok := cs.dl.Get(args.DataID)
	if !ok {
		err = gfs.ErrNoSuchDataID
		log.Errorf("applyWrite, err[%v]", err)
		return
	}

	dataLen := len(data)
	if args.Offset+gfs.Offset(dataLen) > gfs.Offset(gfs.MaxChunkSize) {
		err = gfs.ErrWriteExceedChunkSize
		log.Errorf("applyWrite, err[%v]", err)
		return
	}

	if args.Offset > info.length {
		log.Infof("applyWrite, pad chunk, err[%v]", gfs.ErrWriteExceedFileSize)

		padArgs := gfs.ApplyMutationArg{Mtype: gfs.MutationPad, Version: args.Version, DataID: args.DataID, Offset: args.Offset}
		_, err = cs.applyPad(padArgs, info)
		if err != nil {
			log.Errorf("applyWrite, err[%v]", err)
			return
		}
	}

	if info.version != args.Version {
		err = gfs.ErrStaleVersionAtChunkServer
		log.Errorf("applyWrite, err[%v]", err)
		return
	}

	info.length = gfs.Offset(int64(info.length) + int64(dataLen))

	chunkPath := path.Join(cs.serverRoot, strconv.FormatInt(int64(args.DataID.Handle), 10))
	fp, err := os.OpenFile(chunkPath, os.O_WRONLY, 0644)
	if err != nil {
		log.Errorf("applyWrite, err[%v]", err)
		return
	}
	defer fp.Close()

	log.Infof("applyWrite, write[%s], offset[%v]", chunkPath, args.Offset)

	n, err := fp.WriteAt(data, int64(args.Offset))
	if err != nil {
		log.Errorf("applyWrite, err[%v]", err)
		return
	}

	if n != dataLen {
		err = gfs.ErrWriteIncomplete
		log.Errorf("applyWrite, err[%v]", err)
		return
	}

	return
}

func (cs *ChunkServer) applyAppend(args gfs.ApplyMutationArg, info *chunkInfo) (offset gfs.Offset, err error) {
	data, ok := cs.dl.Get(args.DataID)
	if !ok {
		err = gfs.ErrNoSuchDataID
		log.Errorf("applyAppend, err[%v]", err)
		return
	}

	dataLen := len(data)
	if len(data) > gfs.MaxAppendSize {
		err = gfs.ErrAppendExceedMaxAppendSize
		log.Errorf("applyAppend, err[%v]", err)
		return
	}

	if int64(info.length)+int64(dataLen) > gfs.MaxChunkSize {
		padArgs := gfs.ApplyMutationArg{Mtype: gfs.MutationPad, Version: args.Version, DataID: args.DataID}
		_, err = cs.applyPad(padArgs, info)

		if err != nil {
			log.Errorf("applyAppend, err[%v]", err)
			return
		}

		err = gfs.ErrAppendExceedChunkSize
		log.Errorf("applyAppend, err[%v]", err)
		return
	}

	if info.version != args.Version {
		err = gfs.ErrStaleVersionAtChunkServer
		log.Errorf("applyAppend, err[%v]", err)
		return
	}

	offset = info.length
	info.length = gfs.Offset(int64(info.length) + int64(dataLen))

	chunkPath := path.Join(cs.serverRoot, strconv.FormatInt(int64(args.DataID.Handle), 10))
	fp, err := os.OpenFile(chunkPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Errorf("applyAppend, err[%v]", err)
		return
	}
	defer fp.Close()

	log.Infof("applyAppend, write[%s]", chunkPath)

	n, err := fp.Write(data)
	if err != nil {
		log.Errorf("applyAppend, err[%v]", err)
		return
	}

	if n != dataLen {
		err = gfs.ErrWriteIncomplete
		log.Errorf("applyAppend, err[%v]", err)
		return
	}

	return
}

func (cs *ChunkServer) applyPad(args gfs.ApplyMutationArg, info *chunkInfo) (offset gfs.Offset, err error) {
	var padLen gfs.Offset = 0
	if args.Offset != 0 {
		padLen = args.Offset - info.length
	} else {
		padLen = gfs.MaxChunkSize - info.length
	}

	info.length = info.length + padLen

	padData := make([]byte, padLen)

	chunkPath := path.Join(cs.serverRoot, strconv.FormatInt(int64(args.DataID.Handle), 10))
	fp, err := os.OpenFile(chunkPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Errorf("applyPad, err[%v]", err)
		return
	}
	defer fp.Close()

	n, err := fp.Write(padData)
	if err != nil {
		log.Errorf("applyPad, err[%v]", err)
		return
	}

	if gfs.Offset(n) != padLen {
		err = gfs.ErrWriteIncomplete
		log.Errorf("applyPad, err[%v]", err)
		return
	}

	return
}

// RPCApplyMutation is called by primary to apply mutations
func (cs *ChunkServer) RPCApplyMutation(args gfs.ApplyMutationArg, reply *gfs.ApplyMutationReply) error {
	log.Infof("RPCApplyMutation, addr[%s], args[%+v]", cs.address, args)

	cs.RLock()
	defer cs.RUnlock()
	if _, ok := cs.chunk[args.DataID.Handle]; !ok {
		reply.Error = gfs.ErrNoSuchHandle
		log.Errorf("RPCApplyMutation, err[%v]", reply.Error)
		return nil
	}

	info := cs.chunk[args.DataID.Handle]
	info.Lock()
	defer info.Unlock()

	rpcArgs := gfs.ApplyMutationArg{Mtype: args.Mtype, Version: args.Version, DataID: args.DataID, Offset: args.Offset}

	if _, err := cs.applyMutation(rpcArgs, info); err != nil {
		reply.Error = err
		log.Errorf("RPCApplyMutation, err[%v]", err)
		return nil
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
