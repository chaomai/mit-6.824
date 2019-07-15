package chunkserver

import (
	"encoding/gob"
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

	metaMutex sync.Mutex
	metaFile  string
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
		metaFile:               path.Join(serverRoot, gfs.ChunkServerMetaFileName),
	}
	rpcs := rpc.NewServer()
	rpcs.Register(cs)
	l, err := net.Listen("tcp", string(cs.address))
	if err != nil {
		log.Fatal("listen error:", err)
		log.Exit(1)
	}
	cs.l = l

	err = cs.deserialize()
	if err != nil {
		log.Fatalf("chunkserver[%v], NewAndServe, err[%v]", cs.address, err)
		log.Exit(1)
	}

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

			if err := cs.serialize(true); err != nil {
				log.Errorf("chunkserver[%v], NewAndServe, err[%v]", cs.address, err)
				return
			}

			pe := cs.pendingLeaseExtensions.GetAllAndClear()
			le := make([]gfs.ChunkHandle, len(pe))
			for i, v := range pe {
				le[i] = v.(gfs.ChunkHandle)
			}

			rpcArgs := &gfs.HeartbeatArg{Address: addr, LeaseExtensions: le}
			rpcReply := new(gfs.HeartbeatReply)
			if err := util.Call(cs.master, "Master.RPCHeartbeat", rpcArgs, rpcReply); err != nil || rpcReply.Error != nil {
				log.Fatal("heartbeat rpc error ", err, rpcReply.Error)
				log.Exit(1)
			}

			time.Sleep(gfs.HeartbeatInterval)
		}
	}()

	log.Infof("chunkserver[%v], ChunkServer is now running. addr = %v, root path = %v, master addr = %v", addr, addr, serverRoot, masterAddr)

	return cs
}

func (cs *ChunkServer) serialize(isLock bool) error {
	if isLock {
		cs.RLock()
		defer cs.RUnlock()
	}

	cs.metaMutex.Lock()
	defer cs.metaMutex.Unlock()

	persist := make([]gfs.CSChunkInfo, 0)

	for handle, info := range cs.chunk {
		if isLock {
			info.RLock()
		}

		persist = append(persist, gfs.CSChunkInfo{Handle: handle, Length: info.length, Version: info.version, CheckSum: info.checksum})

		if isLock {
			info.RUnlock()
		}
	}

	fp, err := os.OpenFile(cs.metaFile, os.O_CREATE|os.O_WRONLY, gfs.DefaultFilePerm)
	if err != nil {
		log.Errorf("chunkserver[%v], serialize, err[%v]", cs.address, err)
		return err
	}

	defer fp.Close()

	enc := gob.NewEncoder(fp)
	err = enc.Encode(persist)
	if err != nil {
		log.Errorf("chunkserver[%v], serialize, err[%v]", cs.address, err)
		return err
	}

	return nil
}

func (cs *ChunkServer) deserialize() error {
	cs.Lock()
	defer cs.Unlock()

	cs.metaMutex.Lock()
	defer cs.metaMutex.Unlock()

	if _, err := os.Stat(cs.metaFile); os.IsNotExist(err) {
		log.Infof("chunkserver[%v], deserialize, err[%v]", cs.address, err)
		return nil
	} else if err != nil {
		log.Errorf("chunkserver[%v], deserialize, err[%v]", cs.address, err)
		return err
	}

	persist := make([]gfs.CSChunkInfo, 0)
	fp, err := os.OpenFile(cs.metaFile, os.O_CREATE|os.O_RDONLY, gfs.DefaultFilePerm)
	if err != nil {
		log.Errorf("chunkserver[%v], deserialize, err[%v]", cs.address, err)
		return err
	}

	defer fp.Close()

	dec := gob.NewDecoder(fp)
	err = dec.Decode(&persist)
	if err != nil {
		log.Errorf("chunkserver[%v], deserialize, err[%v]", cs.address, err)
		return err
	}

	for _, chunk := range persist {
		info := new(chunkInfo)
		info.length = chunk.Length
		info.version = chunk.Version
		info.checksum = chunk.CheckSum

		cs.chunk[chunk.Handle] = info
	}

	return nil
}

// Shutdown shuts the chunkserver down
func (cs *ChunkServer) Shutdown() {
	if err := cs.serialize(true); err != nil {
		log.Errorf("chunkserver[%v], Shutdown, err[%v]", cs.address, err)
	}

	if !cs.dead {
		log.Warnf("chunkserver[%v], ChunkServer %v shuts down", cs.address, cs.address)
		cs.dead = true
		close(cs.shutdown)
		cs.l.Close()
	}
}

func (cs *ChunkServer) extendLease(handle gfs.ChunkHandle) {
	cs.pendingLeaseExtensions.Add(handle)
}

// RPCGrantLease is called by mater.
// It increment version of a chunk when master grant lease.
func (cs *ChunkServer) RPCGrantLease(args gfs.GrantLeaseArg, reply *gfs.GrantLeaseReply) error {
	log.Infof("chunkserver[%v], RPCGrantLease, args[%+v]", cs.address, args)

	cs.extendLease(args.Handle)

	cs.RLock()
	defer cs.RUnlock()
	if _, ok := cs.chunk[args.Handle]; !ok {
		reply.Error = gfs.ErrNoSuchHandle
		log.Errorf("chunkserver[%v], RPCGrantLease, err[%v]", cs.address, reply.Error)
		return nil
	}

	info := cs.chunk[args.Handle]
	info.Lock()
	defer info.Unlock()

	if info.version > args.Version {
		reply.Error = gfs.ErrStaleVersionAtMaster
		log.Warnf("chunkserver[%v], RPCGrantLease, master chunk version[%v], cs chunk version[%v], err[%v]", cs.address, args.Version, info.version, reply.Error)
		reply.NewestVersion = info.version
		return nil
	}

	log.Infof("chunkserver[%v], RPCGrantLease, chunk version[%v] assigned", cs.address, args.Version)
	info.version = args.Version

	// TODO write version to disk

	return nil
}

// RPCGetChunks is called by mater.
// It return all chunks and chunks' version.
func (cs *ChunkServer) RPCGetChunks(args gfs.GetChunksArg, reply *gfs.GetChunksReply) error {
	log.Infof("chunkserver[%v], RPCGetChunks, args[%+v]", cs.address, args)

	cs.RLock()
	defer cs.RUnlock()

	for handle, info := range cs.chunk {
		reply.Chunks = append(reply.Chunks, gfs.CSChunkInfo{Handle: handle, Length: info.length, Version: info.version, CheckSum: info.checksum})
	}

	return nil
}

// RPCPushDataAndForward is called by client.
// It saves client pushed data to memory buffer and forward to all other replicas.
// Returns a DataID which represents the index in the memory buffer.
func (cs *ChunkServer) RPCPushDataAndForward(args gfs.PushDataAndForwardArg, reply *gfs.PushDataAndForwardReply) error {
	log.Infof("chunkserver[%v], RPCPushDataAndForward, handle[%v], dataID[%v], forwardTo[%v]", cs.address, args.Handle, args.DataID, args.ForwardTo)

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
		if err := util.Call(forwardTo[0], "ChunkServer.RPCPushDataAndForward", rpcArgs, rpcReply); err != nil {
			log.Errorf("chunkserver[%v], RPCPushDataAndForward, call err[%v]", cs.address, err)
			reply.Error = err
			return nil
		} else if rpcReply.Error != nil {
			log.Errorf("chunkserver[%v], RPCPushDataAndForward, err[%v]", cs.address, rpcReply.Error)
			reply.Error = rpcReply.Error
			return nil
		}
	}

	reply.DataID = *dataID

	return nil
}

// RPCForwardData is called by another replica who sends data to the current memory buffer.
func (cs *ChunkServer) RPCForwardData(args gfs.ForwardDataArg, reply *gfs.ForwardDataReply) error {
	return nil
}

// RPCCreateChunk is called by master to create a new chunk given the chunk handle.
func (cs *ChunkServer) RPCCreateChunk(args gfs.CreateChunkArg, reply *gfs.CreateChunkReply) error {
	log.Infof("chunkserver[%v], RPCCreateChunk, args[%+v]", cs.address, args)

	cs.Lock()
	defer cs.Unlock()

	if _, ok := cs.chunk[args.Handle]; ok {
		reply.Error = gfs.ErrChunkExists
		log.Errorf("chunkserver[%v], RPCCreateChunk, err[%v]", cs.address, reply.Error)
		return nil
	}

	chunkPath := path.Join(cs.serverRoot, strconv.FormatInt(int64(args.Handle), 10))
	fp, err := os.OpenFile(chunkPath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		reply.Error = gfs.NewErrorf("open file err[%v]", cs.address, err)
		log.Errorf("chunkserver[%v], RPCCreateChunk, err[%v]", cs.address, reply.Error)
		return nil
	}

	defer fp.Close()

	log.Infof("chunkserver[%v], RPCCreateChunk, create[%s]", cs.address, chunkPath)

	cs.chunk[args.Handle] = new(chunkInfo)

	return nil
}

// RPCReadChunk is called by client, read chunk data and return
func (cs *ChunkServer) RPCReadChunk(args gfs.ReadChunkArg, reply *gfs.ReadChunkReply) error {
	log.Infof("chunkserver[%v], RPCReadChunk, args[%+v]", cs.address, args)

	cs.extendLease(args.Handle)

	cs.RLock()
	defer cs.RUnlock()
	if _, ok := cs.chunk[args.Handle]; !ok {
		reply.Error = gfs.ErrNoSuchHandle
		log.Errorf("chunkserver[%v], RPCReadChunk, err[%v]", cs.address, reply.Error)
		return nil
	}

	info := cs.chunk[args.Handle]
	info.RLock()
	defer info.RUnlock()

	if args.Offset > info.length {
		reply.Error = gfs.ErrReadExceedFileSize
		log.Errorf("chunkserver[%v], RPCReadChunk, err[%v]", cs.address, reply.Error)
		return nil
	}

	reply.Data = make([]byte, args.Length)

	n, err := cs.readChunk(args.Handle, args.Offset, reply.Data, args.Length)
	if err == gfs.ErrReadEOF {
		log.Warnf("chunkserver[%v], RPCReadChunk, err[%v]", cs.address, err)
	} else if err != nil {
		log.Errorf("chunkserver[%v], RPCReadChunk, err[%v]", cs.address, err)
	}

	reply.Length = n
	reply.Error = err

	return nil
}

func (cs *ChunkServer) readChunk(handle gfs.ChunkHandle, offset gfs.Offset, data []byte, length int) (n int, err error) {
	chunkPath := path.Join(cs.serverRoot, strconv.FormatInt(int64(handle), 10))
	fp, err := os.OpenFile(chunkPath, os.O_RDONLY, 0644)
	if err != nil {
		err = gfs.NewErrorf("open file err[%v]", cs.address, err)
		log.Errorf("chunkserver[%v], readChunk, err[%v]", cs.address, err)
		return
	}
	defer fp.Close()

	n, err = fp.ReadAt(data, int64(offset))

	log.Infof("chunkserver[%v], readChunk, read[%s], bufferLen[%v], readLen[%v]", cs.address, chunkPath, length, n)

	if err == io.EOF {
		err = gfs.ErrReadEOF
		log.Warnf("chunkserver[%v], readChunk, err[%v]", cs.address, err)
		return
	} else if err != nil {
		err = gfs.NewErrorf("read file err[%v]", cs.address, err)
		log.Errorf("chunkserver[%v], readChunk, err[%v]", cs.address, err)
		return
	}

	if n != length {
		err = gfs.ErrReadIncomplete
		log.Errorf("chunkserver[%v], readChunk, err[%v]", cs.address, err)
		return
	}

	return
}

// RPCWriteChunk is called by client
// applies chunk write to itself (primary) and asks secondaries to do the same.
func (cs *ChunkServer) RPCWriteChunk(args gfs.WriteChunkArg, reply *gfs.WriteChunkReply) error {
	log.Infof("chunkserver[%v], RPCWriteChunk, args[%+v]", cs.address, args)

	cs.extendLease(args.DataID.Handle)

	cs.RLock()
	defer cs.RUnlock()
	if _, ok := cs.chunk[args.DataID.Handle]; !ok {
		reply.Error = gfs.ErrNoSuchHandle
		log.Errorf("chunkserver[%v], RPCWriteChunk, err[%v]", cs.address, reply.Error)
		return nil
	}

	info := cs.chunk[args.DataID.Handle]
	info.Lock()
	defer info.Unlock()

	retErrCh := make(chan error, gfs.DefaultNumReplicas)
	rpcArgs := gfs.ApplyMutationArg{Mtype: gfs.MutationWrite, Version: args.Version, DataID: args.DataID, Offset: args.Offset}

	go func() {
		_, err := cs.applyMutation(rpcArgs, info, true)
		if err != nil {
			log.Errorf("chunkserver[%v], RPCWriteChunk, err[%v]", cs.address, err)
		}

		retErrCh <- err
	}()

	for _, v := range args.Secondaries {
		go func(addr gfs.ServerAddress) {
			rpcReply := new(gfs.ApplyMutationReply)
			if err := util.Call(addr, "ChunkServer.RPCApplyMutation", rpcArgs, rpcReply); err != nil {
				log.Errorf("chunkserver[%v], RPCWriteChunk, call err[%v]", cs.address, err)
				retErrCh <- err
				return
			} else if rpcReply.Error != nil {
				log.Errorf("chunkserver[%v], RPCWriteChunk, err[%v]", cs.address, rpcReply.Error)
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
			log.Errorf("chunkserver[%v], RPCWriteChunk, err[%v]", cs.address, reply.Error)
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
	log.Infof("chunkserver[%v], RPCAppendChunk, args[%+v]", cs.address, args)

	cs.extendLease(args.DataID.Handle)

	cs.RLock()
	defer cs.RUnlock()
	if _, ok := cs.chunk[args.DataID.Handle]; !ok {
		reply.Error = gfs.ErrNoSuchHandle
		log.Errorf("chunkserver[%v], RPCAppendChunk, err[%v]", cs.address, reply.Error)
		return nil
	}

	info := cs.chunk[args.DataID.Handle]
	info.Lock()
	defer info.Unlock()

	retErrCh := make(chan error, gfs.DefaultNumReplicas)
	rpcArgs := gfs.ApplyMutationArg{Mtype: gfs.MutationAppend, Version: args.Version, DataID: args.DataID}

	offset, err := cs.applyMutation(rpcArgs, info, true)
	if err == gfs.ErrAppendExceedChunkSize {
		log.Warnf("chunkserver[%v], RPCAppendChunk, err[%v]", cs.address, err)
	} else if err != nil {
		log.Errorf("chunkserver[%v], RPCAppendChunk, err[%v]", cs.address, err)
	}

	retErrCh <- err

	rpcArgs.Offset = offset

	for _, v := range args.Secondaries {
		go func(addr gfs.ServerAddress) {
			rpcReply := new(gfs.ApplyMutationReply)
			if err := util.Call(addr, "ChunkServer.RPCApplyMutation", rpcArgs, rpcReply); err != nil {
				log.Errorf("chunkserver[%v], RPCAppendChunk, call err[%v]", cs.address, err)
				retErrCh <- err
			} else if rpcReply.Error == gfs.ErrAppendExceedChunkSize {
				log.Warnf("chunkserver[%v], RPCAppendChunk, err[%v]", cs.address, rpcReply.Error)
				retErrCh <- rpcReply.Error
			} else if rpcReply.Error != nil {
				log.Errorf("chunkserver[%v], RPCAppendChunk, err[%v]", cs.address, rpcReply.Error)
				retErrCh <- rpcReply.Error
			} else {
				retErrCh <- nil
			}
		}(v)
	}

	for i := 0; i < gfs.DefaultNumReplicas; i++ {
		if err := <-retErrCh; err != nil {
			reply.Error = err
			return nil
		}
	}

	reply.Offset = offset

	return nil
}

func (cs *ChunkServer) applyWrite(args gfs.ApplyMutationArg, info *chunkInfo, isPrimary bool) (offset gfs.Offset, err error) {
	data, ok := cs.dl.Get(args.DataID)
	if !ok {
		err = gfs.ErrNoSuchDataID
		log.Errorf("chunkserver[%v], applyWrite, err[%v]", cs.address, err)
		return
	}

	dataLen := len(data)
	if args.Offset+gfs.Offset(dataLen) > gfs.Offset(gfs.MaxChunkSize) {
		err = gfs.ErrWriteExceedChunkSize
		log.Errorf("chunkserver[%v], applyWrite, err[%v]", cs.address, err)
		return
	}

	if args.Offset > info.length {
		log.Infof("chunkserver[%v], applyWrite, pad chunk, err[%v]", cs.address, gfs.ErrWriteExceedFileSize)

		padArgs := gfs.ApplyMutationArg{Mtype: gfs.MutationPad, Version: args.Version, DataID: args.DataID, Offset: args.Offset}
		_, err = cs.applyPad(padArgs, info, true)
		if err != nil {
			log.Errorf("chunkserver[%v], applyWrite, err[%v]", cs.address, err)
			return
		}
	}

	if info.version != args.Version {
		err = gfs.ErrStaleVersionAtChunkServer
		log.Errorf("chunkserver[%v], applyWrite, err[%v]", cs.address, err)
		return
	}

	info.length = gfs.Offset(int64(info.length) + int64(dataLen))

	chunkPath := path.Join(cs.serverRoot, strconv.FormatInt(int64(args.DataID.Handle), 10))
	fp, err := os.OpenFile(chunkPath, os.O_WRONLY, gfs.DefaultFilePerm)
	if err != nil {
		log.Errorf("chunkserver[%v], applyWrite, err[%v]", cs.address, err)
		return
	}
	defer fp.Close()

	log.Infof("chunkserver[%v], applyWrite, write[%s], offset[%v]", cs.address, chunkPath, args.Offset)

	n, err := fp.WriteAt(data, int64(args.Offset))
	if err != nil {
		log.Errorf("chunkserver[%v], applyWrite, err[%v]", cs.address, err)
		return
	}

	if n != dataLen {
		err = gfs.ErrWriteIncomplete
		log.Errorf("chunkserver[%v], applyWrite, err[%v]", cs.address, err)
		return
	}

	if err = cs.serialize(false); err != nil {
		log.Errorf("chunkserver[%v], applyWrite, err[%v]", cs.address, err)
		return
	}

	return
}

func (cs *ChunkServer) applyAppend(args gfs.ApplyMutationArg, info *chunkInfo, isPrimary bool) (offset gfs.Offset, err error) {
	data, ok := cs.dl.Get(args.DataID)
	if !ok {
		err = gfs.ErrNoSuchDataID
		log.Errorf("chunkserver[%v], applyAppend, err[%v]", cs.address, err)
		return
	}

	dataLen := len(data)
	if len(data) > gfs.MaxAppendSize {
		err = gfs.ErrAppendExceedMaxAppendSize
		log.Errorf("chunkserver[%v], applyAppend, err[%v]", cs.address, err)
		return
	}

	if int64(info.length)+int64(dataLen) > gfs.MaxChunkSize {
		padArgs := gfs.ApplyMutationArg{Mtype: gfs.MutationPad, Version: args.Version, DataID: args.DataID}
		_, err = cs.applyPad(padArgs, info, isPrimary)

		if err != nil {
			log.Errorf("chunkserver[%v], applyAppend, err[%v]", cs.address, err)
			return
		}

		err = gfs.ErrAppendExceedChunkSize
		log.Warnf("chunkserver[%v], applyAppend, err[%v]", cs.address, err)
		return
	}

	if info.version != args.Version {
		err = gfs.ErrStaleVersionAtChunkServer
		log.Errorf("chunkserver[%v], applyAppend, err[%v]", cs.address, err)
		return
	}

	if isPrimary {
		offset = info.length
	} else {
		offset = args.Offset
	}

	info.length = offset + gfs.Offset(dataLen)

	chunkPath := path.Join(cs.serverRoot, strconv.FormatInt(int64(args.DataID.Handle), 10))
	fp, err := os.OpenFile(chunkPath, os.O_WRONLY|os.O_APPEND, gfs.DefaultFilePerm)
	if err != nil {
		log.Errorf("chunkserver[%v], applyAppend, err[%v]", cs.address, err)
		return
	}
	defer fp.Close()

	log.Infof("chunkserver[%v], applyAppend, write[%s]", cs.address, chunkPath)

	n, err := fp.WriteAt(data, int64(offset))
	if err != nil {
		log.Errorf("chunkserver[%v], applyAppend, err[%v]", cs.address, err)
		return
	}

	if n != dataLen {
		err = gfs.ErrWriteIncomplete
		log.Errorf("chunkserver[%v], applyAppend, err[%v]", cs.address, err)
		return
	}

	if err = cs.serialize(false); err != nil {
		log.Errorf("chunkserver[%v], applyAppend, err[%v]", cs.address, err)
		return
	}

	return
}

func (cs *ChunkServer) applyPad(args gfs.ApplyMutationArg, info *chunkInfo, isPrimary bool) (offset gfs.Offset, err error) {
	if info.version != args.Version {
		err = gfs.ErrStaleVersionAtChunkServer
		log.Errorf("chunkserver[%v], applyAppend, err[%v]", cs.address, err)
		return
	}

	var padLen gfs.Offset = 0
	if args.Offset != 0 {
		padLen = args.Offset - info.length
	} else {
		padLen = gfs.MaxChunkSize - info.length
	}

	padData := make([]byte, padLen)

	if isPrimary {
		offset = info.length
	} else {
		offset = args.Offset
	}

	info.length = offset + gfs.Offset(padLen)

	chunkPath := path.Join(cs.serverRoot, strconv.FormatInt(int64(args.DataID.Handle), 10))
	fp, err := os.OpenFile(chunkPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Errorf("chunkserver[%v], applyPad, err[%v]", cs.address, err)
		return
	}
	defer fp.Close()

	n, err := fp.WriteAt(padData, int64(offset))
	if err != nil {
		log.Errorf("chunkserver[%v], applyPad, err[%v]", cs.address, err)
	}

	if gfs.Offset(n) != padLen {
		err = gfs.ErrWriteIncomplete
		log.Errorf("chunkserver[%v], applyPad, err[%v]", cs.address, err)
		return
	}

	if err = cs.serialize(false); err != nil {
		log.Errorf("chunkserver[%v], applyPad, err[%v]", cs.address, err)
		return
	}

	return
}

func (cs *ChunkServer) applyMutation(args gfs.ApplyMutationArg, info *chunkInfo, isPrimary bool) (offset gfs.Offset, err error) {
	switch args.Mtype {
	case gfs.MutationWrite:
		return cs.applyWrite(args, info, isPrimary)
	case gfs.MutationAppend:
		return cs.applyAppend(args, info, isPrimary)
	case gfs.MutationPad:
		return cs.applyPad(args, info, isPrimary)
	default:
		return
	}
}

// RPCApplyMutation is called by primary to apply mutations
func (cs *ChunkServer) RPCApplyMutation(args gfs.ApplyMutationArg, reply *gfs.ApplyMutationReply) error {
	log.Infof("chunkserver[%v], RPCApplyMutation, args[%+v]", cs.address, args)

	cs.RLock()
	defer cs.RUnlock()
	if _, ok := cs.chunk[args.DataID.Handle]; !ok {
		reply.Error = gfs.ErrNoSuchHandle
		log.Errorf("chunkserver[%v], RPCApplyMutation, err[%v]", cs.address, reply.Error)
		return nil
	}

	info := cs.chunk[args.DataID.Handle]
	info.Lock()
	defer info.Unlock()

	rpcArgs := gfs.ApplyMutationArg{Mtype: args.Mtype, Version: args.Version, DataID: args.DataID, Offset: args.Offset}

	if _, err := cs.applyMutation(rpcArgs, info, false); err == gfs.ErrAppendExceedChunkSize {
		reply.Error = err
		log.Warnf("chunkserver[%v], RPCApplyMutation, err[%v]", cs.address, err)
	} else if err != nil {
		reply.Error = err
		log.Errorf("chunkserver[%v], RPCApplyMutation, err[%v]", cs.address, err)
	}

	return nil
}

// RPCSendCopy is called by master, send the whole copy to given address
func (cs *ChunkServer) RPCSendCopy(args gfs.SendCopyArg, reply *gfs.SendCopyReply) error {
	log.Infof("chunkserver[%v], RPCSendCopy, args[%+v]", cs.address, args)

	data := make([]byte, gfs.MaxChunkSize)

	readLen, err := cs.readChunk(args.Handle, 0, data, gfs.MaxChunkSize)
	if err == gfs.ErrReadEOF {
		log.Warnf("chunkserver[%v], RPCSendCopy, err[%v]", cs.address, err)
		// reply.Error = err
	} else if err != nil {
		log.Errorf("chunkserver[%v], RPCSendCopy, err[%v]", cs.address, err)
		reply.Error = err
		return nil
	}

	cs.RLock()
	defer cs.RUnlock()
	if _, ok := cs.chunk[args.Handle]; !ok {
		reply.Error = gfs.ErrNoSuchHandle
		log.Errorf("chunkserver[%v], RPCSendCopy, err[%v]", cs.address, reply.Error)
		return nil
	}

	info := cs.chunk[args.Handle]
	info.RLock()
	defer info.RUnlock()

	rpcArgs := gfs.ApplyCopyArg{Handle: args.Handle, Data: data[:readLen], Version: info.version}
	rpcReply := new(gfs.ApplyCopyReply)
	err = util.Call(args.Address, "ChunkServer.RPCApplyCopy", rpcArgs, rpcReply)
	if err != nil {
		log.Errorf("chunkserver[%v], RPCSendCopy, call err[%v]", cs.address, err)
		reply.Error = err
	} else if rpcReply.Error != nil {
		log.Errorf("chunkserver[%v], RPCSendCopy, err[%v]", cs.address, rpcReply.Error)
		reply.Error = rpcReply.Error
	}

	return nil
}

// RPCApplyCopy is called by another replica
// rewrite the local version to given copy data
func (cs *ChunkServer) RPCApplyCopy(args gfs.ApplyCopyArg, reply *gfs.ApplyCopyReply) error {
	log.Infof("chunkserver[%v], RPCApplyCopy, handle[%v], version[%v]", cs.address, args.Handle, args.Version)

	cs.Lock()
	defer cs.Unlock()

	if _, ok := cs.chunk[args.Handle]; ok {
		reply.Error = gfs.ErrChunkExists
		log.Errorf("chunkserver[%v], RPCApplyCopy, err[%v]", cs.address, reply.Error)
		return nil
	}

	chunkPath := path.Join(cs.serverRoot, strconv.FormatInt(int64(args.Handle), 10))
	fp, err := os.OpenFile(chunkPath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		reply.Error = gfs.NewErrorf("open file err[%v]", cs.address, err)
		log.Errorf("chunkserver[%v], RPCApplyCopy, err[%v]", cs.address, reply.Error)
		return nil
	}

	defer fp.Close()

	dataLen := len(args.Data)
	n, err := fp.WriteAt(args.Data, 0)
	if err != nil {
		log.Errorf("chunkserver[%v], RPCApplyCopy, err[%v]", cs.address, err)
		reply.Error = gfs.NewErrorf("write file err[%v]", cs.address, err)
		return nil
	}

	if n != dataLen {
		reply.Error = gfs.ErrWriteIncomplete
		log.Errorf("chunkserver[%v], RPCApplyCopy, err[%v]", cs.address, err)
		return nil
	}

	log.Infof("chunkserver[%v], RPCApplyCopy, write[%s], offset[%v]", cs.address, chunkPath, 0)

	cs.chunk[args.Handle] = new(chunkInfo)
	info := cs.chunk[args.Handle]
	info.Lock()
	defer info.Unlock()

	info.length = gfs.Offset(dataLen)
	info.version = args.Version

	return nil
}
