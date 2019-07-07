package client

import (
	"math"
	"time"

	"gfs"
	"gfs/util"

	log "github.com/Sirupsen/logrus"
)

// Client struct is the GFS client-side driver
type Client struct {
	master gfs.ServerAddress
	leases *chunkLease
}

// NewClient returns a new gfs client.
func NewClient(m gfs.ServerAddress) *Client {
	return &Client{
		master: m,
		leases: newChunkLease(m),
	}
}

// Create creates a new file on the specific path on GFS.
func (c *Client) Create(path gfs.Path) (err error) {
	rpcArgs := gfs.CreateFileArg{Path: path}
	rpcReply := new(gfs.CreateFileReply)
	err = util.Call(c.master, "Master.RPCCreateFile", rpcArgs, rpcReply)
	if err != nil {
		log.Errorf("Create, call err[%v]", err)
		return
	} else if rpcReply.Error != nil {
		err = rpcReply.Error
		log.Errorf("Create, err[%v]", err)
		return
	}

	return
}

// Mkdir creates a new directory on GFS.
func (c *Client) Mkdir(path gfs.Path) (err error) {
	rpcArgs := gfs.MkdirArg{Path: path}
	rpcReply := new(gfs.MkdirReply)
	err = util.Call(c.master, "Master.RPCMkdir", rpcArgs, rpcReply)
	if err != nil {
		log.Errorf("Mkdir, call err[%v]", err)
		return
	} else if rpcReply.Error != nil {
		err = rpcReply.Error
		log.Errorf("Mkdir, err[%v]", err)
		return
	}

	return
}

// List lists everything in specific directory on GFS.
func (c *Client) List(path gfs.Path) (files []gfs.PathInfo, err error) {
	rpcArgs := gfs.ListArg{Path: path}
	rpcReply := new(gfs.ListReply)
	err = util.Call(c.master, "Master.RPCList", rpcArgs, rpcReply)
	if err != nil {
		log.Errorf("List, call err[%v]", err)
		return
	} else if rpcReply.Error != nil {
		err = rpcReply.Error
		log.Errorf("List, err[%v]", err)
		return
	}

	files = rpcReply.Files

	return
}

func (c *Client) getOffsetChunkIndex(offset gfs.Offset) gfs.ChunkIndex {
	// offset start from zero, so add 1 before performing calculation
	return gfs.ChunkIndex(math.Floor(float64(offset+1) / float64(gfs.MaxChunkSize)))
}

// Read reads the file at specific offset.
// It reads up to len(data) bytes form the File.
// It return the number of bytes, and an error if any.
func (c *Client) Read(path gfs.Path, offset gfs.Offset, data []byte) (n int, err error) {
	rpcArgs := gfs.GetFileInfoArg{Path: path}
	rpcReply := new(gfs.GetFileInfoReply)
	err = util.Call(c.master, "Master.RPCGetFileInfo", rpcArgs, rpcReply)
	if err != nil {
		log.Errorf("Read, call err[%v]", err)
		return
	} else if rpcReply.Error != nil {
		err = rpcReply.Error
		log.Errorf("Read, err[%v]", err)
		return
	}

	readOffset := offset
	for {
		readOffsetChunkIdx := c.getOffsetChunkIndex(readOffset)

		var handle gfs.ChunkHandle
		handle, err = c.GetChunkHandle(path, readOffsetChunkIdx)
		if err != nil {
			log.Errorf("Read, err[%v]", err)
			return
		}

		fileOffset := readOffset - gfs.Offset(readOffsetChunkIdx)*gfs.MaxChunkSize

		var readLen gfs.Offset
		if fileOffset+gfs.Offset(len(data)) > gfs.MaxChunkSize {
			readLen = gfs.Offset(gfs.MaxChunkSize - fileOffset)
		} else {
			readLen = gfs.Offset(len(data))
		}

		var nRead int
		nRead, err = c.ReadChunk(handle, fileOffset, data[0:readLen])
		readOffset += gfs.Offset(nRead)
		n += nRead

		if err == gfs.ErrReadEOF {
			log.Warnf("Read, err[%v]", err)
			return
		} else if err != nil {
			log.Errorf("Read, err[%v]", err)
			return
		}

		data = data[nRead:]

		if len(data) == 0 {
			break
		}
	}

	return
}

// Write writes data to the file at specific offset.
func (c *Client) Write(path gfs.Path, offset gfs.Offset, data []byte) (err error) {
	rpcArgs := gfs.GetFileInfoArg{Path: path}
	rpcReply := new(gfs.GetFileInfoReply)
	err = util.Call(c.master, "Master.RPCGetFileInfo", rpcArgs, rpcReply)
	if err != nil {
		log.Errorf("Write, call err[%v]", err)
		return
	} else if rpcReply.Error != nil {
		err = rpcReply.Error
		log.Errorf("Write, err[%v]", err)
		return
	}

	var maxChunkIdx gfs.ChunkIndex = 0
	if rpcReply.Chunks != 0 {
		maxChunkIdx = gfs.ChunkIndex(rpcReply.Chunks - 1)
	}

	retry := 0
	writeOffset := offset

	for {
		var handle gfs.ChunkHandle
		handle, err = c.GetChunkHandle(path, maxChunkIdx)
		if err != nil {
			log.Errorf("Write, err[%v]", err)
			return err
		}

		if c.getOffsetChunkIndex(writeOffset) > maxChunkIdx {
			maxChunkIdx++
			continue
		}

		fileOffset := writeOffset - gfs.Offset(maxChunkIdx)*gfs.MaxChunkSize

		var writeLen gfs.Offset
		if fileOffset+gfs.Offset(len(data)) > gfs.MaxChunkSize {
			writeLen = gfs.Offset(gfs.MaxChunkSize - fileOffset)
		} else {
			writeLen = gfs.Offset(len(data))
		}

		log.Infof("Write, handle[%v], fileOffset[%v], writeLen[%v]", handle, fileOffset, writeLen)

		err = c.WriteChunk(handle, fileOffset, data[0:writeLen])
		if err != nil {
			log.Errorf("Write, err[%v]", err)

			if retry != gfs.ClientMaxRetry {
				retry++
				continue
			}

			log.Errorf("Write, finally failed, retry[%v]", retry)

			return
		}

		data = data[writeLen:]
		writeOffset += writeLen

		if len(data) == 0 {
			break
		}
	}

	return
}

// Append appends data to the file. Offset of the beginning of appended data is returned.
func (c *Client) Append(path gfs.Path, data []byte) (offset gfs.Offset, err error) {
	rpcArgs := gfs.GetFileInfoArg{Path: path}
	rpcReply := new(gfs.GetFileInfoReply)
	err = util.Call(c.master, "Master.RPCGetFileInfo", rpcArgs, rpcReply)
	if err != nil {
		log.Errorf("Append, call err[%v]", err)
		return
	} else if rpcReply.Error != nil {
		err = rpcReply.Error
		log.Errorf("Append, err[%v]", err)
		return
	}

	var maxChunkIdx gfs.ChunkIndex = 0
	if rpcReply.Chunks != 0 {
		maxChunkIdx = gfs.ChunkIndex(rpcReply.Chunks - 1)
	}

	retry := 0
	needRetry := false
	isSet := false

	for {
		if needRetry {
			if retry != gfs.ClientMaxRetry {
				log.Warnf("Append, retry[%v]", retry)
				time.Sleep(gfs.ClientRetryWait)
				retry++
				needRetry = false
			} else {
				log.Errorf("Append, finally failed, retry[%v]", retry)
				return
			}
		}

		var handle gfs.ChunkHandle
		handle, err = c.GetChunkHandle(path, maxChunkIdx)
		if err != nil {
			log.Errorf("Append, err[%v]", err)
			needRetry = true
			continue
		}

		appendLen := gfs.MaxAppendSize
		if appendLen > len(data) {
			appendLen = len(data)
		}

		var cOffset gfs.Offset
		cOffset, err = c.AppendChunk(handle, data[0:appendLen])
		if err == nil {
			if !isSet {
				offset = gfs.Offset(maxChunkIdx*gfs.MaxChunkSize) + cOffset
				isSet = true
			}
		} else if err == gfs.ErrAppendExceedChunkSize {
			log.Warnf("Append, err[%v]", err)
			maxChunkIdx++
			continue
		} else {
			log.Errorf("Append, err[%v]", err)
			needRetry = true
			continue
		}

		data = data[appendLen:]

		log.Debugf("Append, append data[0:%v], remain[%v:%v]", appendLen, appendLen, len(data))

		if len(data) == 0 {
			break
		}
	}

	return
}

// GetChunkHandle returns the chunk handle of (path, index).
// If the chunk doesn't exist, master will create one.
func (c *Client) GetChunkHandle(path gfs.Path, index gfs.ChunkIndex) (handle gfs.ChunkHandle, err error) {
	rpcArgs := gfs.GetChunkHandleArg{Path: path, Index: index}
	rpcReply := new(gfs.GetChunkHandleReply)
	err = util.Call(c.master, "Master.RPCGetChunkHandle", rpcArgs, rpcReply)
	if err != nil {
		log.Errorf("GetChunkHandle, call err[%v]", err)
		return
	} else if rpcReply.Error != nil {
		err = rpcReply.Error
		log.Errorf("GetChunkHandle, err[%v]", err)
		return
	}

	handle = rpcReply.Handle
	return
}

// ReadChunk reads data from the chunk at specific offset.
// len(data)+offset should be within chunk size.
func (c *Client) ReadChunk(handle gfs.ChunkHandle, offset gfs.Offset, data []byte) (n int, err error) {
	if int64(offset)+int64(len(data)) > gfs.MaxChunkSize {
		err = gfs.ErrReadExceedChunkSize
		log.Errorf("ReadChunk, err[%v]", err)
		return
	}

	rpcArgs := gfs.GetReplicasArg{Handle: handle}
	rpcReply := new(gfs.GetReplicasReply)

	err = util.Call(c.master, "Master.RPCGetReplicas", rpcArgs, rpcReply)
	if err != nil {
		log.Errorf("ReadChunk, call err[%v]", err)
		return
	} else if rpcReply.Error != nil {
		err = rpcReply.Error
		log.Errorf("ReadChunk, err[%v]", err)
		return
	}

	replicasLen := len(rpcReply.Locations)
	if replicasLen == 0 {
		err = gfs.ErrNoReplicas
		log.Errorf("ReadChunk, err[%v]", err)
		return
	}

	samples, err := util.Sample(len(rpcReply.Locations), 1)
	idx := samples[0]
	addr := rpcReply.Locations[idx]

	var readLen int
	if gfs.MaxChunkSize-offset > gfs.Offset(len(data)) {
		readLen = len(data)
	} else {
		readLen = int(gfs.MaxChunkSize - offset)
	}

	rpcRCArgs := gfs.ReadChunkArg{Handle: handle, Offset: offset, Length: readLen}
	rpcRCReply := new(gfs.ReadChunkReply)
	rpcRCReply.Data = make([]byte, readLen)

	err = util.Call(addr, "ChunkServer.RPCReadChunk", rpcRCArgs, rpcRCReply)
	if err != nil {
		log.Errorf("ReadChunk, call err[%v]", err)
		return
	} else if rpcRCReply.Error == gfs.ErrReadEOF {
		err = rpcRCReply.Error
		log.Warnf("ReadChunk, err[%v]", err)
	} else if rpcRCReply.Error != nil {
		err = rpcRCReply.Error
		log.Errorf("ReadChunk, err[%v]", err)
		return
	}

	for i := range rpcRCReply.Data {
		data[i] = rpcRCReply.Data[i]
	}

	n = rpcRCReply.Length

	return
}

// WriteChunk writes data to the chunk at specific offset.
// len(data)+offset should be within chunk size.
func (c *Client) WriteChunk(handle gfs.ChunkHandle, offset gfs.Offset, data []byte) (err error) {
	for i := 0; i < gfs.ClientMaxRetry; i++ {
		// if i != 0 {
		// log.Infof("AppendChunk, full retry[%v], wait[%v]", i, gfs.ClientRetryWait)
		// 	time.Sleep(gfs.ClientRetryWait)
		// }

		if int64(offset)+int64(len(data)) > gfs.MaxChunkSize {
			err = gfs.ErrWriteExceedChunkSize
			log.Errorf("WriteChunk, err[%v]", err)
			return
		}

		var lease *gfs.Lease
		lease, err = c.leases.getChunkLease(handle)
		if err != nil {
			log.Errorf("WriteChunk, err[%v]", err)
			continue
		}

		for j := 0; j < gfs.ClientMaxRetry; j++ {
			// if j != 0 {
			// log.Infof("AppendChunk, retry[%v], wait[%v]", j, gfs.ClientRetryWait)
			// 	time.Sleep(gfs.ClientRetryWait)
			// }

			rpcPDAFArgs := gfs.PushDataAndForwardArg{Handle: handle, Data: data, ForwardTo: lease.Secondaries}
			rpcPDAFReply := new(gfs.PushDataAndForwardReply)
			err = util.Call(lease.Primary, "ChunkServer.RPCPushDataAndForward", rpcPDAFArgs, rpcPDAFReply)
			if err != nil {
				c.leases.deleteInvalid(handle)
				log.Errorf("WriteChunk, call err[%v]", err)
				continue
			} else if rpcPDAFReply.Error != nil {
				err = rpcPDAFReply.Error
				log.Errorf("WriteChunk, err[%v]", err)
				continue
			}

			rpcWArgs := gfs.WriteChunkArg{DataID: rpcPDAFReply.DataID, Offset: offset, Secondaries: lease.Secondaries, Version: lease.Version}
			rpcWReply := new(gfs.WriteChunkReply)
			err = util.Call(lease.Primary, "ChunkServer.RPCWriteChunk", rpcWArgs, rpcWReply)
			if err != nil {
				c.leases.deleteInvalid(handle)
				log.Errorf("WriteChunk, call err[%v]", err)
				continue
			} else if rpcWReply.Error != nil {
				err = rpcWReply.Error
				log.Errorf("WriteChunk, err[%v]", err)
				continue
			}

			return
		}
	}

	return
}

// AppendChunk appends data to a chunk.
// Chunk offset of the start of data will be returned if success.
// len(data) should be within max append size.
func (c *Client) AppendChunk(handle gfs.ChunkHandle, data []byte) (offset gfs.Offset, err error) {
	if len(data) > gfs.MaxAppendSize {
		err = gfs.ErrAppendExceedMaxAppendSize
		log.Errorf("AppendChunk, err[%v]", err)
		return
	}

	var lease *gfs.Lease
	lease, err = c.leases.getChunkLease(handle)
	if err != nil {
		log.Errorf("AppendChunk, err[%v]", err)
		return
	}

	for i := 0; i < gfs.ClientMaxRetry; i++ {
		if i != 0 {
			log.Infof("AppendChunk, retry[%v], wait[%v]", i, gfs.ClientRetryWait)
			time.Sleep(gfs.ClientRetryWait)
		}

		rpcPDAFArgs := gfs.PushDataAndForwardArg{Handle: handle, Data: data, ForwardTo: lease.Secondaries}
		rpcPDAFReply := new(gfs.PushDataAndForwardReply)
		err = util.Call(lease.Primary, "ChunkServer.RPCPushDataAndForward", rpcPDAFArgs, rpcPDAFReply)
		if err != nil {
			c.leases.deleteInvalid(handle)
			log.Errorf("AppendChunk, call err[%v]", err)
			continue
		} else if rpcPDAFReply.Error != nil {
			err = rpcPDAFReply.Error
			log.Errorf("AppendChunk, err[%v]", err)
			continue
		}

		rpcAArgs := gfs.AppendChunkArg{DataID: rpcPDAFReply.DataID, Secondaries: lease.Secondaries, Version: lease.Version}
		rpcAReply := new(gfs.AppendChunkReply)
		err = util.Call(lease.Primary, "ChunkServer.RPCAppendChunk", rpcAArgs, rpcAReply)
		if err != nil {
			c.leases.deleteInvalid(handle)
			log.Errorf("AppendChunk, call err[%v]", err)
			continue
		} else if rpcAReply.Error == gfs.ErrAppendExceedChunkSize {
			err = rpcAReply.Error
			log.Warnf("AppendChunk, err[%v]", err)
			return
		} else if rpcAReply.Error != nil {
			err = rpcAReply.Error
			log.Errorf("AppendChunk, err[%v]", err)
			continue
		}

		offset = rpcAReply.Offset

		return
	}

	return
}
