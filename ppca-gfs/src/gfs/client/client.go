package client

import (
	"math"

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
	if errx := util.Call(c.master, "Master.RPCCreateFile", rpcArgs, rpcReply); errx != nil {
		log.Errorf("Create, call err[%v]", errx)
		err = errx
		return
	} else if rpcReply.Error != nil {
		log.Errorf("Create, err[%v]", rpcReply.Error)
		err = rpcReply.Error
		return
	}

	return
}

// Mkdir creates a new directory on GFS.
func (c *Client) Mkdir(path gfs.Path) (err error) {
	rpcArgs := gfs.MkdirArg{Path: path}
	rpcReply := new(gfs.MkdirReply)
	if errx := util.Call(c.master, "Master.RPCMkdir", rpcArgs, rpcReply); errx != nil {
		log.Errorf("Mkdir, call err[%v]", errx)
		err = errx
		return
	} else if rpcReply.Error != nil {
		log.Errorf("Mkdir, err[%v]", rpcReply.Error)
		err = rpcReply.Error
		return
	}

	return
}

// List lists everything in specific directory on GFS.
func (c *Client) List(path gfs.Path) (files []gfs.PathInfo, err error) {
	rpcArgs := gfs.ListArg{Path: path}
	rpcReply := new(gfs.ListReply)
	if errx := util.Call(c.master, "Master.RPCList", rpcArgs, rpcReply); errx != nil {
		log.Errorf("List, call err[%v]", errx)
		err = errx
		return
	} else if rpcReply.Error != nil {
		log.Errorf("List, err[%v]", rpcReply.Error)
		err = rpcReply.Error
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
	if errx := util.Call(c.master, "Master.RPCGetFileInfo", rpcArgs, rpcReply); errx != nil {
		err = errx
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
		handle, errx := c.GetChunkHandle(path, readOffsetChunkIdx)
		if errx != nil {
			err = errx
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

		nRead, errx := c.ReadChunk(handle, fileOffset, data[0:readLen])
		readOffset += gfs.Offset(nRead)
		n += nRead

		if errx == gfs.ErrReadEOF {
			err = errx
			log.Warnf("Read, err[%v]", err)
			return
		} else if errx != nil {
			err = errx
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
func (c *Client) Write(path gfs.Path, offset gfs.Offset, data []byte) error {
	rpcArgs := gfs.GetFileInfoArg{Path: path}
	rpcReply := new(gfs.GetFileInfoReply)
	if err := util.Call(c.master, "Master.RPCGetFileInfo", rpcArgs, rpcReply); err != nil {
		log.Errorf("Write, call err[%v]", err)
		return err
	} else if rpcReply.Error != nil {
		log.Errorf("Write, err[%v]", rpcReply.Error)
		return rpcReply.Error
	}

	var maxChunkIdx gfs.ChunkIndex = 0
	if rpcReply.Chunks != 0 {
		maxChunkIdx = gfs.ChunkIndex(rpcReply.Chunks - 1)
	}

	writeOffset := offset
	for {
		var handle gfs.ChunkHandle
		handle, err := c.GetChunkHandle(path, maxChunkIdx)
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
			return err
		}

		data = data[writeLen:]
		writeOffset += writeLen

		if len(data) == 0 {
			break
		}
	}

	return nil
}

// Append appends data to the file. Offset of the beginning of appended data is returned.
func (c *Client) Append(path gfs.Path, data []byte) (offset gfs.Offset, err error) {
	rpcArgs := gfs.GetFileInfoArg{Path: path}
	rpcReply := new(gfs.GetFileInfoReply)
	if errx := util.Call(c.master, "Master.RPCGetFileInfo", rpcArgs, rpcReply); errx != nil {
		log.Errorf("Append, call err[%v]", errx)
		err = errx
		return
	} else if rpcReply.Error != nil {
		log.Errorf("Append, err[%v]", rpcReply.Error)
		err = rpcReply.Error
		return
	}

	var curChunkIndex int64 = 0
	if rpcReply.Chunks != 0 {
		curChunkIndex = rpcReply.Chunks - 1
	}

	// todo split data to append
	for {
		var handle gfs.ChunkHandle
		handle, err = c.GetChunkHandle(path, gfs.ChunkIndex(curChunkIndex))
		if err != nil {
			log.Errorf("Append, err[%v]", err)
			return
		}

		var cOffset gfs.Offset
		cOffset, err = c.AppendChunk(handle, data)
		if err == nil {
			offset = gfs.Offset(curChunkIndex*gfs.MaxChunkSize) + cOffset
			break
		} else if err == gfs.ErrAppendExceedChunkSize {
			log.Infof("Append, err[%v]", err)
			curChunkIndex++
		} else {
			log.Errorf("Append, err[%v]", err)
			return
		}
	}

	return
}

// GetChunkHandle returns the chunk handle of (path, index).
// If the chunk doesn't exist, master will create one.
func (c *Client) GetChunkHandle(path gfs.Path, index gfs.ChunkIndex) (handle gfs.ChunkHandle, err error) {
	rpcArgs := gfs.GetChunkHandleArg{Path: path, Index: index}
	rpcReply := new(gfs.GetChunkHandleReply)
	if errx := util.Call(c.master, "Master.RPCGetChunkHandle", rpcArgs, rpcReply); errx != nil {
		log.Errorf("GetChunkHandle, call err[%v]", errx)
		err = errx
		return
	} else if rpcReply.Error != nil {
		log.Errorf("GetChunkHandle, err[%v]", rpcReply.Error)
		err = rpcReply.Error
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

	if errx := util.Call(c.master, "Master.RPCGetReplicas", rpcArgs, rpcReply); errx != nil {
		log.Errorf("ReadChunk, call err[%v]", errx)
		err = errx
		return
	} else if rpcReply.Error != nil {
		log.Errorf("ReadChunk, err[%v]", rpcReply.Error)
		err = rpcReply.Error
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

	if errx := util.Call(addr, "ChunkServer.RPCReadChunk", rpcRCArgs, rpcRCReply); errx != nil {
		log.Errorf("ReadChunk, call err[%v]", errx)
		err = errx
		return
	} else if rpcRCReply.Error == gfs.ErrReadEOF {
		log.Warnf("ReadChunk, err[%v]", rpcRCReply.Error)
		err = rpcRCReply.Error
	} else if rpcRCReply.Error != nil {
		log.Errorf("ReadChunk, err[%v]", rpcRCReply.Error)
		err = rpcRCReply.Error
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
	if int64(offset)+int64(len(data)) > gfs.MaxChunkSize {
		err = gfs.ErrWriteExceedChunkSize
		log.Errorf("WriteChunk, err[%v]", err)
		return
	}

	lease, err := c.leases.getChunkLease(handle)
	if err != nil {
		log.Errorf("WriteChunk, err[%v]", err)
		return
	}

	rpcPDAFArgs := gfs.PushDataAndForwardArg{Handle: handle, Data: data, ForwardTo: lease.Secondaries}
	rpcPDAFReply := new(gfs.PushDataAndForwardReply)
	if errx := util.Call(lease.Primary, "ChunkServer.RPCPushDataAndForward", rpcPDAFArgs, rpcPDAFReply); errx != nil {
		log.Errorf("WriteChunk, call err[%v]", errx)
		err = errx
		return
	} else if rpcPDAFReply.Error != nil {
		log.Errorf("WriteChunk, err[%v]", rpcPDAFReply.Error)
		err = rpcPDAFReply.Error
		return
	}

	rpcWArgs := gfs.WriteChunkArg{DataID: rpcPDAFReply.DataID, Offset: offset, Secondaries: lease.Secondaries, Version: lease.Version}
	rpcWReply := new(gfs.WriteChunkReply)
	if errx := util.Call(lease.Primary, "ChunkServer.RPCWriteChunk", rpcWArgs, rpcWReply); errx != nil {
		log.Errorf("WriteChunk, call err[%v]", errx)
		err = errx
		return
	} else if rpcWReply.Error != nil {
		log.Errorf("WriteChunk, err[%v]", rpcWReply.Error)
		err = rpcWReply.Error
		return
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

	lease, err := c.leases.getChunkLease(handle)
	if err != nil {
		log.Errorf("AppendChunk, err[%v]", err)
		return
	}

	rpcPDAFArgs := gfs.PushDataAndForwardArg{Handle: handle, Data: data, ForwardTo: lease.Secondaries}
	rpcPDAFReply := new(gfs.PushDataAndForwardReply)
	if errx := util.Call(lease.Primary, "ChunkServer.RPCPushDataAndForward", rpcPDAFArgs, rpcPDAFReply); errx != nil {
		log.Errorf("AppendChunk, call err[%v]", errx)
		err = errx
		return
	} else if rpcPDAFReply.Error != nil {
		log.Errorf("AppendChunk, err[%v]", rpcPDAFReply.Error)
		err = rpcPDAFReply.Error
		return
	}

	rpcAArgs := gfs.AppendChunkArg{DataID: rpcPDAFReply.DataID, Secondaries: lease.Secondaries, Version: lease.Version}
	rpcAReply := new(gfs.AppendChunkReply)
	if errx := util.Call(lease.Primary, "ChunkServer.RPCAppendChunk", rpcAArgs, rpcAReply); errx != nil {
		log.Errorf("AppendChunk, call err[%v]", errx)
		err = errx
		return
	} else if rpcAReply.Error != nil {
		log.Errorf("AppendChunk, err[%v]", rpcAReply.Error)
		err = rpcAReply.Error
		return
	}

	offset = rpcAReply.Offset

	return
}
