package client

import (
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
func (c *Client) Create(path gfs.Path) error {
	return nil
}

// Mkdir creates a new directory on GFS.
func (c *Client) Mkdir(path gfs.Path) error {
	return nil
}

// List lists everything in specific directory on GFS.
func (c *Client) List(path gfs.Path) ([]gfs.PathInfo, error) {
	return nil, nil
}

// Read reads the file at specific offset.
// It reads up to len(data) bytes form the File.
// It return the number of bytes, and an error if any.
func (c *Client) Read(path gfs.Path, offset gfs.Offset, data []byte) (n int, err error) {
	return 0, nil
}

// Write writes data to the file at specific offset.
func (c *Client) Write(path gfs.Path, offset gfs.Offset, data []byte) error {
	return nil
}

// Append appends data to the file. Offset of the beginning of appended data is returned.
func (c *Client) Append(path gfs.Path, data []byte) (offset gfs.Offset, err error) {
	return 0, nil
}

// GetChunkHandle returns the chunk handle of (path, index).
// If the chunk doesn't exist, master will create one.
func (c *Client) GetChunkHandle(path gfs.Path, index gfs.ChunkIndex) (handle gfs.ChunkHandle, err error) {
	rpcArgs := gfs.GetChunkHandleArg{Path: path, Index: index}
	rpcReply := new(gfs.GetChunkHandleReply)
	err = util.Call(c.master, "Master.RPCGetChunkHandle", rpcArgs, rpcReply)
	if err != nil {
		log.Errorf("GetChunkHandle, err[%s]", err)
		return
	}

	handle = rpcReply.Handle
	return
}

// ReadChunk reads data from the chunk at specific offset.
// len(data)+offset  should be within chunk size.
func (c *Client) ReadChunk(handle gfs.ChunkHandle, offset gfs.Offset, data []byte) (n int, err error) {
	rpcArgs := gfs.GetReplicasArg{Handle: handle}
	rpcReply := new(gfs.GetReplicasReply)

	err = util.Call(c.master, "Master.RPCGetReplicas", rpcArgs, rpcReply)
	if err != nil {
		log.Errorf("ReadChunk, err[%s]", err)
		return
	}

	replicasLen := len(rpcReply.Locations)
	if replicasLen == 0 {
		err = gfs.ErrNoReplicas
		log.Errorf("ReadChunk, err[%s]", err)
		return
	}

	samples, err := util.Sample(len(rpcReply.Locations), 1)
	idx := samples[0]
	addr := rpcReply.Locations[idx]

	rpcRCArgs := gfs.ReadChunkArg{Handle: handle, Offset: offset, Length: cap(data)}
	rpcRCReply := new(gfs.ReadChunkReply)

	err = util.Call(addr, "ChunkServer.RPCReadChunk", rpcRCArgs, rpcRCReply)
	if err != nil {
		log.Errorf("ReadChunk, err[%s]", err)
		return
	}

	for i := range rpcRCReply.Data {
		data[i] = rpcRCReply.Data[i]
	}

	n = rpcRCReply.Length

	if n != len(data) {
		err = gfs.ErrReadIncomplete
		log.Errorf("ReadChunk, err[%s]", err)
		return
	}

	return
}

// WriteChunk writes data to the chunk at specific offset.
// len(data)+offset should be within chunk size.
func (c *Client) WriteChunk(handle gfs.ChunkHandle, offset gfs.Offset, data []byte) error {
	lease, err := c.leases.getChunkLease(handle)
	if err != nil {
		log.Errorf("WriteChunk, err[%s]", err)
		return err
	}

	rpcPDAFArgs := gfs.PushDataAndForwardArg{Handle: handle, Data: data, ForwardTo: lease.Secondaries}
	rpcPDAFReply := new(gfs.PushDataAndForwardReply)
	if err := util.Call(lease.Primary, "ChunkServer.RPCPushDataAndForward", rpcPDAFArgs, rpcPDAFReply); err != nil {
		log.Errorf("WriteChunk, err[%s]", err)
		return err
	}

	rpcWArgs := gfs.WriteChunkArg{DataID: rpcPDAFReply.DataID, Offset: offset, Secondaries: lease.Secondaries}
	rpcWReply := new(gfs.WriteChunkReply)
	if err := util.Call(lease.Primary, "ChunkServer.RPCWriteChunk", rpcWArgs, rpcWReply); err != nil {
		log.Errorf("WriteChunk, err[%s]", err)
		return err
	}

	return nil
}

// AppendChunk appends data to a chunk.
// Chunk offset of the start of data will be returned if success.
// len(data) should be within max append size.
func (c *Client) AppendChunk(handle gfs.ChunkHandle, data []byte) (offset gfs.Offset, err error) {
	return 0, nil
}
