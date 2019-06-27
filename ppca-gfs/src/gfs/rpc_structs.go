package gfs

import (
	"time"
)

// ------ ChunkServer
type GrantLeaseArg struct {
	Handle  ChunkHandle
	Version ChunkVersion
}
type GrantLeaseReply struct {
	NewestVersion ChunkVersion
	Error         error
}

type GetChunksArg struct {
}
type GetChunksReply struct {
	Chunks []CSChunkInfo
	Error  error
}

type PushDataAndForwardArg struct {
	Handle    ChunkHandle
	DataID    *DataBufferID
	Data      []byte
	ForwardTo []ServerAddress
}
type PushDataAndForwardReply struct {
	DataID DataBufferID
	Error  error
}

type ForwardDataArg struct {
	DataID DataBufferID
	Data   []byte
}
type ForwardDataReply struct {
	Error error
}

type CreateChunkArg struct {
	Handle ChunkHandle
}
type CreateChunkReply struct {
	Error error
}

type WriteChunkArg struct {
	DataID      DataBufferID
	Offset      Offset
	Secondaries []ServerAddress
	Version     ChunkVersion
}
type WriteChunkReply struct {
	Error error
}

type AppendChunkArg struct {
	DataID      DataBufferID
	Secondaries []ServerAddress
	Version     ChunkVersion
}
type AppendChunkReply struct {
	Offset Offset
	Error  error
}

type ApplyMutationArg struct {
	Mtype   MutationType
	Version ChunkVersion
	DataID  DataBufferID
	Offset  Offset
}
type ApplyMutationReply struct {
	Error error
}

type PadChunkArg struct {
	Handle ChunkHandle
}
type PadChunkReply struct {
	Error error
}

type ReadChunkArg struct {
	Handle ChunkHandle
	Offset Offset
	Length int
}
type ReadChunkReply struct {
	Data   []byte
	Length int
	Error  error
}

type SendCopyArg struct {
	Handle  ChunkHandle
	Address ServerAddress
}
type SendCopyReply struct {
	Error error
}

type ApplyCopyArg struct {
	Handle  ChunkHandle
	Data    []byte
	Version ChunkVersion
}
type ApplyCopyReply struct {
	Error error
}

// ------ Master

type HeartbeatArg struct {
	Address         ServerAddress // chunkserver address
	LeaseExtensions []ChunkHandle // leases to be extended
}
type HeartbeatReply struct {
	Error error
}

type GetPrimaryAndSecondariesArg struct {
	Handle ChunkHandle
}
type GetPrimaryAndSecondariesReply struct {
	Primary     ServerAddress
	Expire      time.Time
	Secondaries []ServerAddress
	Version     ChunkVersion
	Error       error
}

type ExtendLeaseArg struct {
	Handle  ChunkHandle
	Address ServerAddress
}
type ExtendLeaseReply struct {
	Expire time.Time
	Error  error
}

type GetReplicasArg struct {
	Handle ChunkHandle
}
type GetReplicasReply struct {
	Locations []ServerAddress
	Error     error
}

type GetFileInfoArg struct {
	Path Path
}
type GetFileInfoReply struct {
	IsDir  bool
	Length int64
	Chunks int64
	Error  error
}

type CreateFileArg struct {
	Path Path
}
type CreateFileReply struct {
	Error error
}

type DeleteFileArg struct {
	Path Path
}
type DeleteFileReply struct {
	Error error
}

type MkdirArg struct {
	Path Path
}
type MkdirReply struct {
	Error error
}

type ListArg struct {
	Path Path
}
type ListReply struct {
	Files []PathInfo
	Error error
}

type GetChunkHandleArg struct {
	Path  Path
	Index ChunkIndex
}
type GetChunkHandleReply struct {
	Handle ChunkHandle
	Error  error
}
