package gfs

import (
	"encoding/gob"
	"fmt"
	"net"
	"time"
)

type Path string
type ServerAddress string
type Offset int64
type ChunkIndex int
type ChunkHandle int64
type ChunkVersion int64
type ChunkCheckSum uint32

type DataBufferID struct {
	Handle    ChunkHandle
	TimeStamp int
}

type PathInfo struct {
	Name string

	Path string

	// if it is a directory
	IsDir bool

	// if it is a file
	Length int64
	Chunks int64
}

type MutationType int

// Lease info
type Lease struct {
	Primary     ServerAddress
	Expire      time.Time
	Secondaries []ServerAddress
	Version     ChunkVersion
}

type CSChunkInfo struct {
	Handle   ChunkHandle
	Length   Offset
	Version  ChunkVersion // version number of the chunk in disk
	CheckSum []ChunkCheckSum
}

type ReReplicationInfo struct {
	Handle      ChunkHandle
	CurReplicas []ServerAddress
}

const (
	MutationWrite = iota
	MutationAppend
	MutationPad
)

var (
	ErrNoNeedForReReplication     = NewError("no need to re-replication")
	ErrChunkExists                = NewError("chunk exists")
	ErrDirectoryExists            = NewError("directory exists")
	ErrCreateDiscontinuousChunk   = NewError("discontinuous chunk should not be created")
	ErrFileExists                 = NewError("file exists")
	ErrFileNotExists              = NewError("file doesn't exists")
	ErrNoChunks                   = NewError("no chunks")
	ErrNoEnoughServersForReplicas = NewError("no enough servers for replicas")
	ErrNoReplicas                 = NewError("no replicas")
	ErrNoSuchDataID               = NewError("no such data ID")
	ErrNoSuchHandle               = NewError("no such handle")
	ErrNoSuchServer               = NewError("no such server")
	ErrPathIsNotDirectory         = NewError("path isn't a directory")
	ErrPathIsNotFile              = NewError("path isn't a file")
	ErrPathNotExists              = NewError("path doesn't exist")
	ErrReadEOF                    = NewError("read eof")
	ErrReadExceedFileSize         = NewError("read exceed file size")
	ErrReadExceedChunkSize        = NewError("read exceed chunk size")
	ErrReadIncomplete             = NewError("read incomplete")
	ErrWriteExceedChunkSize       = NewError("write exceed chunk size")
	ErrWriteExceedFileSize        = NewError("write exceed file size")
	ErrWriteIncomplete            = NewError("write incomplete")
	ErrAppendExceedChunkSize      = NewError("append exceed chunk size")
	ErrAppendExceedMaxAppendSize  = NewError("append exceed max append size")
	ErrStaleVersionAtMaster       = NewError("version is stale as master")
	ErrStaleVersionAtChunkServer  = NewError("version is stale as chunkserver")
)

// error type
type Error interface {
	Error() string
}

type GError struct {
	Err string
}

func NewError(msg string) Error {
	return GError{Err: msg}
}

func NewErrorf(format string, a ...interface{}) Error {
	msg := fmt.Sprintf(format, a)
	return GError{Err: msg}
}

func (e GError) Error() string {
	return e.Err
}

// system config
const (
	LeaseExpire        = 2 * time.Second
	HeartbeatInterval  = 100 * time.Millisecond
	BackgroundInterval = 200 * time.Millisecond
	ServerTimeout      = 1 * time.Second

	MaxChunkSize  = 512 << 10 // 512KB DEBUG ONLY 64 << 20
	MaxAppendSize = MaxChunkSize / 4

	DefaultNumReplicas = 3
	MinimumNumReplicas = 2

	DownloadBufferExpire = 2 * time.Minute
	DownloadBufferTick   = 10 * time.Second

	ClientMaxRetry  = 3
	ClientRetryWait = ServerTimeout

	ChunkServerMetaFileName        = "gfs-server.meta"
	NamespaceManagerMetaFileName   = "gfs-server.nm.meta"
	ChunkManagerMetaFileName       = "gfs-server.cm.meta"
	ChunkServerManagerMetaFileName = "gfs-server.csm.meta"
	DefaultFilePerm                = 0755
)

func init() {
	gob.Register(ErrChunkExists)
	gob.Register(net.OpError{})
}
