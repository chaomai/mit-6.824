package gfs

import (
	"errors"
	"time"
)

type Path string
type ServerAddress string
type Offset int64
type ChunkIndex int
type ChunkHandle int64
type ChunkVersion int64

type DataBufferID struct {
	Handle    ChunkHandle
	TimeStamp int
}

type PathInfo struct {
	Name string

	// if it is a directory
	IsDir bool

	// if it is a file
	Length int64
	Chunks int64
}

type MutationType int

const (
	MutationWrite = iota
	MutationAppend
	MutationPad
)

type ErrorCode int

// const (
// Success = iota
// UnknownError
// AppendExceedChunkSize
// WriteExceedChunkSize
// ReadEOF
// NotAvailableForCopy
// )

var (
	ErrAppendExceedChunkSize      = errors.New("append exceed chunk size")
	ErrChunkExists                = errors.New("chunk exists")
	ErrDirectoryExists            = errors.New("directory exists")
	ErrDiscontinuousChunk         = errors.New("discontinuous chunk should not be created")
	ErrFileExists                 = errors.New("file exists")
	ErrFileNotExists              = errors.New("file doesn't exists")
	ErrNoChunks                   = errors.New("no chunks")
	ErrNoEnoughServersForReplicas = errors.New("no enough servers for replicas")
	ErrNoReplicas                 = errors.New("no replicas")
	ErrNoSuchDataID               = errors.New("no such data ID")
	ErrNoSuchHandle               = errors.New("no such handle")
	ErrPathIsNotDirectory         = errors.New("path isn't a directory")
	ErrPathIsNotFile              = errors.New("path isn't a file")
	ErrPathNotExists              = errors.New("path doesn't exist")
	ErrReadEOF                    = errors.New("read eof")
	ErrReadExceedFileSize         = errors.New("read exceed file size")
	ErrReadIncomplete             = errors.New("read incomplete")
	ErrWriteExceedChunkSize       = errors.New("write exceed chunk size")
	ErrWriteIncomplete            = errors.New("write incomplete")
)

// extended error type with error code
type Error struct {
	Code ErrorCode
	Err  string
}

func (e Error) Error() string {
	return e.Err
}

// system config
const (
	// TODO for debug
	LeaseExpire = 1 * time.Minute
	// LeaseExpire        = 2 * time.Second //1 * time.Minute
	HeartbeatInterval  = 100 * time.Millisecond
	BackgroundInterval = 200 * time.Millisecond //
	ServerTimeout      = 1 * time.Second        //

	MaxChunkSize  = 512 << 10 // 512KB DEBUG ONLY 64 << 20
	MaxAppendSize = MaxChunkSize / 4

	DefaultNumReplicas = 3
	MinimumNumReplicas = 2

	DownloadBufferExpire = 2 * time.Minute
	DownloadBufferTick   = 10 * time.Second
)
