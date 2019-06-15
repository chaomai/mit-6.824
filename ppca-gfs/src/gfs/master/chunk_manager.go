package master

import (
	"gfs"
	"gfs/util"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
)

// chunkManager manges chunks
type chunkManager struct {
	sync.RWMutex

	chunk map[gfs.ChunkHandle]*chunkInfo
	file  map[gfs.Path]*fileInfo

	numChunkHandle gfs.ChunkHandle
}

type chunkInfo struct {
	sync.RWMutex
	location util.ArraySet     // set of replica locations
	primary  gfs.ServerAddress // primary chunkserver
	expire   time.Time         // lease expire time
	path     gfs.Path
}

type fileInfo struct {
	handles []gfs.ChunkHandle
}

type lease struct {
	primary     gfs.ServerAddress
	expire      time.Time
	secondaries []gfs.ServerAddress
}

func newChunkManager() *chunkManager {
	cm := &chunkManager{
		chunk: make(map[gfs.ChunkHandle]*chunkInfo),
		file:  make(map[gfs.Path]*fileInfo),
	}
	return cm
}

// RegisterReplica adds a replica for a chunk
func (cm *chunkManager) RegisterReplica(handle gfs.ChunkHandle, addr gfs.ServerAddress) error {
	return nil
}

// GetReplicas returns the replicas of a chunk
func (cm *chunkManager) GetReplicas(handle gfs.ChunkHandle) (*util.ArraySet, error) {
	return nil, nil
}

// GetChunk returns the chunk handle for (path, index).
func (cm *chunkManager) GetChunk(path gfs.Path, index gfs.ChunkIndex) (gfs.ChunkHandle, error) {
	cm.RLock()
	defer cm.RUnlock()

	if fInfo, ok := cm.file[path]; ok {
		handles := fInfo.handles
		return handles[index], nil
	}

	log.Errorf("GetChunk, file[%s], err[%s]", path, errNoChunks)
	return 0, errNoChunks
}

// GetLeaseHolder returns the chunkserver that hold the lease of a chunk
// (i.e. primary) and expire time of the lease. If no one has a lease,
// grants one to a replica it chooses.
func (cm *chunkManager) GetLeaseHolder(handle gfs.ChunkHandle) (*lease, error) {
	return nil, nil
}

// ExtendLease extends the lease of chunk if the lease holder is primary.
func (cm *chunkManager) ExtendLease(handle gfs.ChunkHandle, primary gfs.ServerAddress) error {
	cm.RLock()
	defer cm.RUnlock()

	if _, ok := cm.chunk[handle]; !ok {
		log.Errorf("ExtendLease, handle[%d], err[%s]", handle, errNoSuchHandle)
		return errNoSuchHandle
	}

	info := cm.chunk[handle]

	info.Lock()
	defer info.Unlock()

	info.expire = time.Now().Add(gfs.LeaseExpire)

	return nil
}

// CreateChunk creates a new chunk for path.
func (cm *chunkManager) CreateChunk(path gfs.Path, addrs []gfs.ServerAddress) (handle gfs.ChunkHandle, err error) {
	cm.Lock()
	defer cm.Unlock()

	handle = cm.numChunkHandle

	rpcArgs := gfs.CreateChunkArg{Handle: handle}
	rpcReply := new(gfs.CreateChunkReply)
	cInfo := new(chunkInfo)
	cInfo.path = path
	for _, addr := range addrs {
		err = util.Call(addr, "ChunkServer.RPCCreateChunk", rpcArgs, rpcReply)
		if err != nil {
			log.Errorf("CreateChunk, addr[%s], err[%s]", addr, err)
			return
		}

		cInfo.location.Add(addr)
	}

	fInfo := new(fileInfo)
	fInfo.handles = append(fInfo.handles, handle)

	cm.numChunkHandle = handle + 1
	cm.chunk[handle] = cInfo
	cm.file[path] = fInfo

	return 0, nil
}
