package master

import (
	"sync"
	"time"

	"gfs"
	"gfs/util"

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
	version  gfs.ChunkVersion
}

type fileInfo struct {
	handles []gfs.ChunkHandle
}

// Lease info
type Lease struct {
	Primary     gfs.ServerAddress
	Expire      time.Time
	Secondaries []gfs.ServerAddress
	Version     gfs.ChunkVersion
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
func (cm *chunkManager) GetReplicas(handle gfs.ChunkHandle) (loc *util.ArraySet, err error) {
	cm.RLock()
	defer cm.RUnlock()

	if _, ok := cm.chunk[handle]; !ok {
		err = gfs.ErrNoSuchHandle
		log.Errorf("GetReplicas, handle[%d], err[%v]", handle, err)
		return
	}

	info := cm.chunk[handle]
	info.RLock()
	defer info.RUnlock()
	loc = &info.location

	return
}

// GetChunk returns the chunk handle for (path, index).
func (cm *chunkManager) GetChunk(path gfs.Path, index gfs.ChunkIndex) (gfs.ChunkHandle, error) {
	cm.RLock()
	defer cm.RUnlock()

	if fInfo, ok := cm.file[path]; ok {
		handles := fInfo.handles
		return handles[index], nil
	}

	log.Errorf("GetChunk, file[%s], err[%v]", path, gfs.ErrNoChunks)
	return 0, gfs.ErrNoChunks
}

// GetLeaseHolder returns the chunkserver that hold the lease of a chunk
// (i.e. primary) and expire time of the lease. If no one has a lease,
// grants one to a replica it chooses.
func (cm *chunkManager) GetLeaseHolder(handle gfs.ChunkHandle) (l *Lease, err error) {
	cm.RLock()
	defer cm.RUnlock()

	if _, ok := cm.chunk[handle]; !ok {
		err = gfs.ErrNoSuchHandle
		log.Errorf("GetLeaseHolder, handle[%d], err[%v]", handle, err)
		return
	}

	info := cm.chunk[handle]

	info.Lock()
	defer info.Unlock()

	if info.expire.After(time.Now()) {
		l = new(Lease)
		l.Primary = info.primary
		l.Expire = info.expire
		l.Version = info.version

		for _, v := range info.location.GetAll() {
			addr := v.(gfs.ServerAddress)
			if addr != l.Primary {
				l.Secondaries = append(l.Secondaries, addr)
			}
		}

		return
	}

	info.primary = info.location.RandomPick().(gfs.ServerAddress)
	info.expire = time.Now().Add(gfs.LeaseExpire)
	info.version++

	for _, v := range info.location.GetAll() {
		addr := v.(gfs.ServerAddress)

		rpcArgs := gfs.GrantLeaseArg{Handle: handle, Version: info.version}
		rpcReply := new(gfs.GrantLeaseReply)
		errx := util.Call(addr, "ChunkServer.RPCGrantLease", rpcArgs, rpcReply)
		if errx != nil {
			log.Errorf("GetLeaseHolder, call err[%v]", errx)
			err = errx
			return
		} else if rpcReply.Error == gfs.ErrStaleVersionAtMaster {
			log.Warnf("GetLeaseHolder, master chunk version[%s], cs chunk version[%s], err[%v]", info.version, rpcReply.NewestVersion, err)
			info.version = rpcReply.NewestVersion
		} else if rpcReply.Error != nil {
			log.Errorf("GetLeaseHolder, err[%v]", rpcReply.Error)
			err = rpcReply.Error
			return
		}
	}

	l = new(Lease)
	l.Primary = info.primary
	l.Expire = info.expire
	l.Version = info.version

	for _, v := range info.location.GetAll() {
		addr := v.(gfs.ServerAddress)
		if addr != l.Primary {
			l.Secondaries = append(l.Secondaries, addr)
		}
	}

	return
}

// ExtendLease extends the lease of chunk if the lease holder is primary.
func (cm *chunkManager) ExtendLease(handle gfs.ChunkHandle, primary gfs.ServerAddress) error {
	cm.RLock()
	defer cm.RUnlock()

	if _, ok := cm.chunk[handle]; !ok {
		log.Errorf("ExtendLease, handle[%d], err[%v]", handle, gfs.ErrNoSuchHandle)
		return gfs.ErrNoSuchHandle
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
		errx := util.Call(addr, "ChunkServer.RPCCreateChunk", rpcArgs, rpcReply)
		if errx != nil {
			log.Errorf("CreateChunk, call err[%v]", errx)
			err = errx
			return
		} else if rpcReply.Error != nil {
			log.Errorf("CreateChunk, err[%v]", rpcReply.Error)
			err = rpcReply.Error
			return
		}

		cInfo.location.Add(addr)
	}

	fInfo := new(fileInfo)
	fInfo.handles = append(fInfo.handles, handle)

	cm.numChunkHandle = handle + 1
	cm.chunk[handle] = cInfo
	cm.file[path] = fInfo

	return
}
