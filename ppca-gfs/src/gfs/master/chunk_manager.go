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

func (cm *chunkManager) getUnFullReplicated() (r []gfs.ChunkHandle) {
	cm.RLock()
	defer cm.RUnlock()

	for handle, cInfo := range cm.chunk {
		cInfo.RLock()
		if cInfo.location.Size() != gfs.DefaultNumReplicas {
			curReplicas := make([]gfs.ServerAddress, 0)
			for _, addr := range cInfo.location.GetAll() {
				curReplicas = append(curReplicas, addr.(gfs.ServerAddress))
			}

			r = append(r, handle)
		}

		cInfo.RUnlock()
	}

	log.Debugf("getUnFullReplicated, handles[%v]", r)

	return
}

// RegisterReplica adds a replica for a chunk
func (cm *chunkManager) RegisterReplica(handle gfs.ChunkHandle, addr gfs.ServerAddress) error {
	cm.Lock()
	defer cm.Unlock()

	if _, ok := cm.chunk[handle]; !ok {
		log.Infof("RegisterReplica, handle[%d], err[%v]", handle, gfs.ErrNoSuchHandle)
		cm.chunk[handle] = new(chunkInfo)
	}

	cm.chunk[handle].location.Add(addr)

	return nil
}

// GetReplicas returns the replicas of a chunk
func (cm *chunkManager) GetReplicas(handle gfs.ChunkHandle) (loc []gfs.ServerAddress, err error) {
	cm.RLock()
	defer cm.RUnlock()

	if _, ok := cm.chunk[handle]; !ok {
		err = gfs.ErrNoSuchHandle
		log.Errorf("GetReplicas, handle[%d], err[%v]", handle, err)
		return
	}

	cInfo := cm.chunk[handle]
	cInfo.RLock()
	defer cInfo.RUnlock()

	for _, e := range cInfo.location.GetAll() {
		addr := e.(gfs.ServerAddress)
		loc = append(loc, addr)
	}

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

	cInfo := cm.chunk[handle]

	cInfo.Lock()
	defer cInfo.Unlock()

	if cInfo.expire.After(time.Now()) {
		l = new(Lease)
		l.Primary = cInfo.primary
		l.Expire = cInfo.expire
		l.Version = cInfo.version

		for _, v := range cInfo.location.GetAll() {
			addr := v.(gfs.ServerAddress)
			if addr != l.Primary {
				l.Secondaries = append(l.Secondaries, addr)
			}
		}

		return
	}

	cInfo.primary = cInfo.location.RandomPick().(gfs.ServerAddress)
	cInfo.expire = time.Now().Add(gfs.LeaseExpire)
	cInfo.version++

	for _, v := range cInfo.location.GetAll() {
		addr := v.(gfs.ServerAddress)

		rpcArgs := gfs.GrantLeaseArg{Handle: handle, Version: cInfo.version}
		rpcReply := new(gfs.GrantLeaseReply)
		err = util.Call(addr, "ChunkServer.RPCGrantLease", rpcArgs, rpcReply)
		if err != nil {
			log.Errorf("GetLeaseHolder, call err[%v]", err)
			return
		} else if rpcReply.Error == gfs.ErrStaleVersionAtMaster {
			log.Warnf("GetLeaseHolder, master chunk version[%v], cs chunk version[%v], err[%v]", cInfo.version, rpcReply.NewestVersion, rpcReply.Error)
			cInfo.version = rpcReply.NewestVersion
		} else if rpcReply.Error != nil {
			err = rpcReply.Error
			log.Errorf("GetLeaseHolder, err[%v]", err)
			return
		}
	}

	l = new(Lease)
	l.Primary = cInfo.primary
	l.Expire = cInfo.expire
	l.Version = cInfo.version

	for _, v := range cInfo.location.GetAll() {
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

	cInfo := cm.chunk[handle]
	cInfo.Lock()
	defer cInfo.Unlock()

	cInfo.expire = time.Now().Add(gfs.LeaseExpire)

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
			log.Errorf("CreateChunk, call err[%v]", err)
			return
		} else if rpcReply.Error != nil {
			err = rpcReply.Error
			log.Errorf("CreateChunk, err[%v]", err)
			return
		}

		cInfo.location.Add(addr)
	}

	cm.numChunkHandle = handle + 1
	cm.chunk[handle] = cInfo

	if _, ok := cm.file[path]; !ok {
		cm.file[path] = new(fileInfo)
	}

	fInfo := cm.file[path]
	fInfo.handles = append(fInfo.handles, handle)

	return
}

func (cm *chunkManager) removeServerFromLocation(handle gfs.ChunkHandle, servers [] gfs.ServerAddress) (loc []gfs.ServerAddress, err error) {
	cm.Lock()
	defer cm.Unlock()

	if _, ok := cm.chunk[handle]; !ok {
		err = gfs.ErrNoSuchHandle
		log.Errorf("removeServerFromLocation, handle[%d], err[%v]", handle, err)
		return
	}

	cInfo := cm.chunk[handle]
	cInfo.Lock()
	defer cInfo.Unlock()

	cInfo.expire = time.Time{}

	for _, addr := range servers {
		log.Debugf("removeServerFromLocation, handle[%d], delete[%v]", handle, addr)
		cInfo.location.Delete(addr)
	}

	for _, a := range cInfo.location.GetAll() {
		loc = append(loc, a.(gfs.ServerAddress))
	}

	return
}
