package master

import (
	"encoding/gob"
	"os"
	"path"
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

	metaMutex sync.RWMutex
	metaFile  string
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

type persistChunk struct {
	Handle   int64
	Location []string
	Path     string
	Version  int64
}

type persistChunkInfo struct {
	Num   int64
	Chunk []persistChunk
}

func newChunkManager(serverRoot string) *chunkManager {
	cm := &chunkManager{
		chunk:    make(map[gfs.ChunkHandle]*chunkInfo),
		file:     make(map[gfs.Path]*fileInfo),
		metaFile: path.Join(serverRoot, gfs.ChunkManagerMetaFileName),
	}
	return cm
}

func (cm *chunkManager) serialize() error {
	persist := persistChunkInfo{}
	persist.Num = int64(cm.numChunkHandle)

	for handle, cInfo := range cm.chunk {
		addrs := make([]string, 0)

		for _, addr := range cInfo.location.GetAll() {
			addrs = append(addrs, string(addr.(gfs.ServerAddress)))
		}

		persist.Chunk = append(persist.Chunk,
			persistChunk{Handle: int64(handle),
				Location: addrs,
				Path:     string(cInfo.path),
				Version:  int64(cInfo.version),
			})
	}

	fp, err := os.OpenFile(cm.metaFile, os.O_CREATE|os.O_WRONLY, gfs.DefaultFilePerm)
	if err != nil {
		log.Errorf("serialize, err[%v]", err)
		return err
	}

	defer fp.Close()

	enc := gob.NewEncoder(fp)
	err = enc.Encode(persist)
	if err != nil {
		log.Errorf("serialize, err[%v]", err)
		return err
	}

	return nil
}

func (cm *chunkManager) deserialize() error {
	if _, err := os.Stat(cm.metaFile); os.IsNotExist(err) {
		log.Infof("deserialize, err[%v]", err)
		return nil
	} else if err != nil {
		log.Errorf("deserialize, err[%v]", err)
		return err
	}

	fp, err := os.OpenFile(cm.metaFile, os.O_CREATE|os.O_RDONLY, gfs.DefaultFilePerm)
	if err != nil {
		log.Errorf("deserialize, err[%v]", err)
		return err
	}

	defer fp.Close()

	persist := persistChunkInfo{}

	dec := gob.NewDecoder(fp)
	err = dec.Decode(&persist)
	if err != nil {
		log.Errorf("deserialize, err[%v]", err)
		return err
	}

	cm.numChunkHandle = gfs.ChunkHandle(persist.Num)

	for _, chunk := range persist.Chunk {
		h := gfs.ChunkHandle(chunk.Handle)
		p := gfs.Path(chunk.Path)
		if _, ok := cm.chunk[h]; !ok {
			cm.chunk[h] = new(chunkInfo)
		}

		for _, addr := range chunk.Location {
			cm.chunk[h].location.Add(addr)
		}

		cm.chunk[h].path = p
		cm.chunk[h].version = gfs.ChunkVersion(chunk.Version)

		if _, ok := cm.file[p]; !ok {
			cm.file[p] = new(fileInfo)
		}

		cm.file[p].handles = append(cm.file[p].handles, h)
	}

	return nil
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

	cInfo := cm.chunk[handle]
	if cInfo.location.Size() < gfs.DefaultNumReplicas {
		cm.chunk[handle].location.Add(addr)
	} else {
		// should be deleted
		log.Warnf("RegisterReplica, handle[%d], ignore at[%v]", handle, addr)
	}

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
func (cm *chunkManager) GetLeaseHolder(handle gfs.ChunkHandle) (l *gfs.Lease, err error) {
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
		l = new(gfs.Lease)
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

	l = new(gfs.Lease)
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
