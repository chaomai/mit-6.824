package master

import (
	"sync"
	"time"

	"gfs"
	"gfs/util"

	log "github.com/Sirupsen/logrus"
)

// chunkServerManager manages chunkservers
type chunkServerManager struct {
	sync.RWMutex
	servers map[gfs.ServerAddress]*chunkServerInfo
}

func newChunkServerManager() *chunkServerManager {
	csm := &chunkServerManager{
		servers: make(map[gfs.ServerAddress]*chunkServerInfo),
	}
	return csm
}

type chunkServerInfo struct {
	lastHeartbeat time.Time
	chunks        map[gfs.ChunkHandle]bool // set of chunks that the chunkserver has
}

// Heartbeat marks the chunkserver alive for now.
func (csm *chunkServerManager) Heartbeat(addr gfs.ServerAddress) {
	csm.Lock()
	defer csm.Unlock()

	if info, ok := csm.servers[addr]; !ok {
		log.Infof("Heartbeat, server[%s] doesn't exist, adding", addr)
		csm.servers[addr] = new(chunkServerInfo)
		csm.servers[addr].chunks = make(map[gfs.ChunkHandle]bool)
	} else {
		info.lastHeartbeat = time.Now().Add(gfs.HeartbeatInterval)
	}
}

// AddChunk creates a chunk on given chunkservers
func (csm *chunkServerManager) AddChunk(addrs []gfs.ServerAddress, handle gfs.ChunkHandle) error {
	csm.Lock()
	defer csm.Unlock()

	for _, addr := range addrs {
		if info, ok := csm.servers[addr]; !ok {
			log.Warnf("AddChunk, server[%s] doesn't exist", addr)
		} else {
			info.chunks[handle] = true
		}
	}

	return nil
}

// ChooseReReplication chooses servers to perform re-replication
// called when the replicas number of a chunk is less than gfs.MinimumNumReplicas
// returns two server address, the master will call 'from' to send a copy to 'to'
func (csm *chunkServerManager) ChooseReReplication(handle gfs.ChunkHandle, curReplicas []gfs.ServerAddress) (from, to gfs.ServerAddress, err error) {
	csm.RLock()
	defer csm.RUnlock()

	if len(curReplicas) == gfs.DefaultNumReplicas {
		err = gfs.ErrNoNeedForReReplication
		return
	}

	var serversList util.ArraySet

	for s, _ := range csm.servers {
		isIn := false
		for _, a := range curReplicas {
			if s == a {
				isIn = true
				break
			}
		}

		if !isIn {
			serversList.Add(s)
		}
	}

	i, err := util.Sample(len(curReplicas), 1)
	if err != nil {
		return
	}

	from = curReplicas[i[0]]
	to = serversList.RandomPick().(gfs.ServerAddress)

	log.Infof("ChooseReReplication, handle[%v] from[%v], to[%v]", handle, from, to)

	return
}

// ChooseServers returns servers to store new chunk.
// It is called when a new chunk is create
func (csm *chunkServerManager) ChooseServers(num int) (servers []gfs.ServerAddress, err error) {
	csm.RLock()
	defer csm.RUnlock()

	n := len(csm.servers)
	if num > n {
		err = gfs.ErrNoEnoughServersForReplicas
		return
	}

	samples, err := util.Sample(n, num)
	if err != nil {
		return
	}

	idx := 0
	for s := range csm.servers {
		for _, v := range samples {
			if idx == v {
				servers = append(servers, s)
			}
		}

		idx++
	}

	return
}

// DetectDeadServers detects disconnected chunkservers according to last heartbeat time
func (csm *chunkServerManager) DetectDeadServers() (servers []gfs.ServerAddress) {
	csm.RLock()
	defer csm.RUnlock()

	for addr, info := range csm.servers {
		if time.Now().After(info.lastHeartbeat) {
			servers = append(servers, addr)
		}
	}

	log.Debugf("DetectDeadServers, servers[%v]", servers)

	return
}

// RemoveServers removes metedata of a disconnected chunkserver.
// It returns the chunks that server holds
func (csm *chunkServerManager) RemoveServer(addr gfs.ServerAddress) (handles []gfs.ChunkHandle, err error) {
	csm.Lock()
	defer csm.Unlock()

	if info, ok := csm.servers[addr]; !ok {
		log.Errorf("RemoveServer, server[%s] doesn't exist", addr)
		err = gfs.ErrNoSuchServer
		return
	} else {
		for h, _ := range info.chunks {
			handles = append(handles, h)
		}

		delete(csm.servers, addr)

		log.Debugf("RemoveServer, server[%v]", addr)

		return
	}
}
