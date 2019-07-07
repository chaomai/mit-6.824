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

// chunkServerManager manages chunkservers
type chunkServerManager struct {
	sync.RWMutex
	servers map[gfs.ServerAddress]*chunkServerInfo

	metaMutex sync.RWMutex
	metaFile  string
}

type chunkServerInfo struct {
	lastHeartbeat time.Time
	chunks        map[gfs.ChunkHandle]bool // set of chunks that the chunkserver has
}

type persistChunkServerInfo map[string][]int64

func newChunkServerManager(serverRoot string) *chunkServerManager {
	csm := &chunkServerManager{
		servers:  make(map[gfs.ServerAddress]*chunkServerInfo),
		metaFile: path.Join(serverRoot, gfs.ChunkServerManagerMetaFileName),
	}
	return csm
}

func (csm *chunkServerManager) serialize() error {
	persist := make(persistChunkServerInfo)

	for addr, serverInfo := range csm.servers {
		s := string(addr)
		if _, ok := persist[s]; !ok {
			persist[s] = make([]int64, 0)
		}

		for handle, _ := range serverInfo.chunks {
			persist[s] = append(persist[s], int64(handle))
		}
	}

	fp, err := os.OpenFile(csm.metaFile, os.O_CREATE|os.O_WRONLY, gfs.DefaultFilePerm)
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

func (csm *chunkServerManager) deserialize() error {
	if _, err := os.Stat(csm.metaFile); os.IsNotExist(err) {
		log.Infof("deserialize, err[%v]", err)
		return nil
	} else if err != nil {
		log.Errorf("deserialize, err[%v]", err)
		return err
	}

	fp, err := os.OpenFile(csm.metaFile, os.O_CREATE|os.O_RDONLY, gfs.DefaultFilePerm)
	if err != nil {
		log.Errorf("deserialize, err[%v]", err)
		return err
	}

	defer fp.Close()

	persist := make(persistChunkServerInfo)

	dec := gob.NewDecoder(fp)
	err = dec.Decode(&persist)
	if err != nil {
		log.Errorf("deserialize, err[%v]", err)
		return err
	}

	for addr, handles := range persist {
		s := gfs.ServerAddress(addr)
		if _, ok := csm.servers[s]; !ok {
			csm.servers[s] = new(chunkServerInfo)
		}

		for _, handle := range handles {
			h := gfs.ChunkHandle(handle)
			csm.servers[s].chunks[h] = true
		}
	}

	return nil
}

// Heartbeat marks the chunkserver alive for now.
func (csm *chunkServerManager) Heartbeat(addr gfs.ServerAddress) (handles []gfs.ChunkHandle, err error) {
	csm.Lock()
	defer csm.Unlock()

	if info, ok := csm.servers[addr]; !ok {
		log.Infof("Heartbeat, server[%s] doesn't exist, adding", addr)
		serverInfo := new(chunkServerInfo)
		serverInfo.lastHeartbeat = time.Now()
		serverInfo.chunks = make(map[gfs.ChunkHandle]bool)

		rpcArgs := gfs.GetChunksArg{}
		rpcReply := new(gfs.GetChunksReply)

		err = util.Call(addr, "ChunkServer.RPCGetChunks", rpcArgs, rpcReply)
		if err != nil {
			log.Errorf("Heartbeat, call err[%v]", err)
		} else if rpcReply.Error != nil {
			err = rpcReply.Error
			log.Errorf("Heartbeat, err[%v]", err)
		}

		for _, chunk := range rpcReply.Chunks {
			log.Debugf("Heartbeat, server[%s], add handle[%v]", addr, chunk.Handle)
			serverInfo.chunks[chunk.Handle] = true
			handles = append(handles, chunk.Handle)
		}

		csm.servers[addr] = serverInfo
	} else {
		info.lastHeartbeat = time.Now().Add(gfs.HeartbeatInterval)
	}

	return
}

// AddChunk creates a chunk on given chunkservers
func (csm *chunkServerManager) AddChunk(addrs []gfs.ServerAddress, handle gfs.ChunkHandle) error {
	csm.Lock()
	defer csm.Unlock()

	for _, addr := range addrs {
		if serverInfo, ok := csm.servers[addr]; !ok {
			log.Warnf("AddChunk, server[%s] doesn't exist", addr)
			csm.servers[addr] = new(chunkServerInfo)
			csm.servers[addr].chunks[handle] = true
		} else {
			serverInfo.chunks[handle] = true
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

	log.Debugf("ChooseReReplication, serversList[%v]", serversList)

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

	for addr, serverInfo := range csm.servers {
		if time.Now().After(serverInfo.lastHeartbeat.Add(gfs.ServerTimeout)) {
			servers = append(servers, addr)
		}
	}

	log.Debugf("DetectDeadServers, servers[%v]", servers)

	return
}

// RemoveServers removes metedata of a disconnected chunkserver.
// It returns the chunks that server holds
func (csm *chunkServerManager) RemoveServers(servers [] gfs.ServerAddress) (handleLocs map[gfs.ChunkHandle][]gfs.ServerAddress, err error) {
	csm.Lock()
	defer csm.Unlock()

	handleLocs = make(map[gfs.ChunkHandle][]gfs.ServerAddress)

	for _, addr := range servers {
		if serverInfo, ok := csm.servers[addr]; !ok {
			log.Errorf("RemoveServer, server[%s] doesn't exist", addr)
			err = gfs.ErrNoSuchServer
			return
		} else {
			for handle, _ := range serverInfo.chunks {
				if _, ok := handleLocs[handle]; !ok {
					handleLocs[handle] = make([]gfs.ServerAddress, 0)
				}

				handleLocs[handle] = append(handleLocs[handle], addr)
			}

			delete(csm.servers, addr)

			log.Debugf("RemoveServer, server[%v]", addr)
		}
	}

	return
}
