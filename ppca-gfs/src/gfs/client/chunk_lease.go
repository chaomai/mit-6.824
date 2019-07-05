package client

import (
	"sync"
	"time"

	"gfs"
	"gfs/master"
	"gfs/util"

	log "github.com/Sirupsen/logrus"
)

type chunkLease struct {
	sync.Mutex
	master gfs.ServerAddress
	leases map[gfs.ChunkHandle]*master.Lease
}

func newChunkLease(m gfs.ServerAddress) *chunkLease {
	return &chunkLease{
		master: m,
		leases: make(map[gfs.ChunkHandle]*master.Lease)}
}

func (cl *chunkLease) getChunkLease(handle gfs.ChunkHandle) (l *master.Lease, err error) {
	cl.Lock()
	defer cl.Unlock()

	l, ok := cl.leases[handle]
	if !ok || l.Expire.Before(time.Now()) {
		rpcArgs := gfs.GetPrimaryAndSecondariesArg{Handle: handle}
		rpcReply := new(gfs.GetPrimaryAndSecondariesReply)

		err = util.Call(cl.master, "Master.RPCGetPrimaryAndSecondaries", rpcArgs, rpcReply)
		if err != nil {
			log.Errorf("RPCGetPrimaryAndSecondaries, call err[%s]", err)
			return
		} else if rpcReply.Error != nil {
			err = rpcReply.Error
			log.Errorf("RPCGetPrimaryAndSecondaries, err[%s]", err)
			return
		}

		cl.leases[handle] = &master.Lease{Primary: rpcReply.Primary, Secondaries: rpcReply.Secondaries, Expire: rpcReply.Expire, Version: rpcReply.Version}
		l = cl.leases[handle]
	}

	log.Infof("getChunkLease, lease[%v]", l)

	return
}

func (cl *chunkLease) deleteInvalid(handle gfs.ChunkHandle) {
	cl.Lock()
	defer cl.Unlock()

	log.Infof("deleteInvalid, handle[%v]", handle)
	delete(cl.leases, handle)
}
