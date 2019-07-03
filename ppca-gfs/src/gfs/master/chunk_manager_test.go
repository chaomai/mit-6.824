package master

import (
	"gfs"
	"sync"
	"testing"
)

func TestGetLeaseHolder(t *testing.T) {
	cm := newChunkManager()
	info := new(chunkInfo)
	info.location.Add(gfs.ServerAddress("1"))
	info.location.Add(gfs.ServerAddress("2"))
	info.location.Add(gfs.ServerAddress("3"))
	cm.chunk[0] = info

	wg := new(sync.WaitGroup)
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			l, err := cm.GetLeaseHolder(gfs.ChunkHandle(0))
			t.Log(l)
			t.Log(err)
			wg.Done()
		}()
	}
	wg.Wait()
}