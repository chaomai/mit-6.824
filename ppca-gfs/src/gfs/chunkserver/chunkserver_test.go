package chunkserver

import (
	"encoding/gob"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"testing"

	"gfs"
)

const (
	filename  = "sample.txt"
	startData = "12345"
)

func printContents() {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		panic(err)
	}
	fmt.Println("CONTENTS:", string(data))
}

func TestWrite(t *testing.T) {
	err := ioutil.WriteFile(filename, []byte(startData), 0644)
	if err != nil {
		panic(err)
	}

	printContents()

	f, err := os.OpenFile(filename, os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	if _, err := f.Seek(3, 0); err != nil {
		panic(err)
	}

	if _, err := f.WriteAt([]byte("ABC"), 5); err != nil {
		panic(err)
	}

	printContents()

	buf := make([]byte, 8)
	if n, err := f.ReadAt(buf, 5); err == io.EOF {
		t.Log(buf)
		t.Log(n)
		t.Log(err)
	} else if err != nil {
		panic(err)
	}

	os.Remove(filename)
}

func TestChunkServer_serialize(t *testing.T) {
	type chunkInfo struct {
		sync.RWMutex
		length   gfs.Offset
		version  gfs.ChunkVersion
		checksum []gfs.ChunkCheckSum
	}
	chunk := make(map[gfs.ChunkHandle]*chunkInfo)

	info1 := new(chunkInfo)
	info1.length = 123
	info1.version = 124
	chunk[gfs.ChunkHandle(12345)] = info1

	info2 := new(chunkInfo)
	info2.length = 125
	info2.version = 126
	info2.checksum = []gfs.ChunkCheckSum{1234, 12, 341, 234}
	chunk[gfs.ChunkHandle(12346)] = info2

	persistChunks := make([]gfs.CSChunkInfo, 0)

	for handle, info := range chunk {
		persistChunks = append(persistChunks, gfs.CSChunkInfo{Handle: handle, Length: info.length, Version: info.version, CheckSum: info.checksum})
	}

	fp, _ := os.OpenFile(gfs.ChunkServerMetaFileName, os.O_CREATE|os.O_WRONLY, gfs.DefaultFilePerm)
	defer fp.Close()

	enc := gob.NewEncoder(fp)
	enc.Encode(persistChunks)

	fp1, _ := os.OpenFile(gfs.ChunkServerMetaFileName, os.O_CREATE|os.O_RDONLY, gfs.DefaultFilePerm)
	defer fp1.Close()

	persistChunksRead := make([]gfs.CSChunkInfo, 0)
	dec := gob.NewDecoder(fp1)
	dec.Decode(&persistChunksRead)

	t.Logf("%+v", persistChunksRead)

	os.Remove(gfs.ChunkServerMetaFileName)
}
