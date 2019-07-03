package chunkserver

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"
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