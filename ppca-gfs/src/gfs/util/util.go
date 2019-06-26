package util

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math/rand"
	"net/rpc"

	"gfs"
)

var fnvHash = fnv.New32()

// Call is RPC call helper
func Call(srv gfs.ServerAddress, rpcname string, args interface{}, reply interface{}) error {
	c, errx := rpc.Dial("tcp", string(srv))
	if errx != nil {
		return errx
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err != nil {
		return err
	}

	return nil
}

// CallAll applies the rpc call to all destinations.
func CallAll(dst []gfs.ServerAddress, rpcname string, args interface{}) error {
	ch := make(chan error)
	for _, d := range dst {
		go func(addr gfs.ServerAddress) {
			ch <- Call(addr, rpcname, args, nil)
		}(d)
	}
	errList := ""
	for range dst {
		if err := <-ch; err != nil {
			errList += err.Error() + ";"
		}
	}

	if errList == "" {
		return nil
	}

	return fmt.Errorf(errList)
}

// Sample randomly chooses k elements from {0, 1, ..., n-1}.
// n should not be less than k.
func Sample(n, k int) ([]int, error) {
	if n < k {
		return nil, fmt.Errorf("population is not enough for sampling (n = %d, k = %d)", n, k)
	}
	return rand.Perm(n)[:k], nil
}

func Fnv32(data []byte) (hash uint32, err error) {
	_, err = fnvHash.Write(data)
	defer fnvHash.Reset()

	if err != nil {
		return
	}

	hash = binary.BigEndian.Uint32(fnvHash.Sum(nil))
	return
}
