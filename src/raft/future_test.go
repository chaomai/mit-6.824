package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestErrorFuture(t *testing.T) {
	errFuture := NewErrorFuture()
	ready := false
	cond := sync.NewCond(&sync.Mutex{})

	F := func() {
		cond.L.Lock()
		for !ready {
			cond.Wait()
		}
		cond.L.Unlock()
		errFuture.RespondErr(fmt.Errorf("error happend"))
	}

	for i := 0; i < 5; i++ {
		go F()
	}

	cond.L.Lock()
	ready = true
	cond.Broadcast()
	cond.L.Unlock()

	fmt.Println(errFuture.Error())
}

func TestRPCFuture(t *testing.T) {
	args := &RequestVoteArgs{}

	rpcFuture := NewRPCFuture(args)

	go func() {
		time.Sleep(time.Second)
		reply := RequestVoteReply{
			TraceId:     rand.Uint32(),
			Term:        123,
			VoteGranted: true,
		}
		rpcFuture.Respond(reply, nil)
	}()

	fmt.Println(rpcFuture.Get())
}
