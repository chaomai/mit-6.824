package raft

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

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
