package raft

import (
	"fmt"
	"testing"
	"time"

	"labrpc"
)

func TestGetLowerBoundOfTerm(t *testing.T) {
	logs := []*Entry{
		&Entry{Index: 2, Term: 0, Data: nil, Type: Noop,},
		&Entry{Index: 3, Term: 1, Data: nil, Type: Noop,},
		&Entry{Index: 4, Term: 1, Data: nil, Type: Noop,},
		&Entry{Index: 5, Term: 1, Data: nil, Type: Noop,},
		&Entry{Index: 6, Term: 2, Data: nil, Type: Noop,},
		&Entry{Index: 7, Term: 3, Data: nil, Type: Noop,},
	}

	rf := Raft{
		log: logs,
	}

	fmt.Println(rf.getLowerBoundOfTerm(-1))
	fmt.Println(rf.getLowerBoundOfTerm(1))
	fmt.Println(rf.getLowerBoundOfTerm(3))
	fmt.Println(rf.getLowerBoundOfTerm(4))
}

func TestCheckMajorityHeartbeat(t *testing.T) {
	rf := Raft{
		peers:            make([]*labrpc.ClientEnd, 3),
		heartBeatInfo:    make(map[ServerId]ackInfo),
		electionDuration: time.Hour,
	}

	rf.heartBeatInfo[0] = ackInfo{
		traceId: 1,
		ts:      time.Now(),
	}
	rf.heartBeatInfo[1] = ackInfo{
		traceId: 1,
		ts:      time.Now(),
	}
	rf.heartBeatInfo[2] = ackInfo{
		traceId: 2,
		ts:      time.Now(),
	}

	fmt.Println(rf.checkMajorityHeartbeat())
}
