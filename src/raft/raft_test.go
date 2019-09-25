package raft

import (
	"fmt"
	"testing"
	"time"

	"labrpc"
)

func TestGetFirstEntryOfTerm(t *testing.T) {
	logs := []*Entry{
		&Entry{Index: 0, Term: 0, Data: nil, Type: Noop,},
		&Entry{Index: 1, Term: 1, Data: nil, Type: Noop,},
		&Entry{Index: 2, Term: 3, Data: nil, Type: Noop,},
		&Entry{Index: 3, Term: 4, Data: nil, Type: Noop,},
		&Entry{Index: 4, Term: 4, Data: nil, Type: Noop,},
		&Entry{Index: 5, Term: 4, Data: nil, Type: Noop,},
		&Entry{Index: 6, Term: 6, Data: nil, Type: Noop,},
		&Entry{Index: 7, Term: 8, Data: nil, Type: Noop,},
	}

	rf := Raft{
		log: logs,
	}

	fmt.Println(rf.getFirstEntryOfTerm(5))
	fmt.Println(rf.getFirstEntryOfTerm(-1))
}

func TestGetFirstEntryOfTermFromEmptyLog(t *testing.T) {
	logs := []*Entry{
		&Entry{Index: 0, Term: 0, Data: nil, Type: Noop,},
	}

	rf := Raft{
		log: logs,
	}

	fmt.Println(rf.getFirstEntryOfTerm(2))
	fmt.Println(rf.getFirstEntryOfTerm(-1))
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
