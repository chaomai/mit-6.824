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
		rs: raftState{
			log: logs,
		},
	}

	fmt.Println(rf.rs.getLowerBoundOfTerm(-1))
	fmt.Println(rf.rs.getLowerBoundOfTerm(1))
	fmt.Println(rf.rs.getLowerBoundOfTerm(3))
	fmt.Println(rf.rs.getLowerBoundOfTerm(4))
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

func TestPersist(t *testing.T) {
	logs := []*Entry{
		&Entry{Index: 2, Term: 0, Data: nil, Type: Noop,},
		&Entry{Index: 3, Term: 1, Data: nil, Type: Noop,},
		&Entry{Index: 4, Term: 1, Data: nil, Type: Noop,},
		&Entry{Index: 5, Term: 1, Data: nil, Type: Noop,},
		&Entry{Index: 6, Term: 2, Data: nil, Type: Noop,},
		&Entry{Index: 7, Term: 3, Data: nil, Type: Noop,},
	}

	persister := MakePersister()
	rf := Raft{
		persister: persister,
		rs: raftState{
			currentTerm: 3,
			votedFor:    1,
			log:         logs,
		},
	}

	rf.persist()

	nrf := Raft{}
	nrf.readPersist(persister.ReadRaftState())
}
