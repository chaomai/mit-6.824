package raft

import "log"

func (rf *Raft) runLeader(event Event) {
	switch event.eventType {
	case HeartbeatTick:
		log.Printf(", me[%d], state[%s], heartbeat\n", rf.me, rf.state)
		rf.heartbeat()
	default:
		log.Printf(", me[%d], state[%s], ignore event[%+v]\n", rf.me, rf.state, event)
	}
}
