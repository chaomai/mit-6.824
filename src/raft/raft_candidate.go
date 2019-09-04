package raft

import "log"

func (rf *Raft) runCandidate(event Event) {
	switch event.eventType {
	case ElectionTimeout:
		rf.mu.Lock()
		log.Printf(", me[%d], state[%s], election timeout, start election\n", rf.me, rf.state)
		rf.votedFor = NullPeerIndex

		log.Printf(", me[%d], state[%s], start election\n", rf.me, rf.state)

		// remember current status while holding the lock
		origTerm := rf.currentTerm

		// update currentTerm and votedFor
		rf.currentTerm++
		rf.votedFor = rf.me

		rf.mu.Unlock()

		go rf.election(origTerm)

	case RequestVoteRPC:
		rpcEvent := event.eventContent.(RPCEvent)
		args := rpcEvent.args.(RequestVoteArgs)
		ch := rpcEvent.ch.(chan RequestVoteReply)

		rf.mu.Lock()
		log.Printf(", me[%d], state[%s], check to grant vote\n", rf.me, rf.state)
		reply := rf.checkToGrantVote(args.Term, args.LastLogIndex, args.LastLogTerm, args.CandidateId)
		reply.TraceId = args.TraceId
		rf.mu.Unlock()

		ch <- reply

	case AppendEntriesRPC:
		rpcEvent := event.eventContent.(RPCEvent)
		args := rpcEvent.args.(AppendEntriesArgs)
		ch := rpcEvent.ch.(chan AppendEntriesReply)

		rf.mu.Lock()
		log.Printf(", me[%d], state[%s], append entries, change candidate to follower\n", rf.me, rf.state)
		rf.state = Follower

		reply := rf.doAppendEntries(args.Term, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit)
		reply.TraceId = args.TraceId
		rf.mu.Unlock()

		ch <- reply

	default:
		log.Printf(", me[%d], state[%s], ignore event[%+v]\n", rf.me, rf.state, event)
	}
}
