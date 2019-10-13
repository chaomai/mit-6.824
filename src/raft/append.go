package raft

import (
	"context"

	"go.uber.org/zap"
)

type AppendEntriesArgs struct {
	TraceId      uint32
	Term         Term
	LeaderId     ServerId
	PrevLogIndex Index // index of entry immediately preceding new ones
	PrevLogTerm  Term  // term of PrevLogIndex entry
	Entries      []*Entry
	LeaderCommit Index
}

type AppendEntriesReply struct {
	TraceId                uint32
	Term                   Term
	Success                bool
	ConflictTermFirstIndex Index
}

type appendResult struct {
	AppendEntriesReply
	LastAppendLogIndex     Index
	ConflictTermFirstIndex Index
	ServerId               ServerId
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	zap.L().Info("receive AppendEntries",
		zap.Stringer("server", rf.me),
		zap.Stringer("term", rf.rs.getCurrentTerm()),
		zap.Stringer("state", rf.getState()),
		zap.Any("args", args))

	rpcFuture := NewRPCFuture(args)
	rf.rpcCh <- rpcFuture
	rep, _ := rpcFuture.Get()
	*reply = rep.(AppendEntriesReply)
}

// call by main goroutine.
func (rf *Raft) handleAppendEntries(rpc *RPCFuture, args *AppendEntriesArgs) {
	reply := AppendEntriesReply{
		TraceId: args.TraceId,
		Term:    rf.rs.getCurrentTerm(),
		Success: false,
	}

	defer func() { rpc.Respond(reply, nil) }()

	if args.Term < rf.rs.getCurrentTerm() {
		zap.L().Debug("get older term from append entries and ignore",
			zap.Stringer("server", rf.me),
			zap.Stringer("term", rf.rs.getCurrentTerm()),
			zap.Stringer("state", rf.getState()),
			zap.Uint32("traceId", args.TraceId))
		return
	}

	if args.Term > rf.rs.getCurrentTerm() {
		rf.rs.setCurrentTerm(args.Term)
		rf.rs.setVoteFor(NilServerId)
		rf.persist()
		rf.setState(Follower)
		reply.Term = rf.rs.getCurrentTerm()

		zap.L().Info("get newer term from append entries and change to follower",
			zap.Stringer("server", rf.me),
			zap.Stringer("term", rf.rs.getCurrentTerm()),
			zap.Stringer("state", rf.getState()),
			zap.Uint32("traceId", args.TraceId))
	}

	// check prev
	if args.PrevLogIndex > 0 {
		index, term := rf.rs.getLogInfoAt(args.PrevLogIndex)

		if index <= 0 {
			// no such a prev entry
			lastLogIndex, _ := rf.rs.getLastLogInfo()
			reply.ConflictTermFirstIndex = lastLogIndex + 1

			zap.L().Debug("no such a prev log index and ignore",
				zap.Stringer("server", rf.me),
				zap.Stringer("term", rf.rs.getCurrentTerm()),
				zap.Stringer("state", rf.getState()),
				zap.Uint32("traceId", args.TraceId))
			return
		} else if term != args.PrevLogTerm {
			// include the term of the conflicting entry and the first index it stores for that term.
			firstIndex, _ := rf.rs.getLowerBoundOfTerm(term)
			reply.ConflictTermFirstIndex = firstIndex

			zap.L().Debug("prev log term is different and ignore",
				zap.Stringer("server", rf.me),
				zap.Stringer("term", rf.rs.getCurrentTerm()),
				zap.Stringer("state", rf.getState()),
				zap.Uint32("traceId", args.TraceId))
			return
		}
	}

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it.
	//
	// skip same entry, delete entries after the conflict
	numEntries := len(args.Entries)
	if numEntries > 0 {
		lastLogIndex, _ := rf.rs.getLastLogInfo()
		skipUntil := 0 // the first conflict
		for skipUntil < numEntries {
			if args.Entries[skipUntil].Index > lastLogIndex {
				break
			}

			_, term := rf.rs.getLogInfoAt(args.Entries[skipUntil].Index)
			if term != args.Entries[skipUntil].Term {
				rf.rs.deleteLogRange(args.Entries[skipUntil].Index, lastLogIndex+1)
				rf.persist()
				break
			}

			skipUntil++
		}

		rf.rs.appendLog(args.Entries[skipUntil:]...)
		rf.persist()

		zap.L().Debug("append log",
			zap.Stringer("server", rf.me),
			zap.Stringer("term", rf.rs.getCurrentTerm()),
			zap.Stringer("state", rf.getState()),
			zap.Any("log", rf.rs.getLog()))
	}

	if args.LeaderCommit > rf.getCommitIndex() {
		lastLogIndex, _ := rf.rs.getLastLogInfo()
		commitIndex := min(args.LeaderCommit, lastLogIndex)
		rf.setCommitIndex(commitIndex)

		zap.L().Debug("update commit index",
			zap.Stringer("server", rf.me),
			zap.Stringer("term", rf.rs.getCurrentTerm()),
			zap.Stringer("state", rf.getState()),
			zap.Stringer("commit index", commitIndex))

		notify(rf.backgroundApplyCh)
	}

	reply.Success = true
	rf.updateLastContact()
}

func (rf *Raft) sendAppendEntries(server ServerId, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ctx, cancel := context.WithTimeout(context.Background(), rf.rpcTimeout)
	defer cancel()

	rpcCall := func(retCh chan bool) {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		retCh <- ok
		return
	}

	ch := make(chan bool)
	go rpcCall(ch)

	select {
	case ok := <-ch:
		return ok
	case <-ctx.Done():
		zap.L().Warn("send AppendEntries timeout",
			zap.Stringer("server", rf.me),
			zap.Stringer("term", rf.rs.getCurrentTerm()),
			zap.Stringer("state", rf.getState()),
			zap.Stringer("remote server", server),
			zap.Any("args", args))
	}

	return false
}
