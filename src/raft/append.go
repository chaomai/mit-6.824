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
		zap.Stringer("term", rf.getCurrentTerm()),
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
		Term:    rf.getCurrentTerm(),
		Success: false,
	}

	defer func() { rpc.Respond(reply, nil) }()

	if args.Term < rf.getCurrentTerm() {
		zap.L().Debug("get older term from append entries and ignore",
			zap.Stringer("server", rf.me),
			zap.Stringer("term", rf.getCurrentTerm()),
			zap.Stringer("state", rf.getState()),
			zap.Uint32("traceId", args.TraceId))
		return
	}

	if args.Term > rf.getCurrentTerm() {
		rf.setCurrentTerm(args.Term)
		rf.setState(Follower)
		reply.Term = rf.getCurrentTerm()

		zap.L().Info("get newer term from append entries and change to follower",
			zap.Stringer("server", rf.me),
			zap.Stringer("term", rf.getCurrentTerm()),
			zap.Stringer("state", rf.getState()),
			zap.Uint32("traceId", args.TraceId))
	}

	// check prev
	if args.PrevLogIndex > 0 {
		index, term := rf.getLogInfoAt(args.PrevLogIndex)

		if index <= 0 {
			// no such a prev entry
			lastLogIndex, _ := rf.getLastLogInfo()
			reply.ConflictTermFirstIndex = lastLogIndex + 1

			zap.L().Debug("no such a prev log index and ignore",
				zap.Stringer("server", rf.me),
				zap.Stringer("term", rf.getCurrentTerm()),
				zap.Stringer("state", rf.getState()),
				zap.Uint32("traceId", args.TraceId))
			return
		} else if term != args.PrevLogTerm {
			// include the term of the conflicting entry and the first index it stores for that term.
			firstIndex, _ := rf.getLowerBoundOfTerm(term)
			reply.ConflictTermFirstIndex = firstIndex

			zap.L().Debug("prev log term is different and ignore",
				zap.Stringer("server", rf.me),
				zap.Stringer("term", rf.getCurrentTerm()),
				zap.Stringer("state", rf.getState()),
				zap.Uint32("traceId", args.TraceId))
			return
		}
	}

	// skip until conflict entry
	numEntries := len(args.Entries)
	if numEntries > 0 {
		skipUntil := 0
		for i := range args.Entries {
			skipUntil = i
			_, term := rf.getLogInfoAt(args.Entries[i].Index)
			if term != args.Entries[i].Term {
				break
			}
		}

		rf.mu.Lock()
		rf.log = append(rf.log[:args.Entries[skipUntil].Index], args.Entries[skipUntil:]...)
		rf.mu.Unlock()

		zap.L().Debug("append log",
			zap.Stringer("server", rf.me),
			zap.Stringer("term", rf.getCurrentTerm()),
			zap.Stringer("state", rf.getState()),
			zap.Any("log", rf.log))
	}

	if args.LeaderCommit > rf.getCommitIndex() {
		lastLogIndex, _ := rf.getLastLogInfo()
		commitIndex := min(args.LeaderCommit, lastLogIndex)
		rf.setCommitIndex(commitIndex)

		zap.L().Debug("update commit index",
			zap.Stringer("server", rf.me),
			zap.Stringer("term", rf.getCurrentTerm()),
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
			zap.Stringer("term", rf.getCurrentTerm()),
			zap.Stringer("state", rf.getState()),
			zap.Stringer("remote server", server),
			zap.Any("args", args))
	}

	return false
}
