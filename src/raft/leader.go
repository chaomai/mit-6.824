package raft

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"go.uber.org/zap"
)

type ackInfo struct {
	traceId uint32
	ts      time.Time
}

// call by main goroutine.
func (rf *Raft) setupLeader() {
	zap.L().Debug("setup leader",
		zap.Stringer("server", rf.me),
		zap.Stringer("term", rf.getCurrentTerm()),
		zap.Stringer("state", rf.getState()))

	numServers := len(rf.peers)
	rf.nextIndex = make([]Index, numServers)
	rf.matchIndex = make([]Index, numServers)
	rf.appendResultCh = make(chan appendResult, 1)
	rf.replicateNotifyCh = make(map[ServerId]chan Notification)
	rf.heartBeatNotifyCh = make(map[ServerId]chan Notification)
	rf.heartBeatInfo = make(map[ServerId]ackInfo)

	for i := 0; i < numServers; i++ {
		serverId := ServerId(i)

		lastLogIndex, _ := rf.getLastLogInfo()
		rf.nextIndex[i] = lastLogIndex + 1
		rf.matchIndex[i] = 0
		rf.replicateNotifyCh[serverId] = make(chan Notification, 1)
		rf.heartBeatNotifyCh[serverId] = make(chan Notification, 1)
		rf.heartBeatInfo[serverId] = ackInfo{
			traceId: 0,
			ts:      time.Time{},
		}
	}

	rf.updateIsLeaderSetup(true)
}

// call by main goroutine.
func (rf *Raft) cleanupLeader() {
	close(rf.appendResultCh)

	rf.nextIndex = nil
	rf.matchIndex = nil
	rf.appendResultCh = nil
	rf.replicateNotifyCh = nil
	rf.heartBeatNotifyCh = nil
	rf.heartBeatInfo = nil

	rf.updateIsLeaderSetup(false)
}

// call by main goroutine.
func (rf *Raft) runHeartbeat(ctx context.Context, stepDownWg *sync.WaitGroup, curTerm Term) {
	defer stepDownWg.Done()
	heartbeatTicker := time.NewTicker(rf.heartbeatDuration)

	for {
		select {
		case <-ctx.Done():
			zap.L().Info("heartbeat stop",
				zap.Stringer("server", rf.me),
				zap.Stringer("term", rf.getCurrentTerm()),
				zap.Stringer("state", rf.getState()))
			return
		case <-heartbeatTicker.C:
			r := rand.Uint32()
			for i := range rf.peers {
				serverId := ServerId(i)

				zap.L().Info("heartbeat",
					zap.Stringer("server", rf.me),
					zap.Stringer("term", rf.getCurrentTerm()),
					zap.Stringer("state", rf.getState()),
					zap.Stringer("remote server", serverId))

				notify(rf.heartBeatNotifyCh[serverId], r)
			}
		}
	}
}

// call by main goroutine.
func (rf *Raft) dispatchEntries(entries ...*Entry) {
	rf.mu.Lock()

	numLogs := len(rf.log)
	lastLogIndex := rf.log[numLogs-1].Index

	for i := range entries {
		entry := entries[i]
		if entry.Index == NilIndex {
			lastLogIndex++
			entry.Index = lastLogIndex
			entry.Term = rf.currentTerm
		}
		rf.log = append(rf.log, entry)
	}

	rf.mu.Unlock()

	r := rand.Uint32()
	for i := range rf.peers {
		serverId := ServerId(i)
		notify(rf.replicateNotifyCh[serverId], r)
	}
}

// call by main goroutine.
// each server has a dedicated replicate goroutine.
func (rf *Raft) replicate(ctx context.Context, stepDownWg *sync.WaitGroup, s ServerId, curTerm Term) {
	defer stepDownWg.Done()
	replicateNotifyCh := rf.replicateNotifyCh[s]
	heartbeatNotifyCh := rf.heartBeatNotifyCh[s]

	for {
		var traceId uint32

		select {
		case <-ctx.Done():
			zap.L().Info("replicate stop",
				zap.Stringer("server", rf.me),
				zap.Stringer("term", rf.getCurrentTerm()),
				zap.Stringer("state", rf.getState()),
				zap.Stringer("remote server", s))
			return
		case n := <-heartbeatNotifyCh:
			zap.L().Debug("get heartbeat notify",
				zap.Stringer("server", rf.me),
				zap.Stringer("term", rf.getCurrentTerm()),
				zap.Stringer("state", rf.getState()),
				zap.Uint32("traceId", traceId),
				zap.Stringer("remote server", s))
			traceId = n.TraceId
		case n := <-replicateNotifyCh:
			zap.L().Debug("get replicate notify",
				zap.Stringer("server", rf.me),
				zap.Stringer("term", rf.getCurrentTerm()),
				zap.Stringer("state", rf.getState()),
				zap.Uint32("traceId", traceId),
				zap.Stringer("remote server", s))
			traceId = n.TraceId
		}

		entries := make([]*Entry, 0)
		lastLogIndex, _ := rf.getLastLogInfo()
		nextLogIndex := rf.getNextIndex(s)
		prevLogIndex, prevLogTerm := rf.getPrevLogInfo(nextLogIndex)
		var lastAppendLogIndex Index

		if nextLogIndex <= lastLogIndex {
			es := rf.getEntries(nextLogIndex, lastLogIndex+1)
			entries = append(entries, es...)
			lastAppendLogIndex = lastLogIndex
		} else {
			lastAppendLogIndex = NilIndex
		}

		args := AppendEntriesArgs{
			TraceId:      traceId,
			Term:         curTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.getCommitIndex(),
		}

		if s == rf.me {
			reply := AppendEntriesReply{
				TraceId: args.TraceId,
				Term:    curTerm,
				Success: true,
			}

			ret := appendResult{
				AppendEntriesReply:     reply,
				LastAppendLogIndex:     lastAppendLogIndex,
				ConflictTermFirstIndex: NilIndex,
				ServerId:               rf.me,
			}

			rf.updateLastContact()
			trySend(ctx, rf.appendResultCh, ret)
		} else {
			zap.L().Debug("send AppendEntries",
				zap.Stringer("server", rf.me),
				zap.Stringer("term", rf.getCurrentTerm()),
				zap.Stringer("state", rf.getState()),
				zap.Stringer("remote server", s),
				zap.Any("args", args))

			reply := AppendEntriesReply{}
			if ok := rf.sendAppendEntries(s, &args, &reply); ok {
				ret := appendResult{
					AppendEntriesReply:     reply,
					LastAppendLogIndex:     lastAppendLogIndex,
					ConflictTermFirstIndex: reply.ConflictTermFirstIndex,
					ServerId:               s,
				}

				trySend(ctx, rf.appendResultCh, ret)
			} else {
				zap.L().Warn("send AppendEntries failed",
					zap.Stringer("server", rf.me),
					zap.Stringer("term", rf.getCurrentTerm()),
					zap.Stringer("state", rf.getState()),
					zap.Stringer("remote server", s),
					zap.Any("args", args))
			}
		}
	}
}

// call by main goroutine.
func (rf *Raft) runReplicate(ctx context.Context, stepDownWg *sync.WaitGroup, curTerm Term) {
	defer stepDownWg.Done()

	for i := range rf.peers {
		serverId := ServerId(i)
		stepDownWg.Add(1)
		go rf.replicate(ctx, stepDownWg, serverId, curTerm)
	}
}

// call by main goroutine.
func (rf *Raft) runLeader(ctx context.Context) {
	defer rf.goroutineWg.Done()

	zap.L().Info("run leader",
		zap.Stringer("server", rf.me),
		zap.Stringer("term", rf.getCurrentTerm()),
		zap.Stringer("state", rf.getState()))

	rf.setupLeader()

	// send noop
	zap.L().Debug("send noop",
		zap.Stringer("server", rf.me),
		zap.Stringer("term", rf.getCurrentTerm()),
		zap.Stringer("state", rf.getState()))
	rf.dispatchEntries(makeNoopEntry())

	leaderCtx, cancel := context.WithCancel(ctx)
	stepDownWg := sync.WaitGroup{}

	defer func() {
		cancel()
		// wait for goroutines
		stepDownWg.Wait()
		rf.cleanupLeader()

		zap.L().Debug("cleanup leader",
			zap.Stringer("server", rf.me),
			zap.Stringer("term", rf.getCurrentTerm()),
			zap.Stringer("state", rf.getState()))
	}()

	stepDownWg.Add(2)
	go rf.runHeartbeat(leaderCtx, &stepDownWg, rf.getCurrentTerm())
	rf.runReplicate(leaderCtx, &stepDownWg, rf.getCurrentTerm())

	for rf.getState() == Leader {
		select {
		case rpc := <-rf.rpcCh:
			rf.handleRPC(rpc)
		case <-ctx.Done():
			zap.L().Info("leader shutdown",
				zap.Stringer("server", rf.me),
				zap.Stringer("term", rf.getCurrentTerm()),
				zap.Stringer("state", rf.getState()))
			return
		case a := <-rf.appendResultCh:
			zap.L().Debug("receive AppendEntries reply",
				zap.Stringer("server", rf.me),
				zap.Stringer("term", rf.getCurrentTerm()),
				zap.Stringer("state", rf.getState()),
				zap.Any("a", a))

			if a.Term > rf.getCurrentTerm() {
				rf.setCurrentTerm(a.Term)
				rf.setState(Follower)
				zap.L().Info("get newer term from append response and change to follower",
					zap.Stringer("server", rf.me),
					zap.Stringer("term", rf.getCurrentTerm()),
					zap.Stringer("state", rf.getState()),
					zap.Stringer("remote server", a.ServerId),
					zap.Stringer("remote server term", a.Term))
				return
			}

			if a.Success {
				if a.LastAppendLogIndex > 0 {
					rf.setMatchIndex(a.ServerId, a.LastAppendLogIndex)
					zap.L().Debug("update match index",
						zap.Stringer("server", rf.me),
						zap.Stringer("term", rf.getCurrentTerm()),
						zap.Stringer("state", rf.getState()),
						zap.Stringer("remote server", a.ServerId),
						zap.Stringer("match index", rf.getMatchIndex(a.ServerId)))

					rf.setNextIndex(a.ServerId, a.LastAppendLogIndex+1)
					zap.L().Debug("update next index",
						zap.Stringer("server", rf.me),
						zap.Stringer("term", rf.getCurrentTerm()),
						zap.Stringer("state", rf.getState()),
						zap.Stringer("remote server", a.ServerId),
						zap.Stringer("next index", rf.getNextIndex(a.ServerId)))

					rf.updateCommitIndex(a.LastAppendLogIndex)
					zap.L().Debug("update commit index",
						zap.Stringer("server", rf.me),
						zap.Stringer("term", rf.getCurrentTerm()),
						zap.Stringer("state", rf.getState()),
						zap.Stringer("commit index", rf.getCommitIndex()))
				}
			} else {
				if a.ConflictTermFirstIndex <= 0 {
					// send snapshot or send the first entry after index 0.
					rf.setNextIndex(a.ServerId, 1)
				} else {
					rf.setNextIndex(a.ServerId, a.ConflictTermFirstIndex)
				}

				zap.L().Debug("decrease next index",
					zap.Stringer("server", rf.me),
					zap.Stringer("term", rf.getCurrentTerm()),
					zap.Stringer("state", rf.getState()),
					zap.Stringer("remote server", a.ServerId),
					zap.Stringer("next index", rf.getNextIndex(a.ServerId)))
			}
		case <-rf.electionTimer.C:
			zap.L().Info("election timeout and ignore",
				zap.Stringer("server", rf.me),
				zap.Stringer("term", rf.getCurrentTerm()),
				zap.Stringer("state", rf.getState()))
		}
	}
}
