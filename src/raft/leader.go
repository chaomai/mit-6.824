package raft

import (
	"math/rand"
	"sync"
	"time"

	"go.uber.org/zap"
)

// call by main goroutine
func (rf *Raft) setupLeader() {
	zap.L().Debug("setup leader",
		zap.Stringer("server", rf.me),
		zap.Stringer("term", rf.getCurrentTerm()),
		zap.Stringer("state", rf.getState()))

	numServers := len(rf.peers)
	rf.nextIndex = make([]Index, numServers)
	rf.matchIndex = make([]Index, numServers)
	rf.replicateNotifyCh = make(map[ServerId]chan struct{})
	rf.heartBeatNotifyCh = make(map[ServerId]chan struct{})

	for i := 0; i < numServers; i++ {
		serverId := ServerId(i)

		lastLogIndex, _ := rf.getLastLogInfo()
		rf.nextIndex[i] = lastLogIndex + 1
		rf.matchIndex[i] = 0
		rf.replicateNotifyCh[serverId] = make(chan struct{}, 1)
		rf.heartBeatNotifyCh[serverId] = make(chan struct{}, 1)
	}
}

// call by main goroutine.
func (rf *Raft) cleanupLeader() {
	zap.L().Debug("cleanup leader",
		zap.Stringer("server", rf.me),
		zap.Stringer("term", rf.getCurrentTerm()),
		zap.Stringer("state", rf.getState()))

	rf.nextIndex = nil
	rf.matchIndex = nil
	rf.replicateNotifyCh = nil
	rf.heartBeatNotifyCh = nil
}

// call by main goroutine.
// each server has a dedicated heartbeat goroutine.
func (rf *Raft) heartbeat(stepDownCh chan struct{}, stepDownWg *sync.WaitGroup, s ServerId) {
	defer stepDownWg.Done()

	heartbeatTicker := time.NewTicker(rf.heartbeatDuration)

	for {
		select {
		case <-stepDownCh:
			return
		case <-heartbeatTicker.C:
			zap.L().Info("heartbeat",
				zap.Stringer("server", rf.me),
				zap.Stringer("term", rf.getCurrentTerm()),
				zap.Stringer("state", rf.getState()),
				zap.Stringer("remote server", s))
			notify(rf.heartBeatNotifyCh[s])
		}
	}
}

// call by main goroutine
func (rf *Raft) runHeartbeat(stepDownCh chan struct{}, stepDownWg *sync.WaitGroup, curTerm Term) {
	defer stepDownWg.Done()

	for i := range rf.peers {
		serverId := ServerId(i)

		if serverId == rf.me {
			continue
		}

		stepDownWg.Add(1)
		go rf.heartbeat(stepDownCh, stepDownWg, serverId)
	}
}

// call by main goroutine
func (rf *Raft) dispatchEntries(entries ...*Entry) {
	rf.mu.Lock()

	logLen := len(rf.log)
	var lastLogIndex Index
	if logLen != 0 {
		lastLogIndex = rf.log[logLen-1].Index
	} else {
		lastLogIndex = 0
	}

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

	for i := range rf.peers {
		serverId := ServerId(i)
		notify(rf.replicateNotifyCh[serverId])
	}
}

// call by main goroutine
// each server has a dedicated replicate goroutine.
func (rf *Raft) replicate(stepDownCh chan struct{}, stepDownWg *sync.WaitGroup, s ServerId,
	replicateNotifyCh chan struct{}, heartBeatNotifyCh chan struct{}, curTerm Term) {
	defer stepDownWg.Done()

	for {
		entries := make([]*Entry, 0)
		nextLogIndex := rf.getNextIndex(s)
		prevLogIndex, prevLogTerm := rf.getPrevLogInfo(nextLogIndex)
		var lastAppendLogIndex Index

		select {
		case <-stepDownCh:
			return
		case <-heartBeatNotifyCh:
			zap.L().Debug("get heartbeat notify",
				zap.Stringer("server", rf.me),
				zap.Stringer("term", rf.getCurrentTerm()),
				zap.Stringer("state", rf.getState()),
				zap.Stringer("remote server", s))

			lastLogIndex, _ := rf.getLastLogInfo()
			if rf.getMatchIndex(s) < lastLogIndex {
				es := rf.getEntries(nextLogIndex, lastLogIndex+1)
				entries = append(entries, es...)
				lastAppendLogIndex = nextLogIndex
			} else {
				lastAppendLogIndex = NilIndex
			}

		case <-replicateNotifyCh:
			zap.L().Debug("get replicate notify",
				zap.Stringer("server", rf.me),
				zap.Stringer("term", rf.getCurrentTerm()),
				zap.Stringer("state", rf.getState()),
				zap.Stringer("remote server", s))

			es := rf.getEntries(nextLogIndex)
			entries = append(entries, es...)
			lastAppendLogIndex = nextLogIndex
		}

		args := AppendEntriesArgs{
			TraceId:      rand.Uint32(),
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
				AppendEntriesReply: reply,
				LastAppendLogIndex: lastAppendLogIndex,
				ServerId:           rf.me,
			}

			rf.appendResultCh <- ret
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
					AppendEntriesReply: reply,
					LastAppendLogIndex: lastAppendLogIndex,
					ServerId:           s,
				}

				rf.appendResultCh <- ret
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

// call by main goroutine
func (rf *Raft) runBackgroundReplicate(stepDownCh chan struct{}, stepDownWg *sync.WaitGroup, curTerm Term) {
	defer stepDownWg.Done()

	for i := range rf.peers {
		serverId := ServerId(i)
		stepDownWg.Add(1)
		go rf.replicate(stepDownCh, stepDownWg, serverId, rf.replicateNotifyCh[serverId], rf.heartBeatNotifyCh[serverId], curTerm)
	}
}

// call by main goroutine
func (rf *Raft) runLeader() {
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
	lastLogIndex, _ := rf.getLastLogInfo()
	noopEntry := makeNoopEntry(lastLogIndex+1, rf.getCurrentTerm())
	rf.dispatchEntries(noopEntry)

	stepDownCh := make(chan struct{})
	stepDownWg := sync.WaitGroup{}

	defer func() {
		// cleanup
		close(stepDownCh)
		// wait for goroutines
		stepDownWg.Wait()
		rf.cleanupLeader()
	}()

	currentTerm := rf.getCurrentTerm()
	stepDownWg.Add(2)
	rf.runHeartbeat(stepDownCh, &stepDownWg, currentTerm)
	rf.runBackgroundReplicate(stepDownCh, &stepDownWg, currentTerm)

	for rf.getState() == Leader {
		select {
		case rpc := <-rf.rpcCh:
			rf.handleRPC(rpc)
		case <-rf.shutdownCh:
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
				notify(stepDownCh)
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
				rf.setNextIndex(a.ServerId, rf.getNextIndex(a.ServerId)-1)
				zap.L().Debug("decrease next index",
					zap.Stringer("server", rf.me),
					zap.Stringer("term", rf.getCurrentTerm()),
					zap.Stringer("state", rf.getState()),
					zap.Stringer("remote server", a.ServerId),
					zap.Stringer("next index", rf.getNextIndex(a.ServerId)))
			}
		case <-rf.electionTimer.C:
		}
	}
}
