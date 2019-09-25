package raft

import (
	"context"
	"time"

	"go.uber.org/zap"
)

// call by main goroutine.
func (rf *Raft) setupFollower() {
	zap.L().Debug("setup follower",
		zap.Stringer("server", rf.me),
		zap.Stringer("term", rf.getCurrentTerm()),
		zap.Stringer("state", rf.getState()))

	if rf.electionTimer == nil {
		rf.electionTimer = time.NewTimer(getRandomDuration(rf.electionDuration))
	} else {
		// drain the channel before run
		if !rf.electionTimer.Stop() {
			select {
			case <-rf.electionTimer.C:
			default:
			}
		}

		rf.resetElectionTimer()
	}
}

// call by main goroutine.
func (rf *Raft) cleanupFollower() {
	zap.L().Debug("cleanup follower",
		zap.Stringer("server", rf.me),
		zap.Stringer("term", rf.getCurrentTerm()),
		zap.Stringer("state", rf.getState()))
}

// call by main goroutine.
func (rf *Raft) runFollower(ctx context.Context) {
	defer rf.goroutineWg.Done()

	zap.L().Info("run follower",
		zap.Stringer("server", rf.me),
		zap.Stringer("term", rf.getCurrentTerm()),
		zap.Stringer("state", rf.getState()))

	rf.setupFollower()
	defer rf.cleanupFollower()

	for rf.getState() == Follower {
		select {
		case rpc := <-rf.rpcCh:
			rf.handleRPC(rpc)
		case <-ctx.Done():
			zap.L().Info("follower shutdown",
				zap.Stringer("server", rf.me),
				zap.Stringer("term", rf.getCurrentTerm()),
				zap.Stringer("state", rf.getState()))
			return
		case <-rf.electionTimer.C:
			rf.setState(PreCandidate)
			zap.L().Info("election timeout and change to pre candidate",
				zap.Stringer("server", rf.me),
				zap.Stringer("term", rf.getCurrentTerm()),
				zap.Stringer("state", rf.getState()))
			return
		}
	}
}
