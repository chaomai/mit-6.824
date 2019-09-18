package raft

import "go.uber.org/zap"

// call by main goroutine
func (rf *Raft) runFollower() {
	zap.L().Info("run follower",
		zap.Stringer("server", rf.me),
		zap.Stringer("term", rf.getCurrentTerm()),
		zap.Stringer("state", rf.getState()))

	for rf.getState() == Follower {
		select {
		case rpc := <-rf.rpcCh:
			rf.handleRPC(rpc)
		case <-rf.shutdownCh:
			zap.L().Info("follower shutdown",
				zap.Stringer("server", rf.me),
				zap.Stringer("term", rf.getCurrentTerm()),
				zap.Stringer("state", rf.getState()))
			return
		case <-rf.electionTimer.C:
			rf.setState(Candidate)
			zap.L().Info("election timeout and change to candidate",
				zap.Stringer("server", rf.me),
				zap.Stringer("term", rf.getCurrentTerm()),
				zap.Stringer("state", rf.getState()))
			return
		}
	}
}
