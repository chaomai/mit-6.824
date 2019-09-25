package raft

import (
	"context"

	"go.uber.org/zap"
)

// call by main goroutine.
func (rf *Raft) setupPreCandidate() {
	zap.L().Debug("setup pre candidate",
		zap.Stringer("server", rf.me),
		zap.Stringer("term", rf.getCurrentTerm()),
		zap.Stringer("state", rf.getState()))

	// drain the channel before run
	if !rf.electionTimer.Stop() {
		select {
		case <-rf.electionTimer.C:
		default:
		}
	}
}

// call by main goroutine.
func (rf *Raft) cleanupPreCandidate() {
	zap.L().Debug("cleanup pre candidate",
		zap.Stringer("server", rf.me),
		zap.Stringer("term", rf.getCurrentTerm()),
		zap.Stringer("state", rf.getState()))
}

// call by main goroutine.
func (rf *Raft) runPreCandidate(ctx context.Context) {
	defer rf.goroutineWg.Done()

	zap.L().Info("run pre candidate",
		zap.Stringer("server", rf.me),
		zap.Stringer("term", rf.getCurrentTerm()),
		zap.Stringer("state", rf.getState()))

	rf.setupPreCandidate()
	defer rf.cleanupPreCandidate()

	preVoteCh := rf.preVote(ctx)
	numVotedDenied := 0
	numVotedPermitted := 0

	for rf.getState() == PreCandidate {
		select {
		case rpc := <-rf.rpcCh:
			rf.handleRPC(rpc)
		case <-ctx.Done():
			zap.L().Info("pre candidate shutdown",
				zap.Stringer("server", rf.me),
				zap.Stringer("term", rf.getCurrentTerm()),
				zap.Stringer("state", rf.getState()))
			return
		case v := <-preVoteCh:
			zap.L().Debug("receive RequestVote reply",
				zap.Stringer("server", rf.me),
				zap.Stringer("term", rf.getCurrentTerm()),
				zap.Stringer("state", rf.getState()),
				zap.Any("v", v))

			if v.Term > rf.getCurrentTerm() {
				rf.setCurrentTerm(v.Term)
				rf.setState(Follower)
				zap.L().Info("get newer term from vote permit response and change to follower",
					zap.Stringer("server", rf.me),
					zap.Stringer("term", rf.getCurrentTerm()),
					zap.Stringer("state", rf.getState()),
					zap.Stringer("remote server", v.ServerId),
					zap.Stringer("remote server term", v.Term))
				return
			}

			if v.VoteGranted {
				numVotedPermitted += 1
				zap.L().Info("vote permit granted",
					zap.Stringer("server", rf.me),
					zap.Stringer("term", rf.getCurrentTerm()),
					zap.Stringer("state", rf.getState()),
					zap.Stringer("remote server", v.ServerId))
			}

			if !v.VoteGranted {
				numVotedDenied += 1
				zap.L().Info("vote permit denied",
					zap.Stringer("server", rf.me),
					zap.Stringer("term", rf.getCurrentTerm()),
					zap.Stringer("state", rf.getState()),
					zap.Stringer("remote server", v.ServerId))
			}

			if rf.checkMajority(numVotedPermitted) {
				rf.setState(Candidate)
				zap.L().Info("get majority vote permit and change to candidate",
					zap.Stringer("server", rf.me),
					zap.Stringer("term", rf.getCurrentTerm()),
					zap.Stringer("state", rf.getState()),
					zap.Int("vote permit", numVotedPermitted))
				return
			}

			if rf.checkMajority(numVotedDenied) {
				rf.setState(Follower)
				zap.L().Info("get majority vote denied and change to follower",
					zap.Stringer("server", rf.me),
					zap.Stringer("term", rf.getCurrentTerm()),
					zap.Stringer("state", rf.getState()),
					zap.Int("vote denied", numVotedDenied))
				return
			}
		case <-rf.electionTimer.C:
			zap.L().Info("election timeout",
				zap.Stringer("server", rf.me),
				zap.Stringer("term", rf.getCurrentTerm()),
				zap.Stringer("state", rf.getState()))
			return
		}
	}
}
