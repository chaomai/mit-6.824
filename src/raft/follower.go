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

	var preVoteCh chan voteResult
	var numPreVotedGranted int
	var numPreVotedDenied int

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
				numPreVotedGranted++
				zap.L().Info("vote permit granted",
					zap.Stringer("server", rf.me),
					zap.Stringer("term", rf.getCurrentTerm()),
					zap.Stringer("state", rf.getState()),
					zap.Stringer("remote server", v.ServerId))
			} else {
				numPreVotedDenied++
				zap.L().Info("vote permit denied",
					zap.Stringer("server", rf.me),
					zap.Stringer("term", rf.getCurrentTerm()),
					zap.Stringer("state", rf.getState()),
					zap.Stringer("remote server", v.ServerId))
			}

			if rf.checkMajority(numPreVotedGranted) {
				rf.setState(Candidate)
				zap.L().Info("get majority vote permit and change to candidate",
					zap.Stringer("server", rf.me),
					zap.Stringer("term", rf.getCurrentTerm()),
					zap.Stringer("state", rf.getState()),
					zap.Int("vote permit", numPreVotedGranted))
				return
			}

			if rf.checkMajority(numPreVotedDenied) {
				zap.L().Info("cannot get majority vote and stay in follower",
					zap.Stringer("server", rf.me),
					zap.Stringer("term", rf.getCurrentTerm()),
					zap.Stringer("state", rf.getState()),
					zap.Int("vote permit", numPreVotedGranted))
			}
		case <-rf.electionTimer.C:
			zap.L().Info("election timeout and start pre vote",
				zap.Stringer("server", rf.me),
				zap.Stringer("term", rf.getCurrentTerm()),
				zap.Stringer("state", rf.getState()))

			numPreVotedGranted = 0
			numPreVotedDenied = 0
			preVoteCh = rf.preElectSelf(ctx)
		}
	}
}
