package raft

import (
	"context"

	"go.uber.org/zap"
)

// call by main goroutine.
func (rf *Raft) setupCandidate() {
	zap.L().Debug("setup candidate",
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
func (rf *Raft) cleanupCandidate() {
	zap.L().Debug("cleanup candidate",
		zap.Stringer("server", rf.me),
		zap.Stringer("term", rf.getCurrentTerm()),
		zap.Stringer("state", rf.getState()))
}

// call by main goroutine.
func (rf *Raft) runCandidate(ctx context.Context) {
	defer rf.goroutineWg.Done()

	zap.L().Info("run candidate",
		zap.Stringer("server", rf.me),
		zap.Stringer("term", rf.getCurrentTerm()),
		zap.Stringer("state", rf.getState()))

	rf.setupCandidate()
	defer rf.cleanupCandidate()

	voteCh := rf.electSelf(ctx)
	numVotedGranted := 0
	numVotedDenied := 0

	for rf.getState() == Candidate {
		select {
		case rpc := <-rf.rpcCh:
			rf.handleRPC(rpc)
		case <-ctx.Done():
			zap.L().Info("candidate shutdown",
				zap.Stringer("server", rf.me),
				zap.Stringer("term", rf.getCurrentTerm()),
				zap.Stringer("state", rf.getState()))
			return
		case v := <-voteCh:
			zap.L().Debug("receive RequestVote reply",
				zap.Stringer("server", rf.me),
				zap.Stringer("term", rf.getCurrentTerm()),
				zap.Stringer("state", rf.getState()),
				zap.Any("v", v))

			if v.Type != Vote {
				zap.L().Debug("not vote reply and ignore",
					zap.Any("v", v))
				continue
			}

			if v.Term > rf.getCurrentTerm() {
				rf.setCurrentTerm(v.Term)
				rf.setState(Follower)
				zap.L().Info("get newer term from vote response and change to follower",
					zap.Stringer("server", rf.me),
					zap.Stringer("term", rf.getCurrentTerm()),
					zap.Stringer("state", rf.getState()),
					zap.Stringer("remote server", v.ServerId),
					zap.Stringer("remote server term", v.Term))
				return
			}

			if v.VoteGranted {
				numVotedGranted++
				zap.L().Info("vote granted",
					zap.Stringer("server", rf.me),
					zap.Stringer("term", rf.getCurrentTerm()),
					zap.Stringer("state", rf.getState()),
					zap.Stringer("remote server", v.ServerId))
			} else {
				numVotedDenied++
				zap.L().Info("vote denied",
					zap.Stringer("server", rf.me),
					zap.Stringer("term", rf.getCurrentTerm()),
					zap.Stringer("state", rf.getState()),
					zap.Stringer("remote server", v.ServerId))
			}

			if rf.checkMajority(numVotedGranted) {
				rf.setState(Leader)
				zap.L().Info("get majority vote and change to leader",
					zap.Stringer("server", rf.me),
					zap.Stringer("term", rf.getCurrentTerm()),
					zap.Stringer("state", rf.getState()),
					zap.Int("vote", numVotedGranted))
				return
			}

			if rf.checkMajority(numVotedDenied) {
				rf.setState(Follower)
				zap.L().Info("cannot get majority vote and change to follower",
					zap.Stringer("server", rf.me),
					zap.Stringer("term", rf.getCurrentTerm()),
					zap.Stringer("state", rf.getState()),
					zap.Int("vote", numVotedGranted))
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
