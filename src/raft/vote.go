package raft

import (
	"context"
	"math/rand"

	"go.uber.org/zap"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	TraceId      uint32
	Term         Term
	CandidateId  ServerId
	LastLogIndex Index
	LastLogTerm  Term
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	TraceId     uint32
	Term        Term
	VoteGranted bool
}

type voteResult struct {
	RequestVoteReply
	ServerId ServerId
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	zap.L().Info("receive RequestVote",
		zap.Stringer("server", rf.me),
		zap.Stringer("term", rf.getCurrentTerm()),
		zap.Stringer("state", rf.getState()),
		zap.Any("args", args))

	rpcFuture := NewRPCFuture(args)
	rf.rpcCh <- rpcFuture
	rep, _ := rpcFuture.Get()
	*reply = rep.(RequestVoteReply)
}

// call by main goroutine
func (rf *Raft) handleRequestVote(rpc *RPCFuture, args *RequestVoteArgs) {
	reply := RequestVoteReply{
		TraceId:     args.TraceId,
		Term:        rf.getCurrentTerm(),
		VoteGranted: false,
	}

	defer func() { rpc.Respond(reply, nil) }()

	if args.Term < rf.getCurrentTerm() {
		zap.L().Debug("get older term from vote request and reject",
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

		zap.L().Info("get newer term from vote request and change to follower",
			zap.Stringer("server", rf.me),
			zap.Stringer("term", rf.getCurrentTerm()),
			zap.Stringer("state", rf.getState()),
			zap.Uint32("traceId", args.TraceId))
	}

	lastLogIndex, lastLogTerm := rf.getLastLogInfo()

	// rf.votedForTerm == args.Term also works here.
	if rf.votedForTerm == rf.getCurrentTerm() && rf.votedFor != NilServerId {
		if rf.votedFor == args.CandidateId {
			zap.L().Debug("already voted same candidate in same term and grant",
				zap.Stringer("server", rf.me),
				zap.Stringer("term", rf.getCurrentTerm()),
				zap.Stringer("state", rf.getState()),
				zap.Uint32("traceId", args.TraceId))
			reply.VoteGranted = true
		}

		zap.L().Debug("already voted other candidate in same term and reject",
			zap.Stringer("server", rf.me),
			zap.Stringer("term", rf.getCurrentTerm()),
			zap.Stringer("state", rf.getState()),
			zap.Uint32("traceId", args.TraceId))
		return
	}

	if args.LastLogTerm < lastLogTerm {
		zap.L().Debug("candidate has older LastLogTerm and reject",
			zap.Stringer("server", rf.me),
			zap.Stringer("term", rf.getCurrentTerm()),
			zap.Stringer("state", rf.getState()),
			zap.Stringer("args LastLogTerm", args.LastLogTerm),
			zap.Stringer("lastLogTerm", lastLogTerm),
			zap.Uint32("traceId", args.TraceId))
		return
	}

	if args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex {
		zap.L().Debug("candidate has lesser index and reject",
			zap.Stringer("server", rf.me),
			zap.Stringer("term", rf.getCurrentTerm()),
			zap.Stringer("state", rf.getState()),
			zap.Stringer("args LastLogIndex", args.LastLogIndex),
			zap.Stringer("LastLogIndex", lastLogIndex),
			zap.Uint32("traceId", args.TraceId))
		return
	}

	rf.votedFor = args.CandidateId
	rf.votedForTerm = rf.getCurrentTerm()

	reply.VoteGranted = true

	zap.L().Debug("grant",
		zap.Stringer("server", rf.me),
		zap.Stringer("term", rf.getCurrentTerm()),
		zap.Stringer("state", rf.getState()),
		zap.Uint32("traceId", args.TraceId))
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server ServerId, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ctx, cancel := context.WithTimeout(context.Background(), rf.rpcTimeout)
	defer cancel()

	rpcCall := func(retCh chan bool) {
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		retCh <- ok
		return
	}

	ch := make(chan bool)
	go rpcCall(ch)

	select {
	case ok := <-ch:
		return ok
	case <-ctx.Done():
		zap.L().Warn("send RequestVote timeout",
			zap.Stringer("server", rf.me),
			zap.Stringer("term", rf.getCurrentTerm()),
			zap.Stringer("state", rf.getState()),
			zap.Stringer("remote server", server),
			zap.Any("args", args))
	}

	return false
}

// call by main goroutine
func (rf *Raft) vote() chan voteResult {
	rf.setCurrentTerm(rf.getCurrentTerm() + 1) // Increment current Term
	rf.votedFor = rf.me                        // vote for self
	rf.votedForTerm = rf.getCurrentTerm()
	rf.resetElectionTimer() // reset election timer
	lastLogIndex, lastLogTerm := rf.getLastLogInfo()

	// send
	args := RequestVoteArgs{
		TraceId:      rand.Uint32(),
		Term:         rf.getCurrentTerm(),
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	zap.L().Info("request vote",
		zap.Stringer("server", rf.me),
		zap.Stringer("term", rf.getCurrentTerm()),
		zap.Stringer("state", rf.getState()),
		zap.Any("args", args))

	numServers := len(rf.peers)
	voteCh := make(chan voteResult, numServers)

	for i := range rf.peers {
		serverId := ServerId(i)

		if serverId == rf.me {
			reply := RequestVoteReply{
				Term:        rf.getCurrentTerm(),
				VoteGranted: true,
			}

			ret := voteResult{
				RequestVoteReply: reply,
				ServerId:         rf.me,
			}

			voteCh <- ret
		} else {
			go func(s ServerId) {
				zap.L().Debug("send RequestVote",
					zap.Stringer("server", rf.me),
					zap.Stringer("term", rf.getCurrentTerm()),
					zap.Stringer("state", rf.getState()),
					zap.Stringer("remote server", s),
					zap.Any("args", args))

				reply := RequestVoteReply{}
				if ok := rf.sendRequestVote(s, &args, &reply); ok {
					ret := voteResult{
						RequestVoteReply: reply,
						ServerId:         s,
					}

					voteCh <- ret
				} else {
					zap.L().Warn("send RequestVote failed",
						zap.Stringer("server", rf.me),
						zap.Stringer("term", rf.getCurrentTerm()),
						zap.Stringer("state", rf.getState()),
						zap.Stringer("remote server", s),
						zap.Any("args", args))
				}
			}(serverId)
		}
	}

	return voteCh
}
