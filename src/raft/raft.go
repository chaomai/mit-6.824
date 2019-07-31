package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

import "labrpc"

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

// const
const NullVotedFor = -1

// role
type Role string

const (
	Candidate Role = "candidate"
	Follower  Role = "follower"
	Leader    Role = "leader"
)

// event
type EventType uint32

const (
	HeartBeat EventType = iota
	ElectionTimeout
	RequestVoteRPC
	AppendEntryRPC
	FollowerTransform
)

type RPCEvent struct {
	args interface{}
	ch   interface{}
}

type Event struct {
	eventType    EventType
	eventContent interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	entries     []LogEntry // first index is 1

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// current role
	role               Role
	eventCh            chan Event
	heartbeatTicker    *time.Ticker
	electionMinTimeout time.Duration
	electionMaxTimeout time.Duration
	electionTimer      *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVoteRPC RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVoteRPC RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	prevLogIndex int
}

type AppendEntriesReply struct {
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	log.Printf(", me[%d], role[%s], receive RequestVote[%+v]\n", rf.me, rf.role, args)
	ch := make(chan RequestVoteReply, 1)
	eventContent := RPCEvent{args: *args, ch: ch}
	event := Event{eventType: RequestVoteRPC, eventContent: eventContent}
	rf.eventCh <- event
	*reply = <-ch
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) getRandDuration() time.Duration {
	t := rf.electionMaxTimeout - rf.electionMinTimeout

	if t < 0 {
		log.Panicf(", me[%d], role[%s], value of election timeout is invalid\n", rf.me, rf.role)
	}

	r := rand.Int63n(t.Nanoseconds())
	return rf.electionMinTimeout + time.Duration(r)
}

func (rf *Raft) heartbeatTickerFunc() {
	for {
		select {
		case <-rf.heartbeatTicker.C:
			rf.eventCh <- Event{eventType: HeartBeat, eventContent: nil}
		}
	}
}

func (rf *Raft) electionTimerFunc() {
	for {
		select {
		case <-rf.electionTimer.C:
			rf.eventCh <- Event{eventType: ElectionTimeout, eventContent: nil}
		}
	}
}

func (rf *Raft) resetElectionTimer() {
	log.Printf(", me[%d], role[%s], reset election timer\n", rf.me, rf.role)
	rf.electionTimer.Reset(rf.getRandDuration())
}

func (rf *Raft) election() {
	rf.currentTerm++
	grantedVote := 0

	// vote self
	grantedVote++

	rf.resetElectionTimer()

	// send RequestVoteRPC RPCs
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}

		lastLogIndex := len(rf.entries)
		lastLogTerm := rf.currentTerm
		if lastLogIndex-1 > 0 {
			lastLogTerm = rf.entries[lastLogIndex-1].Term
		}

		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}
		reply := RequestVoteReply{}

		log.Printf(", me[%d], role[%s], sendRequestVote[%+v] to [%d]\n", rf.me, rf.role, args, idx)
		if ok := rf.sendRequestVote(idx, &args, &reply); !ok {
			log.Printf(", me[%d], role[%s], sendRequestVote to [%d] error\n", rf.me, rf.role, idx)
		} else if reply.VoteGranted {
			grantedVote++
		}
	}

	if 2*grantedVote > len(rf.peers) {
		log.Printf(", me[%d], role[%s], got majority vote[%d], change candidate to leader", rf.me, rf.role, grantedVote)
		rf.role = Leader
	}

	log.Printf(", me[%d], role[%s], cannot get majority vote[%d]", rf.me, rf.role, grantedVote)
}

func (rf *Raft) checkAndGrantVote(args RequestVoteArgs) (term int, voteGranted bool) {
	lastLogIndex := len(rf.entries)
	lastLogTerm := rf.currentTerm
	if lastLogIndex-1 > 0 {
		lastLogTerm = rf.entries[lastLogIndex-1].Term
	}

	if args.Term < rf.currentTerm ||
		rf.votedFor != NullVotedFor ||
		args.LastLogTerm < lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {

		term = rf.currentTerm
		voteGranted = false
	} else {
		rf.votedFor = args.CandidateId
		term = rf.currentTerm
		voteGranted = true
	}

	return
}

func (rf *Raft) followerStateHandler(event Event) {
	switch event.eventType {
	case ElectionTimeout:
		log.Printf(", me[%d], role[%s], election timeout, change follower to candidate\n", rf.me, rf.role)
		rf.mu.Lock()
		rf.role = Candidate
		rf.votedFor = NullVotedFor
		rf.eventCh <- Event{eventType: FollowerTransform, eventContent: nil}
		rf.mu.Unlock()
	case RequestVoteRPC:
		rpcEvent := event.eventContent.(RPCEvent)
		args := rpcEvent.args.(RequestVoteArgs)
		ch := rpcEvent.ch.(chan RequestVoteReply)
		reply := RequestVoteReply{}

		term, voteGranted := rf.checkAndGrantVote(args)

		reply.VoteGranted = voteGranted

		if voteGranted {
			rf.votedFor = args.CandidateId
			reply.Term = term

			rf.role = Candidate

			log.Printf(", me[%d], role[%s], request vote, vote granted\n", rf.me, rf.role)
		} else {
			reply.Term = term

			log.Printf(", me[%d], role[%s], request vote, vote denied\n", rf.me, rf.role)
		}

		ch <- reply
	}
}

func (rf *Raft) candidateStateHandler(event Event) {
	switch event.eventType {
	case ElectionTimeout, FollowerTransform:
		rf.votedFor = NullVotedFor
		log.Printf(", me[%d], role[%s], start election\n", rf.me, rf.role)
		rf.election()
	case RequestVoteRPC:
		rpcEvent := event.eventContent.(RPCEvent)
		args := rpcEvent.args.(RequestVoteArgs)
		ch := rpcEvent.ch.(chan RequestVoteReply)
		reply := RequestVoteReply{}

		term, voteGranted := rf.checkAndGrantVote(args)

		if voteGranted {
			rf.votedFor = args.CandidateId
			reply.Term = term
			reply.VoteGranted = voteGranted

			log.Printf(", me[%d], role[%s], request vote, vote granted\n", rf.me, rf.role)
		} else {
			reply.Term = term
			reply.VoteGranted = voteGranted

			log.Printf(", me[%d], role[%s], request vote, vote denied\n", rf.me, rf.role)
		}

		ch <- reply
	case AppendEntryRPC:
		rf.role = Follower
	}
}

func (rf *Raft) serve() {
	go rf.heartbeatTickerFunc()
	go rf.electionTimerFunc()

	for {
		event := <-rf.eventCh

		log.Printf(", me[%d], role[%s], revice event[%+v]\n", rf.me, rf.role, event)

		switch rf.role {
		case Follower:
			rf.followerStateHandler(event)
		case Candidate:
			rf.candidateStateHandler(event)
		case Leader:
		default:
			log.Panicf(", me[%d], role[%s], role isn't valid\n", rf.me, rf.role)
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.currentTerm = 0
	rf.votedFor = NullVotedFor
	rf.entries = make([]LogEntry, 0)

	rf.commitIndex = 0
	rf.lastApplied = 0

	// rf.nextIndex
	// rf.matchIndex

	rf.role = Follower
	rf.eventCh = make(chan Event)
	rf.heartbeatTicker = time.NewTicker(time.Millisecond * 10)
	rf.electionMinTimeout = time.Millisecond * 50
	rf.electionMaxTimeout = time.Millisecond * 100
	rf.electionTimer = time.NewTimer(rf.getRandDuration())

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rand.Seed(time.Now().UnixNano())
	go rf.serve()

	return rf
}
