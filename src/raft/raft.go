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
	"context"
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
const (
	NullPeerIndex = -1
	NullLogIndex  = -1
	NullLogTerm   = -1
)

// state
type State string

const (
	Candidate State = "candidate"
	Follower  State = "follower"
	Leader    State = "leader"
)

// event
type EventType string

const (
	HeartbeatTick    EventType = "heartbeat"
	ElectionTimeout  EventType = "election timeout"
	RequestVoteRPC   EventType = "request vote"
	AppendEntriesRPC EventType = "append entries"
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

	state   State // current state
	eventCh chan Event

	heartbeatStopCh chan context.CancelFunc
	heartbeatTicker *time.Ticker

	electionStopCh     chan context.CancelFunc
	electionMinTimeout time.Duration
	electionMaxTimeout time.Duration
	electionTimer      *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isLeader := rf.state == Leader
	return term, isLeader
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
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	log.Printf(", me[%d], state[%s], receive RequestVote[%+v]\n", rf.me, rf.state, args)
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
// func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
// 	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
// 	return ok
// }

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) (ok bool) {
	t := time.NewTimer(time.Millisecond * 300)

	rpcCall := func(retCh chan bool) {
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		retCh <- ok
		return
	}

	ch := make(chan bool)
	go rpcCall(ch)

	select {
	case ok = <-ch:
	case <-t.C:
		log.Printf(", me[%d], state[%s], sendRequestVote[%+v] timeout\n", rf.me, rf.state, args)
	}

	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	log.Printf(", me[%d], state[%s], receive AppendEntries[%+v]\n", rf.me, rf.state, args)
	ch := make(chan AppendEntriesReply, 1)
	eventContent := RPCEvent{args: *args, ch: ch}
	event := Event{eventType: AppendEntriesRPC, eventContent: eventContent}
	rf.eventCh <- event
	*reply = <-ch
}

// func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
// 	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
// 	return ok
// }

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) (ok bool) {
	t := time.NewTimer(time.Millisecond * 300)

	rpcCall := func(retCh chan bool) {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		retCh <- ok
		return
	}

	ch := make(chan bool)
	go rpcCall(ch)

	select {
	case ok = <-ch:
	case <-t.C:
		log.Printf(", me[%d], state[%s], sendAppendEntries[%+v] timeout\n", rf.me, rf.state, args)
	}

	return
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
		log.Panicf(", me[%d], state[%s], value of election timeout is invalid\n", rf.me, rf.state)
	}

	r := rand.Int63n(t.Nanoseconds())
	return rf.electionMinTimeout + time.Duration(r)
}

func (rf *Raft) electionTimerFunc() {
	for {
		select {
		case <-rf.electionTimer.C:
			rf.eventCh <- Event{eventType: ElectionTimeout}
		}
	}
}

func (rf *Raft) resetElectionTimer() {
	d := rf.getRandDuration()
	rf.electionTimer.Reset(d)

	log.Printf(", me[%d], state[%s], reset election timer to [%s]\n", rf.me, rf.state, d)
}

func (rf *Raft) doElection(origTerm int, peersEntryInfo []RequestVoteArgs) {
	// vote self
	grantedVote := 1

	rf.resetElectionTimer()

	// send RequestVoteRPC RPCs
	num := len(peersEntryInfo)
	voteReplyCh := make(chan RequestVoteReply, num)
	wg := sync.WaitGroup{}
	wg.Add(num)

	for i, v := range peersEntryInfo {
		go func(idx int, args RequestVoteArgs) {
			defer wg.Done()

			if idx == rf.me {
				return
			}

			reply := RequestVoteReply{}

			log.Printf(", me[%d], state[%s], sendRequestVote[%+v] to [%d]\n", rf.me, rf.state, args, idx)
			if ok := rf.sendRequestVote(idx, &args, &reply); !ok {
				log.Printf(", me[%d], state[%s], sendRequestVote[%+v] to [%d] error\n", rf.me, rf.state, args, idx)
			} else {
				log.Printf(", me[%d], state[%s], sendRequestVote reply[%+v]\n", rf.me, rf.state, reply)
				voteReplyCh <- reply
			}
		}(i, v)
	}

	wg.Wait()
	close(voteReplyCh)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	for reply := range voteReplyCh {
		if rf.currentTerm != origTerm+1 {
			log.Printf(", me[%d], state[%s], term[%d] is changed since election[%d], stop current election\n", rf.me, rf.state, rf.currentTerm, origTerm)
			return
		}

		if ok := rf.doCheckRPC(reply.Term); !ok {
			return
		}

		if reply.VoteGranted {
			grantedVote++
		}
	}

	if 2*grantedVote > len(rf.peers) {
		log.Printf(", me[%d], state[%s], got majority vote[%d], change candidate to leader\n", rf.me, rf.state, grantedVote)
		rf.state = Leader

		log.Printf(", me[%d], state[%s], start heart beat\n", rf.me, rf.state)
		ctx, cancel := context.WithCancel(context.Background())
		rf.heartbeatStopCh <- cancel
		go rf.heartbeatTickerFunc(ctx)
	} else {
		log.Printf(", me[%d], state[%s], cannot get majority vote[%d]\n", rf.me, rf.state, grantedVote)
	}
}

func (rf *Raft) election() {
	// remember current status while holding the lock
	origTerm := rf.currentTerm
	peersEntryInfo := make([]RequestVoteArgs, 0)

	// update currentTerm and votedFor
	rf.currentTerm++
	rf.votedFor = rf.me

	for idx := range rf.peers {
		if idx == rf.me {
			peersEntryInfo = append(peersEntryInfo, RequestVoteArgs{})
			continue
		}

		lastLogIndex := NullLogIndex
		numEntries := len(rf.entries)

		if numEntries > 1 {
			lastLogIndex = numEntries - 1
		}

		lastLogTerm := NullLogTerm
		if numEntries > 1 {
			lastLogTerm = rf.entries[lastLogIndex].Term
		}

		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}

		peersEntryInfo = append(peersEntryInfo, args)
	}

	go rf.doElection(origTerm, peersEntryInfo)
}

func (rf *Raft) checkToGrantVote(term int, lastLogIndex int, lastLogTerm int, candidateId int) (reply RequestVoteReply) {
	meLastLogIndex := NullLogIndex
	numEntries := len(rf.entries)

	if numEntries > 0 {
		meLastLogIndex = numEntries
	}

	meLastLogTerm := NullLogTerm
	if numEntries > 0 {
		meLastLogTerm = rf.entries[numEntries-1].Term
	}

	reply.Term = rf.currentTerm

	if term < rf.currentTerm ||
		rf.votedFor != NullPeerIndex ||
		(lastLogIndex != NullLogIndex && meLastLogIndex != NullLogIndex &&
			lastLogTerm != NullLogTerm && meLastLogTerm != NullLogTerm &&
			(lastLogTerm < meLastLogTerm || (lastLogTerm == meLastLogTerm && lastLogIndex < meLastLogIndex))) {

		reply.VoteGranted = false
	} else {
		rf.votedFor = candidateId
		reply.VoteGranted = true
	}

	if reply.VoteGranted {
		log.Printf(", me[%d], state[%s], request vote, vote granted\n", rf.me, rf.state)
		rf.resetElectionTimer()
	} else {
		log.Printf(", me[%d], state[%s], request vote, vote denied\n", rf.me, rf.state)
	}

	return
}

func (rf *Raft) initializeLeaderState() {
	peersNum := len(rf.peers)
	rf.nextIndex = make([]int, peersNum, peersNum)
	rf.matchIndex = make([]int, peersNum, peersNum)

	lastLogIndex := len(rf.entries)

	for idx := range rf.nextIndex {
		rf.nextIndex[idx] = lastLogIndex + 1
		rf.matchIndex[idx] = lastLogIndex + 1
	}
}

func (rf *Raft) tryStopHeartbeat() {
	select {
	case cancel := <-rf.heartbeatStopCh:
		log.Printf(", me[%d], state[%s], stop heartbeat\n", rf.me, rf.state)
		cancel()
	default:
		log.Printf(", me[%d], state[%s], no heartbeat to stop\n", rf.me, rf.state)
	}
}

func (rf *Raft) heartbeatTickerFunc(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Printf(", me[%d], state[%s], heartbeat stopped\n", rf.me, rf.state)
			return
		case <-rf.heartbeatTicker.C:
			rf.eventCh <- Event{eventType: HeartbeatTick}
		}
	}
}

func (rf *Raft) doHeartbeat(origTerm int, peersEntryInfo []AppendEntriesArgs) {
	numPeers := len(rf.peers)
	heartbeatReplyCh := make(chan AppendEntriesReply, numPeers)
	wg := sync.WaitGroup{}
	wg.Add(numPeers)

	for i, v := range peersEntryInfo {
		go func(idx int, args AppendEntriesArgs) {
			defer wg.Done()
			if idx == rf.me {
				return
			}

			reply := AppendEntriesReply{}

			log.Printf(", me[%d], state[%s], sendAppendEntries[%+v] to [%d]\n", rf.me, rf.state, args, idx)
			if ok := rf.sendAppendEntries(idx, &args, &reply); !ok {
				log.Printf(", me[%d], state[%s], sendAppendEntries[%+v] to [%d] error\n", rf.me, rf.state, args, idx)
			} else {
				log.Printf(", me[%d], state[%s], sendAppendEntries reply[%+v]\n", rf.me, rf.state, reply)
				heartbeatReplyCh <- reply
			}
		}(i, v)
	}

	wg.Wait()
	close(heartbeatReplyCh)

	for reply := range heartbeatReplyCh {
		if rf.currentTerm != origTerm {
			log.Printf(", me[%d], state[%s], term[%d] is changed since heartbeat[%d], stop heartbeat\n", rf.me, rf.state, rf.currentTerm, origTerm)
			rf.tryStopHeartbeat()
			return
		}

		if ok := rf.doCheckRPC(reply.Term); !ok {
			rf.tryStopHeartbeat()
			return
		}
	}
}

func (rf *Raft) heartbeat() {
	// remember current status while holding the lock
	origTerm := rf.currentTerm
	peersEntryInfo := make([]AppendEntriesArgs, 0)

	for idx := range rf.peers {
		if idx == rf.me {
			peersEntryInfo = append(peersEntryInfo, AppendEntriesArgs{})
			continue
		}

		prevLogIndex := NullLogIndex
		numEntries := len(rf.entries)

		if numEntries > 1 {
			prevLogIndex = numEntries - 2
		}

		prevLogTerm := NullLogTerm
		if numEntries > 1 {
			prevLogTerm = rf.entries[prevLogIndex].Term
		}

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      nil,
			LeaderCommit: rf.commitIndex,
		}

		peersEntryInfo = append(peersEntryInfo, args)
	}

	go rf.doHeartbeat(origTerm, peersEntryInfo)
}

func (rf *Raft) doCheckRPC(term int) bool {
	if term != NullLogTerm && term > rf.currentTerm {
		log.Printf(", me[%d], state[%s], check rpc, term is old, update term\n", rf.me, rf.state)
		rf.currentTerm = term

		if rf.state != Follower {
			log.Printf(", me[%d], state[%s], term is old, change to follower\n", rf.me, rf.state)
			rf.state = Follower
			rf.votedFor = NullPeerIndex
		}

		return false
	}

	return true
}

func (rf *Raft) checkRPC(event Event) {
	argsTerm := NullLogTerm

	switch event.eventType {
	case RequestVoteRPC:
		rpcEvent := event.eventContent.(RPCEvent)
		args := rpcEvent.args.(RequestVoteArgs)
		argsTerm = args.Term
	case AppendEntriesRPC:
		rpcEvent := event.eventContent.(RPCEvent)
		args := rpcEvent.args.(AppendEntriesArgs)
		argsTerm = args.Term
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.doCheckRPC(argsTerm)
}

func (rf *Raft) doAppendEntries(term int, prevLogIndex int, prevLogTerm int, entries []LogEntry, leaderCommit int) (reply AppendEntriesReply) {
	rf.resetElectionTimer()

	reply.Term = rf.currentTerm

	if term < rf.currentTerm {
		log.Printf(", me[%d], state[%s], append entry failed, term is newer\n", rf.me, rf.state)
		reply.Success = false
		return
	}

	numEntries := len(entries)
	if numEntries == 0 {
		reply.Success = true
		return
	}

	return
}

func (rf *Raft) followerStateHandler(event Event) {
	switch event.eventType {
	case ElectionTimeout:
		rf.mu.Lock()
		log.Printf(", me[%d], state[%s], election timeout, change follower to candidate\n", rf.me, rf.state)
		rf.state = Candidate
		rf.votedFor = NullPeerIndex

		log.Printf(", me[%d], state[%s], start election\n", rf.me, rf.state)
		rf.election()
		rf.mu.Unlock()

	case RequestVoteRPC:
		rpcEvent := event.eventContent.(RPCEvent)
		args := rpcEvent.args.(RequestVoteArgs)
		ch := rpcEvent.ch.(chan RequestVoteReply)

		rf.mu.Lock()
		log.Printf(", me[%d], state[%s], check to grant vote\n", rf.me, rf.state)
		reply := rf.checkToGrantVote(args.Term, args.LastLogIndex, args.LastLogTerm, args.CandidateId)
		rf.mu.Unlock()

		ch <- reply

	case AppendEntriesRPC:
		rpcEvent := event.eventContent.(RPCEvent)
		args := rpcEvent.args.(AppendEntriesArgs)
		ch := rpcEvent.ch.(chan AppendEntriesReply)

		rf.mu.Lock()
		log.Printf(", me[%d], state[%s], append entries\n", rf.me, rf.state)
		reply := rf.doAppendEntries(args.Term, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit)
		rf.mu.Unlock()

		ch <- reply

	default:
		log.Printf(", me[%d], state[%s], ignore event[%+v]\n", rf.me, rf.state, event)
	}
}

func (rf *Raft) candidateStateHandler(event Event) {
	switch event.eventType {
	case ElectionTimeout:
		rf.mu.Lock()
		log.Printf(", me[%d], state[%s], election timeout, start election\n", rf.me, rf.state)
		rf.votedFor = NullPeerIndex

		log.Printf(", me[%d], state[%s], start election\n", rf.me, rf.state)
		rf.election()
		rf.mu.Unlock()

	case RequestVoteRPC:
		rpcEvent := event.eventContent.(RPCEvent)
		args := rpcEvent.args.(RequestVoteArgs)
		ch := rpcEvent.ch.(chan RequestVoteReply)

		rf.mu.Lock()
		log.Printf(", me[%d], state[%s], check to grant vote\n", rf.me, rf.state)
		reply := rf.checkToGrantVote(args.Term, args.LastLogIndex, args.LastLogTerm, args.CandidateId)
		rf.mu.Unlock()

		ch <- reply

	case AppendEntriesRPC:
		rpcEvent := event.eventContent.(RPCEvent)
		args := rpcEvent.args.(AppendEntriesArgs)
		ch := rpcEvent.ch.(chan AppendEntriesReply)

		rf.mu.Lock()
		log.Printf(", me[%d], state[%s], append entry, change candidate to follower\n", rf.me, rf.state)
		rf.state = Follower

		log.Printf(", me[%d], state[%s], append entries\n", rf.me, rf.state)
		reply := rf.doAppendEntries(args.Term, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit)
		rf.mu.Unlock()

		ch <- reply

	default:
		log.Printf(", me[%d], state[%s], ignore event[%+v]\n", rf.me, rf.state, event)
	}
}

func (rf *Raft) leaderStateHandler(event Event) {
	switch event.eventType {
	// case ElectionTimeout:
	case HeartbeatTick:
		rf.mu.Lock()
		log.Printf(", me[%d], state[%s], heartbeat\n", rf.me, rf.state)
		rf.heartbeat()
		rf.mu.Unlock()
	default:
		log.Printf(", me[%d], state[%s], ignore event[%+v]\n", rf.me, rf.state, event)
	}
}

func (rf *Raft) serve() {
	go rf.electionTimerFunc()

	for {
		event := <-rf.eventCh

		log.Printf(", me[%d], state[%s], receive event[%+v]\n", rf.me, rf.state, event)

		rf.checkRPC(event)

		switch rf.state {
		case Follower:
			rf.followerStateHandler(event)
		case Candidate:
			rf.candidateStateHandler(event)
		case Leader:
			rf.leaderStateHandler(event)
		default:
			log.Printf(", me[%d], state[%s], ignore state\n", rf.me, rf.state)
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
	rand.Seed(time.Now().UnixNano())

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.currentTerm = 0
	rf.votedFor = NullPeerIndex
	rf.entries = make([]LogEntry, 0)
	rf.entries = append(rf.entries, LogEntry{
		Command: nil,
		Term:    NullLogTerm,
	})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.initializeLeaderState()

	rf.state = Follower
	rf.eventCh = make(chan Event)

	rf.heartbeatStopCh = make(chan context.CancelFunc, 1)
	rf.heartbeatTicker = time.NewTicker(time.Millisecond * 10)

	rf.electionStopCh = make(chan context.CancelFunc, 1)
	rf.electionMinTimeout = time.Millisecond * 300
	rf.electionMaxTimeout = time.Millisecond * 450
	rf.electionTimer = time.NewTimer(rf.getRandDuration())

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.serve()

	return rf
}
