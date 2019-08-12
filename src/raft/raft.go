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
	NullLogTerm   = -1
	NilLogIndex   = 0
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

	state            State // current state
	eventCh          chan Event
	ctx              context.Context
	ctxCancel        context.CancelFunc
	backgroundTicker *time.Ticker

	heartbeatStopCh chan context.CancelFunc
	heartbeatTicker *time.Ticker

	electionStopCh     chan context.CancelFunc
	electionMinTimeout time.Duration
	electionMaxTimeout time.Duration
	electionTimer      *time.Timer

	rpcTimeout time.Duration

	applyCh chan ApplyMsg
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
	TraceId      uint32
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
	TraceId     uint32
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	TraceId      uint32
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	TraceId uint32
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	log.Printf(", me[%d], state[%s], receive RequestVote[%+v]\n", rf.me, rf.state, args)
	rf.mu.Unlock()

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) (ok bool) {
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
	case ok = <-ch:
		return
	case <-ctx.Done():
		log.Printf(", me[%d], state[%s], sendRequestVote[%+v] to [%d] timeout\n", rf.me, rf.state, args, server)
	}

	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	log.Printf(", me[%d], state[%s], receive AppendEntries[%+v]\n", rf.me, rf.state, args)
	rf.mu.Unlock()

	ch := make(chan AppendEntriesReply, 1)
	eventContent := RPCEvent{args: *args, ch: ch}
	event := Event{eventType: AppendEntriesRPC, eventContent: eventContent}
	rf.eventCh <- event
	*reply = <-ch
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) (ok bool) {
	ctx, cancel := context.WithTimeout(context.Background(), rf.rpcTimeout)
	defer cancel()

	rpcCall := func(retCh chan bool) {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		retCh <- ok
		return
	}

	ch := make(chan bool)
	go rpcCall(ch)

	select {
	case ok = <-ch:
		return
	case <-ctx.Done():
		log.Printf(", me[%d], state[%s], sendAppendEntries[%+v] to [%d] timeout\n", rf.me, rf.state, args, server)
	}

	return
}

func (rf *Raft) getLastLogInfo() (lastLogIndex int, lastLogTerm int) {
	lastLogIndex = NilLogIndex
	numEntries := len(rf.entries)
	if numEntries > 1 {
		lastLogIndex = numEntries - 1
	}

	lastLogTerm = NullLogTerm
	if lastLogIndex != NilLogIndex {
		lastLogTerm = rf.entries[lastLogIndex].Term
	}

	return
}

func (rf *Raft) getPrevLogInfo(nextIndex int) (prevLogIndex int, prevLogTerm int) {
	prevLogIndex = NilLogIndex
	if nextIndex > 1 {
		prevLogIndex = nextIndex - 1
	}

	prevLogTerm = NullLogTerm
	if nextIndex != NilLogIndex {
		prevLogTerm = rf.entries[prevLogIndex].Term
	}

	return
}

func (rf *Raft) getLogEntry(index int) (command interface{}, term int) {
	command = rf.entries[index].Command
	term = rf.entries[index].Term
	return
}

func (rf *Raft) doSendAppendEntries(ctx context.Context, origTerm int, origNextIndex int, server int, toReplicateIndex int, numReplicated *int, cond *sync.Cond) {
	nextIndex := origNextIndex

	for nextIndex <= toReplicateIndex {
		prevLogIndex, prevLogTerm := rf.getPrevLogInfo(nextIndex)

		// entries may become invalid if leader changed
		// if it's invalid, then the new term will ensure that following update won't happen
		rf.mu.Lock()

		command, term := rf.getLogEntry(nextIndex)
		curTerm := rf.currentTerm
		curCommitIdx := rf.commitIndex

		rf.mu.Unlock()

		var entries []LogEntry
		entries = make([]LogEntry, 0)
		entry := LogEntry{
			Command: command,
			Term:    term,
		}
		entries = append(entries, entry)

		args := AppendEntriesArgs{
			TraceId:      rand.Uint32(),
			Term:         curTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: curCommitIdx,
		}

		reply := AppendEntriesReply{}

		log.Printf(", me[%d], state[%s], send append entries[%+v] to [%d]\n", rf.me, rf.state, args, server)
		isSendOk := rf.sendAppendEntries(server, &args, &reply)

		rf.mu.Lock()

		if !isSendOk {
			log.Printf(", me[%d], state[%s], send append entries to [%d] error\n", rf.me, rf.state, server)

			select {
			case <-ctx.Done():
				log.Printf(", me[%d], state[%s], stop retry, will retry in heartbeat\n", rf.me, rf.state)
				rf.mu.Unlock()
				return
			default:
				log.Printf(", me[%d], state[%s], retry send append entries\n", rf.me, rf.state)
				continue
			}
		} else {
			log.Printf(", me[%d], state[%s], send append entries reply[%+v]\n", rf.me, rf.state, reply)
		}

		if rf.currentTerm != origTerm {
			log.Printf(", me[%d], state[%s], term[%d] is changed since[%d] start append, stop\n", rf.me, rf.state, rf.currentTerm, origTerm)

			rf.mu.Unlock()
			return
		}

		if ok := rf.doCheckRPC(reply.Term); !ok {
			rf.mu.Unlock()
			return
		}

		if !reply.Success {
			log.Printf(", me[%d], state[%s], send append entries failed, decrease nextIndex\n", rf.me, rf.state)

			nextIndex -= 1

			if nextIndex == NilLogIndex {
				log.Panicf(", me[%d], state[%s], nextIndex reaches NilLogIndex", rf.me, rf.state)
				// rf.mu.Unlock()
				return
			}

			continue
		}

		if rf.nextIndex[server] != nextIndex {
			log.Printf(", me[%d], state[%s], nextIndex[%d] of [%d] is changed since[%d] start append, stop\n", rf.me, rf.state, rf.nextIndex[server], server, nextIndex)
			rf.mu.Unlock()
			return
		}

		rf.matchIndex[server] = nextIndex

		numMatch := 0

		for idx := range rf.peers {
			if rf.matchIndex[idx] >= nextIndex {
				numMatch++
			}
		}

		numPeers := len(rf.peers)
		if 2*numMatch > numPeers && rf.entries[nextIndex].Term == rf.currentTerm {
			rf.commitIndex = nextIndex
			log.Printf(", me[%d], state[%s], replicate on majority[%d] and has current term, update commitIndex to [%d]", rf.me, rf.state, numMatch, rf.commitIndex)
		}

		rf.nextIndex[server]++
		nextIndex++

		log.Printf(", me[%d], state[%s], update nextIndex of server[%d] to [%d]", rf.me, rf.state, server, rf.nextIndex[server])

		rf.mu.Unlock()
	}

	cond.L.Lock()
	*numReplicated += 1
	cond.Signal()
	cond.L.Unlock()
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
	rf.mu.Lock()

	term = rf.currentTerm

	if rf.state != Leader {
		log.Printf(", me[%d], state[%s], Start failed, server is not leader\n", rf.me, rf.state)
		index = NilLogIndex
		isLeader = false

		rf.mu.Unlock()
		return index, term, isLeader
	} else {
		isLeader = true
	}

	origTerm := rf.currentTerm

	// append log entry
	entry := LogEntry{
		Command: command,
		Term:    origTerm,
	}
	rf.entries = append(rf.entries, entry)

	// replicate entry
	lastLogIndex, _ := rf.getLastLogInfo()

	rf.matchIndex[rf.me] = lastLogIndex

	numPeers := len(rf.peers)
	numReplicated := 1 // self is replicated
	cl := sync.Mutex{}
	cond := sync.NewCond(&cl)
	ctx, cancel := context.WithCancel(context.Background())

	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}

		origNextIndex := rf.nextIndex[idx]

		go rf.doSendAppendEntries(ctx, origTerm, origNextIndex, idx, lastLogIndex, &numReplicated, cond)
	}

	rf.mu.Unlock()

	// wait until entry is safely replicated
	cond.L.Lock()
	for !(2*numReplicated > numPeers) {
		cond.Wait()
	}
	cond.L.Unlock()

	cancel()

	log.Printf(", me[%d], state[%s], reach majority of replicate num[%d]\n", rf.me, rf.state, numReplicated)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm != origTerm {
		log.Printf(", me[%d], state[%s], term[%d] is changed since[%d] last acquire, stop\n", rf.me, rf.state, rf.currentTerm, origTerm)
		return index, term, isLeader
	}

	// apply entry
	applyEntry := ApplyMsg{
		CommandValid: true,
		Command:      command,
		CommandIndex: lastLogIndex,
	}
	rf.applyCh <- applyEntry

	rf.lastApplied = lastLogIndex
	index = lastLogIndex

	log.Printf(", me[%d], state[%s], apply entry[%+v]\n", rf.me, rf.state, applyEntry)

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
	rf.ctxCancel()
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

func (rf *Raft) doSendRequestVote(server int, voteReplyCh chan RequestVoteReply, wg *sync.WaitGroup) {
	defer wg.Done()

	// if term is changed invalid, then the new term will ensure that following update won't happen
	rf.mu.Lock()

	lastLogIndex, lastLogTerm := rf.getLastLogInfo()
	curTerm := rf.currentTerm

	rf.mu.Unlock()

	args := RequestVoteArgs{
		TraceId:      rand.Uint32(),
		Term:         curTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	reply := RequestVoteReply{}

	log.Printf(", me[%d], state[%s], sendRequestVote[%+v] to [%d]\n", rf.me, rf.state, args, server)
	if ok := rf.sendRequestVote(server, &args, &reply); !ok {
		log.Printf(", me[%d], state[%s], sendRequestVote[%+v] to [%d] error\n", rf.me, rf.state, args, server)
	} else {
		log.Printf(", me[%d], state[%s], sendRequestVote reply[%+v]\n", rf.me, rf.state, reply)
		voteReplyCh <- reply
	}
}

func (rf *Raft) election(origTerm int) {
	// vote self
	grantedVote := 1

	rf.resetElectionTimer()

	// send RequestVoteRPC RPCs
	numPeers := len(rf.peers)
	voteReplyCh := make(chan RequestVoteReply, numPeers)
	wg := sync.WaitGroup{}
	wg.Add(numPeers - 1) // self is voted

	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}

		go rf.doSendRequestVote(idx, voteReplyCh, &wg)
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

		ctx, cancel := context.WithCancel(context.Background())
		rf.heartbeatStopCh <- cancel
		go rf.heartbeatTickerFunc(ctx)
	} else {
		log.Printf(", me[%d], state[%s], cannot get majority vote[%d]\n", rf.me, rf.state, grantedVote)
	}
}

func (rf *Raft) checkToGrantVote(term int, lastLogIndex int, lastLogTerm int, candidateId int) (reply RequestVoteReply) {
	meLastLogIndex, meLastLogTerm := rf.getLastLogInfo()

	reply.Term = rf.currentTerm

	log.Printf(", me[%d], state[%s], check vote term[%d] rf.currentTerm[%d] lastLogIndex[%d] meLastLogIndex[%d] lastLogTerm[%d] meLastLogTerm[%d]",
		rf.me, rf.state, term, rf.currentTerm, lastLogIndex, meLastLogIndex, lastLogTerm, meLastLogTerm)

	if term < rf.currentTerm ||
		rf.votedFor != NullPeerIndex ||
		(lastLogIndex != NilLogIndex && meLastLogIndex != NilLogIndex &&
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

	lastLogIndex, _ := rf.getLastLogInfo()

	for idx := range rf.nextIndex {
		rf.nextIndex[idx] = lastLogIndex + 1
		rf.matchIndex[idx] = 0
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
	// send first heartbeat immediately
	log.Printf(", me[%d], state[%s], start heartbeat\n", rf.me, rf.state)
	rf.eventCh <- Event{eventType: HeartbeatTick}

	for {
		select {
		case <-rf.ctx.Done():
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
			log.Printf(", me[%d], state[%s], term[%d] is changed since[%d] heartbeat, stop heartbeat\n", rf.me, rf.state, rf.currentTerm, origTerm)
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

		prevLogIndex := NilLogIndex
		numEntries := len(rf.entries)

		if numEntries > 1 {
			prevLogIndex = numEntries - 2
		}

		prevLogTerm := NullLogTerm
		if numEntries > 1 {
			prevLogTerm = rf.entries[prevLogIndex].Term
		}

		args := AppendEntriesArgs{
			TraceId:      rand.Uint32(),
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
		}

		rf.votedFor = NullPeerIndex

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

func (rf *Raft) backgroundApplyFunc() {
	for {
		select {
		case <-rf.ctx.Done():
			log.Printf(", me[%d], state[%s], backgroundApplyFunc stopped\n", rf.me, rf.state)
			return
		case <-rf.backgroundTicker.C:
			rf.mu.Lock()

			if rf.commitIndex > rf.lastApplied {
				rf.lastApplied++

				applyEntry := ApplyMsg{
					CommandValid: true,
					Command:      rf.entries[rf.lastApplied].Command,
					CommandIndex: rf.lastApplied,
				}
				rf.applyCh <- applyEntry

				log.Printf(", me[%d], state[%s], apply entry[%+v]\n", rf.me, rf.state, applyEntry)
			}

			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) doAppendEntries(term int, prevLogIndex int, prevLogTerm int, entries []LogEntry, leaderCommit int) (reply AppendEntriesReply) {
	rf.resetElectionTimer()

	reply.Term = rf.currentTerm

	if term < rf.currentTerm {
		log.Printf(", me[%d], state[%s], append entry failed, term[%d] is newer[%d]\n", rf.me, rf.state, term, rf.currentTerm)
		reply.Success = false
		return
	}

	if prevLogIndex >= len(rf.entries) {
		log.Printf(", me[%d], state[%s], append entry failed, no such prevLog at [%d]\n", rf.me, rf.state, prevLogIndex)
		reply.Success = false
		return
	}

	if prevLogTerm != rf.entries[prevLogIndex].Term {
		log.Printf(", me[%d], state[%s], prevLog isn't match at [%d], remove all following\n", rf.me, rf.state, prevLogIndex)
		rf.entries = rf.entries[:prevLogIndex]
	}

	rf.entries = append(rf.entries, entries...)

	lastLogIndex, _ := rf.getLastLogInfo()
	commitIndex := lastLogIndex

	if commitIndex > leaderCommit {
		commitIndex = leaderCommit
	}

	log.Printf(", me[%d], state[%s], set commitIndex to [%d]\n", rf.me, rf.state, commitIndex)
	rf.commitIndex = commitIndex

	if commitIndex != NilLogIndex && rf.commitIndex > rf.lastApplied {
		rf.lastApplied = rf.commitIndex

		applyEntry := ApplyMsg{
			CommandValid: true,
			Command:      rf.entries[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.applyCh <- applyEntry

		log.Printf(", me[%d], state[%s], apply entry[%+v]\n", rf.me, rf.state, applyEntry)
	}

	reply.Success = true

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

		// remember current status while holding the lock
		origTerm := rf.currentTerm

		// update currentTerm and votedFor
		rf.currentTerm++
		rf.votedFor = rf.me

		// peersEntryInfo := rf.prepareElection()

		rf.mu.Unlock()

		go rf.election(origTerm)

		// go rf.doElection(origTerm, peersEntryInfo)

	case RequestVoteRPC:
		rpcEvent := event.eventContent.(RPCEvent)
		args := rpcEvent.args.(RequestVoteArgs)
		ch := rpcEvent.ch.(chan RequestVoteReply)

		rf.mu.Lock()
		log.Printf(", me[%d], state[%s], check to grant vote\n", rf.me, rf.state)
		reply := rf.checkToGrantVote(args.Term, args.LastLogIndex, args.LastLogTerm, args.CandidateId)
		reply.TraceId = args.TraceId
		rf.mu.Unlock()

		ch <- reply

	case AppendEntriesRPC:
		rpcEvent := event.eventContent.(RPCEvent)
		args := rpcEvent.args.(AppendEntriesArgs)
		ch := rpcEvent.ch.(chan AppendEntriesReply)

		rf.mu.Lock()
		log.Printf(", me[%d], state[%s], append entries\n", rf.me, rf.state)
		reply := rf.doAppendEntries(args.Term, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit)
		reply.TraceId = args.TraceId
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

		// remember current status while holding the lock
		origTerm := rf.currentTerm

		// update currentTerm and votedFor
		rf.currentTerm++
		rf.votedFor = rf.me

		// peersEntryInfo := rf.prepareElection()

		rf.mu.Unlock()

		go rf.election(origTerm)

		// go rf.doElection(origTerm, peersEntryInfo)

	case RequestVoteRPC:
		rpcEvent := event.eventContent.(RPCEvent)
		args := rpcEvent.args.(RequestVoteArgs)
		ch := rpcEvent.ch.(chan RequestVoteReply)

		rf.mu.Lock()
		log.Printf(", me[%d], state[%s], check to grant vote\n", rf.me, rf.state)
		reply := rf.checkToGrantVote(args.Term, args.LastLogIndex, args.LastLogTerm, args.CandidateId)
		reply.TraceId = args.TraceId
		rf.mu.Unlock()

		ch <- reply

	case AppendEntriesRPC:
		rpcEvent := event.eventContent.(RPCEvent)
		args := rpcEvent.args.(AppendEntriesArgs)
		ch := rpcEvent.ch.(chan AppendEntriesReply)

		rf.mu.Lock()
		log.Printf(", me[%d], state[%s], append entries, change candidate to follower\n", rf.me, rf.state)
		rf.state = Follower

		reply := rf.doAppendEntries(args.Term, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit)
		reply.TraceId = args.TraceId
		rf.mu.Unlock()

		ch <- reply

	default:
		log.Printf(", me[%d], state[%s], ignore event[%+v]\n", rf.me, rf.state, event)
	}
}

func (rf *Raft) leaderStateHandler(event Event) {
	switch event.eventType {
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
	go rf.backgroundApplyFunc()

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

	rf.commitIndex = NilLogIndex
	rf.lastApplied = NilLogIndex

	rf.initializeLeaderState()

	rf.state = Follower
	rf.eventCh = make(chan Event)
	rf.ctx, rf.ctxCancel = context.WithCancel(context.Background())
	rf.backgroundTicker = time.NewTicker(time.Millisecond * 10)

	rf.heartbeatStopCh = make(chan context.CancelFunc, 1)
	rf.heartbeatTicker = time.NewTicker(time.Millisecond * 10)

	rf.electionStopCh = make(chan context.CancelFunc, 1)
	rf.electionMinTimeout = time.Millisecond * 300
	rf.electionMaxTimeout = time.Millisecond * 450
	rf.electionTimer = time.NewTimer(rf.getRandDuration())

	rf.rpcTimeout = time.Millisecond * 20

	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.serve()

	return rf
}
