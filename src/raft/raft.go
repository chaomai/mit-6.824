package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isLeader)
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
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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

// Entry
type EntryType int

const (
	Noop EntryType = iota
	Command
)

type Entry struct {
	Index Index
	Term  Term
	Data  interface{}
	Type  EntryType
}

func makeNoopEntry() *Entry {
	entry := &Entry{
		Index: NilIndex,
		Term:  NilTerm,
		Data:  nil,
		Type:  Noop,
	}

	return entry
}

// Term
type Term int

const NilTerm Term = -1

func (t Term) String() string {
	return strconv.Itoa(int(t))
}

// Index
type Index int

const NilIndex Index = -1

func (i Index) String() string {
	return strconv.Itoa(int(i))
}

// Server
type ServerId int

const NilServerId ServerId = -1

func (s ServerId) String() string {
	if s == NilServerId {
		return "nil serverId"
	} else {
		return strconv.Itoa(int(s))
	}
}

type ServerState int

const (
	NilState ServerState = iota
	Candidate
	Follower
	Leader
)

func (s ServerState) String() string {
	switch s {
	case NilState:
		return "nilState"
	case Candidate:
		return "candidate"
	case Follower:
		return "follower"
	case Leader:
		return "leader"
	default:
		zap.L().Panic("unknown server state", zap.Int("s", int(s)))
		return ""
	}
}

// Notification
type Notification struct {
	TraceId uint32
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        ServerId            // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm Term // latest term server has seen
	// candidateId that received vote in current term.
	// votedFor is valid only if votedForTerm == args.Term.
	votedFor     ServerId
	votedForTerm Term // the term that candidate received vote
	log          []*Entry

	// Volatile state on all servers
	commitIndex Index
	lastApplied Index

	// Volatile state on leaders
	nextIndex  []Index
	matchIndex []Index

	applyDuration     time.Duration
	heartbeatDuration time.Duration
	electionDuration  time.Duration
	rpcTimeout        time.Duration
	lastContact       time.Time
	state             ServerState

	rpcCh             chan *RPCFuture
	applyCh           chan ApplyMsg // applyCh is a channel on which the tester or service expects Raft to send ApplyMsg messages
	appendResultCh    chan appendResult
	commitFutures     []*CommitFuture
	backgroundApplyCh chan Notification // get notification when commit index is changed
	raftCtxCancel     context.CancelFunc
	replicateNotifyCh map[ServerId]chan Notification
	heartBeatNotifyCh map[ServerId]chan Notification
	heartBeatInfo     map[ServerId]ackInfo
	goroutineWg       sync.WaitGroup
	isLeaderSetup     bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (term int, isLeader bool) {
	// Your code here (2A).
	term = int(rf.getCurrentTerm())
	isLeader = rf.isLeader()
	return
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

func (rf *Raft) StartFuture(command interface{}) (cf *CommitFuture, ok bool) {
	ok = true
	if !rf.isLeader() || !rf.getIsLeaderSetup() {
		ok = false
		return
	}

	entry := &Entry{
		Index: NilIndex,
		Term:  NilTerm,
		Data:  command,
		Type:  Command,
	}

	rf.dispatchEntries(entry)

	cf = NewCommitFuture(entry.Index, entry.Term)

	rf.mu.Lock()
	rf.commitFutures = append(rf.commitFutures, cf)
	rf.mu.Unlock()

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
// func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
// 	index = int(NilIndex)
// 	term = int(NilTerm)
// 	isLeader = true
//
// 	// Your code here (2B).
// 	if rf.getState() != Leader || !rf.getIsLeaderSetup() {
// 		isLeader = false
// 		return
// 	}
//
// 	entry := &Entry{
// 		Index: NilIndex,
// 		Term:  NilTerm,
// 		Data:  command,
// 		Type:  Command,
// 	}
//
// 	rf.dispatchEntries(entry)
//
// 	index = int(entry.Index)
// 	term = int(entry.Term)
//
// 	zap.L().Debug("start",
// 		zap.Stringer("server", rf.me),
// 		zap.Stringer("term", rf.getCurrentTerm()),
// 		zap.Stringer("state", rf.getState()),
// 		zap.Any("entry", entry))
//
// 	return
// }
//
// ensure the command is committed before return.
func (rf *Raft) Start(command interface{}) (index int, term int, ok bool) {
	index = int(NilIndex)
	term = int(NilTerm)
	ok = true

	// Your code here (2B).
	cf, ok := rf.StartFuture(command)

	if !ok {
		return
	}

	zap.L().Debug("start",
		zap.Stringer("server", rf.me),
		zap.Stringer("term", rf.getCurrentTerm()),
		zap.Stringer("state", rf.getState()),
		zap.Any("command", command))

	// wait for commit or error
	if idx, t, err := cf.Get(); err != nil {
		zap.L().Error("start future error",
			zap.Stringer("server", rf.me),
			zap.Stringer("term", rf.getCurrentTerm()),
			zap.Stringer("state", rf.getState()),
			zap.Any("command", command),
			zap.Error(err))
		ok = false
	} else {
		zap.L().Debug("start future finish",
			zap.Stringer("server", rf.me),
			zap.Stringer("term", rf.getCurrentTerm()),
			zap.Stringer("state", rf.getState()),
			zap.Any("command", command))
		index = int(idx)
		term = int(t)
	}

	return
}

func (rf *Raft) StartWithCtx(ctx context.Context, command interface{}) (index int, term int, ok bool) {
	index = int(NilIndex)
	term = int(NilTerm)
	ok = true

	// Your code here (2B).
	cf, ok := rf.StartFuture(command)

	if !ok {
		return
	}

	zap.L().Debug("start",
		zap.Stringer("server", rf.me),
		zap.Stringer("term", rf.getCurrentTerm()),
		zap.Stringer("state", rf.getState()),
		zap.Any("command", command))

	// wait for commit or error
	if idx, t, err := cf.GetWithCtx(ctx); err != nil {
		zap.L().Error("start future error",
			zap.Stringer("server", rf.me),
			zap.Stringer("term", rf.getCurrentTerm()),
			zap.Stringer("state", rf.getState()),
			zap.Any("command", command),
			zap.Error(err))
		ok = false
	} else {
		zap.L().Debug("start future finish",
			zap.Stringer("server", rf.me),
			zap.Stringer("term", rf.getCurrentTerm()),
			zap.Stringer("state", rf.getState()),
			zap.Any("command", command))
		index = int(idx)
		term = int(t)
	}

	return
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.raftCtxCancel()
	rf.goroutineWg.Wait()
}

func (rf *Raft) updateHeartbeatInfo(s ServerId, traceId uint32) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.heartBeatInfo[s] = ackInfo{
		traceId: traceId,
		ts:      time.Now(),
	}
}

// return true if some heartbeat successfully with majority within election timeout.
// the heartbeat maybe not the most recent one.
func (rf *Raft) checkMajorityHeartbeat() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	nowTs := time.Now()
	count := make(map[uint32]int)
	for _, ackInfo := range rf.heartBeatInfo {
		if nowTs.Sub(ackInfo.ts) >= rf.electionDuration {
			continue
		}

		if _, ok := count[ackInfo.traceId]; ok {
			count[ackInfo.traceId]++
		} else {
			count[ackInfo.traceId] = 1
		}

		if rf.checkMajority(count[ackInfo.traceId]) {
			return true
		}
	}

	return false
}

// ensure leader finish setup
func (rf *Raft) getIsLeaderSetup() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.isLeaderSetup
}

func (rf *Raft) updateIsLeaderSetup(b bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.isLeaderSetup = b
}

func (rf *Raft) getLastContact() time.Time {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastContact
}

func (rf *Raft) updateLastContact() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastContact = time.Now()
}

func (rf *Raft) getLogInfoAt(i Index) (Index, Term) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	numLogs := len(rf.log)
	lastLogIndex := rf.log[numLogs-1].Index
	if i > lastLogIndex {
		return NilIndex, NilTerm
	}
	return rf.log[i].Index, rf.log[i].Term
}

// get log[b, e)
func (rf *Raft) getEntries(i ...Index) []*Entry {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var b Index
	var e Index
	if len(i) == 1 {
		b = i[0]
		e = b + 1
	} else if len(i) == 2 {
		b = i[0]
		e = i[1]
	} else {
		zap.L().Panic("only one or two indexes is accepted", zap.Any("i", i))
	}
	return rf.log[b:e]
}

func (rf *Raft) getMatchIndex(s ServerId) Index {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.matchIndex[s]
}

func (rf *Raft) setMatchIndex(s ServerId, new Index) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.matchIndex[s] = new
}

func (rf *Raft) getNextIndex(s ServerId) Index {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.nextIndex[s]
}

func (rf *Raft) setNextIndex(s ServerId, new Index) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.nextIndex[s] = new
}

func (rf *Raft) getPrevLogInfo(index Index) (Index, Term) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index == 0 {
		zap.L().Panic("the previous log of index 0 doesn't exist")
	}

	numLogs := len(rf.log)
	if index > Index(numLogs) {
		zap.L().Panic("entry doesn't exist",
			zap.Stringer("index", index),
			zap.Int("num of log", numLogs))
	}

	prevLogIndex := rf.log[index-1].Index
	prevLogTerm := rf.log[index-1].Term
	return prevLogIndex, prevLogTerm
}

func (rf *Raft) getLastLogInfo() (Index, Term) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	numLogs := len(rf.log)
	lastLogIndex := rf.log[numLogs-1].Index
	lastLogTerm := rf.log[numLogs-1].Term
	return lastLogIndex, lastLogTerm
}

func (rf *Raft) setCurrentTerm(t Term) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = t
}

func (rf *Raft) getCurrentTerm() Term {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}

func (rf *Raft) setCommitIndex(i Index) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.commitIndex = i
}

func (rf *Raft) getCommitIndex() Index {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.commitIndex
}

// if there exists an N such that N > commitIndex,
// a majority of matchIndex[i] ≥ N,
// and log[N].term == currentTerm: set commitIndex = N.
//
// if entry is committed, then notify to apply.
func (rf *Raft) updateCommitIndex(n Index) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if n < rf.commitIndex {
		return
	}

	if rf.log[n].Term != rf.currentTerm {
		return
	}

	num := 0
	for _, m := range rf.matchIndex {
		if m >= n {
			num++
		}
	}

	if rf.checkMajority(num) {
		rf.commitIndex = n
		notify(rf.backgroundApplyCh)
	}
}

func (rf *Raft) setState(s ServerState) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = s
}

func (rf *Raft) getState() ServerState {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

func (rf *Raft) isLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return false
	}

	entry := rf.log[rf.commitIndex]
	if entry.Term == rf.currentTerm {
		return true
	}

	return false
}

// get the lower bound of term.
func (rf *Raft) getLowerBoundOfTerm(t Term) (Index, Term) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	zap.L().Debug("logs",
		zap.Stringer("server", rf.me),
		zap.Stringer("term", rf.currentTerm),
		zap.Stringer("state", rf.state),
		zap.Any("logs", rf.log))

	// find lower bound
	first0 := 0
	last0 := len(rf.log)

	first := first0
	last := last0

	for first < last {
		mid := first + (last-first)/2
		if rf.log[mid].Term < t {
			first = mid + 1
		} else {
			last = mid
		}
	}

	if last == last0 {
		return NilIndex, NilTerm
	}

	return rf.log[last].Index, rf.log[last].Term
}

// call by main goroutine.
//
// return true if the num meet majority.
func (rf *Raft) checkMajority(num int) bool {
	numServers := len(rf.peers)
	if 2*num > numServers {
		return true
	}
	return false
}

// call by main goroutine.
func (rf *Raft) handleRPC(rpc *RPCFuture) {
	switch req := rpc.request.(type) {
	case *RequestVoteArgs:
		rf.handleRequestVote(rpc, req)
	case *AppendEntriesArgs:
		rf.handleAppendEntries(rpc, req)
	default:
		zap.L().Panic("unknown rpc type", zap.Any("rpc request", rpc.request))
	}
}

// call by main goroutine.
func (rf *Raft) runApply(ctx context.Context) {
	defer rf.goroutineWg.Done()

	backgroundTicker := time.NewTicker(rf.applyDuration)
	defer backgroundTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			zap.L().Info("background apply stop",
				zap.Stringer("server", rf.me),
				zap.Stringer("term", rf.getCurrentTerm()),
				zap.Stringer("state", rf.getState()))
			return
		case <-backgroundTicker.C:
		case <-rf.backgroundApplyCh:
		}

		zap.L().Debug("get background apply notify",
			zap.Stringer("server", rf.me),
			zap.Stringer("term", rf.getCurrentTerm()),
			zap.Stringer("state", rf.getState()),
			zap.Stringer("commit index", rf.getCommitIndex()),
			zap.Stringer("last applied", rf.lastApplied))

		commitIndex := rf.getCommitIndex()

		rf.mu.Lock()
		i := 0
		for _, cf := range rf.commitFutures {
			if cf.index <= commitIndex {
				cf.Respond(nil)
			} else {
				break
			}

			i++
		}

		if i > 0 {
			rf.commitFutures = rf.commitFutures[i:]
		}
		rf.mu.Unlock()

		for commitIndex > rf.lastApplied {
			rf.lastApplied++

			rf.mu.Lock()
			entry := rf.log[rf.lastApplied]
			rf.mu.Unlock()

			if entry.Type == Command {
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      entry.Data,
					CommandIndex: int(rf.lastApplied),
				}
				rf.applyCh <- applyMsg

				zap.L().Info("background apply",
					zap.Stringer("server", rf.me),
					zap.Stringer("term", rf.getCurrentTerm()),
					zap.Stringer("state", rf.getState()),
					zap.Any("entry", entry))
			}
		}
	}
}

// call by main goroutine.
func (rf *Raft) run(ctx context.Context) {
	defer rf.goroutineWg.Done()

	for {
		select {
		case <-ctx.Done():
			zap.L().Info("shutdown",
				zap.Stringer("server", rf.me),
				zap.Stringer("term", rf.getCurrentTerm()),
				zap.Stringer("state", rf.getState()))
			return
		default:
		}

		rf.goroutineWg.Add(1)
		switch rf.getState() {
		case Candidate:
			rf.runCandidate(ctx)
		case Follower:
			rf.runFollower(ctx)
		case Leader:
			rf.runLeader(ctx)
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
	// Your initialization code here (2A, 2B, 2C).
	atom := zap.NewAtomicLevelAt(zap.DebugLevel)
	encoderCfg := zap.NewProductionEncoderConfig()
	logger := zap.New(
		zapcore.NewCore(
			zapcore.NewJSONEncoder(encoderCfg),
			zapcore.Lock(os.Stdout),
			atom),
		zap.AddCaller(),
	)
	defer func() { _ = logger.Sync() }()
	zap.ReplaceGlobals(logger)

	rf := &Raft{
		peers:             peers,
		persister:         persister,
		me:                ServerId(me),
		currentTerm:       0,
		votedFor:          NilServerId,
		votedForTerm:      0,
		log:               make([]*Entry, 0),
		commitIndex:       0,
		lastApplied:       0,
		nextIndex:         nil,
		matchIndex:        nil,
		applyDuration:     time.Millisecond * 100,
		heartbeatDuration: time.Millisecond * 30,
		electionDuration:  time.Millisecond * 300,
		rpcTimeout:        time.Millisecond * 20,
		state:             Follower,
		rpcCh:             make(chan *RPCFuture, 1),
		applyCh:           applyCh,
		appendResultCh:    nil,
		commitFutures:     nil,
		backgroundApplyCh: make(chan Notification),
		replicateNotifyCh: nil,
		heartBeatNotifyCh: nil,
		heartBeatInfo:     nil,
		isLeaderSetup:     false,
	}

	var ctx context.Context
	ctx, rf.raftCtxCancel = context.WithCancel(context.Background())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	if len(rf.log) == 0 {
		entry := makeNoopEntry()
		entry.Index = 0
		rf.log = append(rf.log, entry)
	}

	rf.goroutineWg.Add(2)
	go rf.run(ctx)
	go rf.runApply(ctx)

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	return rf
}
