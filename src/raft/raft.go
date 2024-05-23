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
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"

	dlog "6.5840/debug"
)

const (
	// in microseconds
	electionTimeoutBase  = 500
	electionTImeoutDelta = 500
	heartbeatTimeout     = 150

	followerServer  = 0
	candidateServer = 1
	leaderServer    = 2

	voteForNothing = -1

	pollPause = 20 // polling consumes CPU and affects scheduling, need time.Sleep, pollPause * time.MillSecond
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	identity       int32 // follower, candidate, leader
	currentTerm    int32
	votedFor       int32
	leaderId       int32
	electionTicker *time.Ticker
	logs           []LogEntry
	nextIndex      []int
	matchIndex     []int
	applyCh        chan ApplyMsg
	commitIndex    int32
	lastApplied    int32
}

func getElectionTime() time.Duration {
	ms := electionTimeoutBase + rand.Int()%electionTImeoutDelta
	return time.Duration(ms) * time.Millisecond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = int(rf.currentTerm)
	isleader = rf.identity == leaderServer
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int32
	CandidateID  int32
	LastLogIndex int
	LastlogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int32
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if rf.currentTerm > args.Term {
		dlog.Printf(dlog.DVote, "S%d refused to vote S%d(%d > %d)", rf.me, args.CandidateID, rf.currentTerm, args.Term)
		return
	}
	if rf.currentTerm == args.Term {
		// avoid vote again in the same term
		// TODO  rf.votedFor == args.CandidateID && rf.votedFor != voteForNothing --> ||
		if rf.votedFor != voteForNothing {
			dlog.Printf(dlog.DVote, "S%d refused to vote S%d(vote for %d))", rf.me, args.CandidateID, rf.votedFor)
			return
		}
	}

	if len(rf.logs) > 0 && (rf.logs[len(rf.logs)-1].Term > args.LastlogTerm ||
		(rf.logs[len(rf.logs)-1].Term == args.LastlogTerm &&
			len(rf.logs)-1 > args.LastLogIndex)) {
		dlog.Printf(dlog.DVote, "S%d refused to vote S%d(log))", rf.me, args.CandidateID)
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			if rf.identity == leaderServer {
				rf.identity = followerServer // for the safty in the heart
				rf.electionTicker.Reset(getElectionTime())
				rf.votedFor = voteForNothing
				go rf.ticker()
			} else {
				rf.identity = followerServer // there is no need to reset election ticker
			}
		}
		return
	}

	// rf.currentTerm < args.Term || rf.currentTerm == args.Term and can vote
	dlog.Printf(dlog.DVote, "S%d(%d) -> S%d(%d)", rf.me, rf.currentTerm, args.CandidateID, args.Term)

	reply.VoteGranted = true
	rf.votedFor = args.CandidateID

	if rf.identity == leaderServer && rf.currentTerm < args.Term {
		rf.identity = followerServer // for the safty in the heart
		rf.electionTicker.Reset(getElectionTime())
		go rf.ticker()
	} else {
		rf.identity = followerServer               // candidate feel bigger term
		rf.electionTicker.Reset(getElectionTime()) // reset election timer
	}
	rf.currentTerm = args.Term
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntryArgs struct {
	Term         int32
	LeadId       int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry

	LeaderCommit int
}

type AppendEntryReply struct {
	Term    int32
	Success bool
}

func (rf *Raft) handleHeartBeat(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		reply.Success = false
		// dlog.Printf(dlog.DInfo, "S%d refused to heartbeat from S%d(%d > %d)", rf.me, args.LeadId, rf.currentTerm, args.Term)
		return
	}
	if rf.identity == leaderServer && rf.currentTerm == args.Term {
		// just check it
		dlog.Printf(dlog.DError, "S%d find S%d is a leader, one term has two leader", rf.me, args.LeadId)
		reply.Success = false
		return
	}
	// TODO debug:为什么rf.lastApplied not updated
	dlog.Printf(dlog.DInfo, "S%d receive heartbeat S%d leadercommit %d, self lastApplied %d", rf.me, args.LeadId, args.LeaderCommit, rf.lastApplied)
	updateCommitIndex := min(int32(args.LeaderCommit), rf.lastApplied)

	if rf.commitIndex < int32(args.LeaderCommit) && rf.commitIndex < updateCommitIndex {
		dlog.Printf(dlog.DCommit, "S%d need commit something, updatecommit %d -> %d", rf.me, rf.commitIndex, updateCommitIndex)
		for rf.commitIndex += 1; ; rf.commitIndex++ {
			if rf.logs[rf.commitIndex].Command == "dummy msg" {
				if rf.commitIndex == updateCommitIndex {
					break
				}
				continue
			}
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[rf.commitIndex].Command,
				CommandIndex: int(rf.commitIndex),
			}
			dlog.Printf(dlog.DCommit, "S%d commit %d", rf.me, rf.commitIndex)
			if rf.commitIndex == updateCommitIndex {
				break
			}
		}
	}
	if rf.currentTerm == args.Term {
		if rf.identity == leaderServer {
			// just check it
			dlog.Printf(dlog.DError, "S%d find S%d is a leader, one term has two leader", rf.me, args.LeadId)
			reply.Success = false
			return
		} else {
			rf.identity = followerServer // candidate feel bigger term
			rf.electionTicker.Reset(getElectionTime())
			reply.Success = true
		}
		return
	} else {
		rf.currentTerm = args.Term

		if rf.identity == leaderServer {
			go rf.ticker()
		}
		rf.identity = followerServer // candidate feel bigger term
		rf.electionTicker.Reset(getElectionTime())
		reply.Success = true
		rf.votedFor = voteForNothing
	}
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	if len(args.Entries) == 0 {
		rf.handleHeartBeat(args, reply)
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if rf.currentTerm > args.Term {
		reply.Success = false
		dlog.Printf(dlog.DInfo, "S%d refused to appendEntries from S%d(%d > %d)", rf.me, args.LeadId, rf.currentTerm, args.Term)
		return
	}

	if rf.currentTerm == args.Term {
		if rf.identity == leaderServer {
			// just check it
			dlog.Printf(dlog.DError, "S%d find S%d is a leader, one term has two leader", rf.me, args.LeadId)
			reply.Success = false
			return
		}
		rf.identity = followerServer // candidate feel bigger term
		rf.electionTicker.Reset(getElectionTime())
	} else {
		rf.currentTerm = args.Term

		if rf.identity == leaderServer {
			rf.identity = followerServer
			go rf.ticker()
		}
		rf.identity = followerServer // candidate feel bigger term
		rf.electionTicker.Reset(getElectionTime())
		rf.votedFor = voteForNothing
	}

	if len(rf.logs) <= args.PrevLogIndex || (args.PrevLogIndex >= 0 && rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm) {
		dlog.Printf(dlog.DLog, "S%d refused appendEndtrys from S%d, index %d", rf.me, args.LeadId, args.PrevLogIndex+1)
		reply.Success = false
		return
	}
	dlog.Printf(dlog.DLog, "S%d(len %d preidx %d preTerm %d args.leadercommit %d) accept S%d entry %d %v", rf.me, len(rf.logs), args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.LeadId, args.Term, args.Entries[0])
	reply.Success = true

	i := 0
	for ; i < len(args.Entries); i++ {
		if i+args.PrevLogIndex+1 == len(rf.logs) {
			break
		}
		rf.logs[i+args.PrevLogIndex+1] = args.Entries[i]
	}
	rf.logs = append(rf.logs, args.Entries[i:]...)

	rf.lastApplied = int32(args.PrevLogIndex + len(args.Entries))

	updateCommitIndex := min(int32(args.LeaderCommit), rf.lastApplied)

	if rf.commitIndex < int32(args.LeaderCommit) && rf.commitIndex < updateCommitIndex {
		dlog.Printf(dlog.DCommit, "S%d need commit something, updatecommit %d -> %d", rf.me, rf.commitIndex, updateCommitIndex)
		for rf.commitIndex += 1; ; rf.commitIndex++ {
			if rf.logs[rf.commitIndex].Command == "dummy msg" {
				if rf.commitIndex == updateCommitIndex {
					break
				}
				continue
			}
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[rf.commitIndex].Command,
				CommandIndex: int(rf.commitIndex),
			}
			dlog.Printf(dlog.DCommit, "S%d commit %d", rf.me, rf.commitIndex)
			if rf.commitIndex == updateCommitIndex {
				break
			}
		}
	}

	return
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.identity != leaderServer {

		isLeader = false
		return
	}
	// Your code here (3B).
	index = int(rf.lastApplied) + 1
	term = int(rf.currentTerm)
	isLeader = true
	if len(rf.logs) == index {
		rf.logs = append(rf.logs, LogEntry{
			Term:    int(rf.currentTerm),
			Command: command,
		})

	} else {
		rf.logs[index] = LogEntry{
			Term:    int(rf.currentTerm),
			Command: command,
		}
	}
	rf.lastApplied++
	dlog.Printf(dlog.DLeader, "S%d accept new task %d", rf.me, index)
	return
}

func (rf *Raft) sendDummyLogEntry(startTerm int, dummyIndex int, server int, report chan struct{}) {
	args := AppendEntryArgs{
		LeadId:  rf.me,
		Entries: make([]LogEntry, 1),
	}
	reply := AppendEntryReply{}
	for rf.nextIndex[server] != dummyIndex+1 {
		args.PrevLogIndex = rf.nextIndex[server] - 1
		if args.PrevLogIndex >= 0 {
			args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
		}
		args.Entries[0] = rf.logs[rf.nextIndex[server]]
		// dlog.Printf(dlog.DLog, "S%d send dummy S%d, log index %d", rf.me, server, rf.nextIndex[server])
		for {
			rf.mu.Lock()

			if rf.identity != leaderServer || startTerm != int(rf.currentTerm) {
				rf.mu.Unlock()
				return
			}
			args.Term = rf.currentTerm
			args.LeaderCommit = int(rf.commitIndex)
			rf.mu.Unlock()
			ok := rf.sendAppendEntry(server, &args, &reply)
			if ok {
				break
			}
		}
		if rf.identity != leaderServer || startTerm != int(rf.currentTerm) {
			rf.mu.Unlock()
			return
		}
		rf.mu.Lock()

		if reply.Term > rf.currentTerm && rf.identity == leaderServer {
			dlog.Printf(dlog.DLeader, "S%d find S%d's term(%d) bigger", rf.me, server, reply.Term)
			rf.identity = followerServer
			rf.votedFor = voteForNothing
			rf.currentTerm = reply.Term
			rf.electionTicker.Reset(getElectionTime())
			go rf.ticker()
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		if reply.Success {
			rf.nextIndex[server]++
		} else {
			dlog.Printf(dlog.DLog, "S%d fail send dummy S%d, log index %d", rf.me, server, rf.nextIndex[server])
			rf.nextIndex[server]--
			if rf.nextIndex[server] < 0 {
				dlog.Printf(dlog.DInfo, "S%d %d nextIndex %d", rf.me, server, rf.nextIndex[server])
			}
		}
	}
	rf.mu.Lock()
	rf.matchIndex[server] = dummyIndex
	rf.mu.Unlock()
	report <- struct{}{}
	// dlog.Printf(dlog.DLog, "S%d --> S%d dummy goroutine panic dummyidx %d startDummyidx %d sendIdx %d startNext: %d", rf.me, server, dummyIndex, startDummyidx, rf.nextIndex[server], startNext)

}

func (rf *Raft) sendRealLogEntry(startTerm int, server int, task chan int) {
	rf.mu.Lock()
	args := AppendEntryArgs{
		Term:    rf.currentTerm,
		LeadId:  rf.me,
		Entries: make([]LogEntry, 1),
	}
	rf.mu.Unlock()
	reply := AppendEntryReply{}
	for {
		index := <-task
		if index == 0 {
			// task close
			break
		}
		args.PrevLogIndex = index - 1
		args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
		args.Entries[0] = rf.logs[index]

		for atomic.LoadInt32(&rf.identity) == leaderServer {
			rf.mu.Lock()
			if rf.identity != leaderServer || startTerm != int(rf.currentTerm) {
				rf.mu.Unlock()
				return
			}
			args.Term = rf.currentTerm
			args.LeaderCommit = int(rf.commitIndex)
			rf.mu.Unlock()
			ok := rf.sendAppendEntry(server, &args, &reply)
			if ok {
				break
			}
		}
		rf.mu.Lock()
		if rf.identity != leaderServer || startTerm != int(rf.currentTerm) {
			rf.mu.Unlock()
			return
		}
		if reply.Term > rf.currentTerm && rf.identity == leaderServer {
			dlog.Printf(dlog.DLeader, "S%d find S%d's term(%d) bigger", rf.me, server, reply.Term)
			rf.identity = followerServer
			rf.votedFor = voteForNothing
			rf.currentTerm = reply.Term
			rf.electionTicker.Reset(getElectionTime())
			go rf.ticker()
			rf.mu.Unlock()
			return
		}
		if !reply.Success {
			dlog.Printf(dlog.DInfo, "S%d refuse to append %v", server, args.Entries)
		}
		if rf.matchIndex[server] != index-1 {
			dlog.Printf(dlog.DInfo, "S%d matchIndex %d shouldn't update %d", server, rf.matchIndex[server], index)
		}
		// dlog.Printf(dlog.DInfo, "S%d --> S%d update matchIndex %d", rf.me, server, index)
		rf.matchIndex[server] = index // there is no need to update nextIndex
		rf.mu.Unlock()
	}
}

// TODO important debug 为什么dummyIndex 奇怪

func (rf *Raft) copyLogs(dummyIndex int) {
	// 首先是发送空日志，这个空日志在大多数服务器上完成之后，提交之前没有提交的信息然后在轮询是否有新日志需要发送
	dlog.Printf(dlog.DLeader, "S%d start to copy %d dummy log", rf.me, dummyIndex)

	report := make(chan struct{}, len(rf.peers))
	defer func() {
		for range report {
		}
		close(report)
	}()
	rf.mu.Lock()
	dummyTerm := rf.logs[dummyIndex].Term
	rf.mu.Unlock()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendDummyLogEntry(dummyTerm, int(dummyIndex), i, report)
	}
	dummySend := 0
	for dummySend < len(rf.peers)/2 {
		select {
		case <-report:
			dummySend++
		default:
			rf.mu.Lock()
			if rf.identity != leaderServer {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			time.Sleep(pollPause * time.Millisecond)
		}
	}
	dlog.Printf(dlog.DCommit, "S%d dummy log commit", rf.me)
	for rf.commitIndex += 1; rf.commitIndex < int32(dummyIndex); rf.commitIndex++ {
		if rf.logs[rf.commitIndex].Command == "dummy msg" {
			continue
		}
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[rf.commitIndex],
			CommandIndex: int(rf.commitIndex),
		}
		dlog.Printf(dlog.DCommit, "S%d commit %d task", rf.me, rf.commitIndex)
	}
	taskBroadcast := make([]chan int, len(rf.peers))
	defer func() {
		for i := range rf.peers {
			for range taskBroadcast[i] {
			}
			close(taskBroadcast[i])
		}
	}()
	dlog.Printf(dlog.DLog, "S%d start to accept request", rf.me)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		taskBroadcast[i] = make(chan int, 1024) // Assuming it won't jam.
		go func(server int, task chan int) {
			for {
				rf.mu.Lock()
				if rf.matchIndex[server] == int(dummyIndex) {
					rf.mu.Unlock()
					break
				}
				rf.mu.Unlock()
				time.Sleep(pollPause * time.Millisecond)
			}
			rf.sendRealLogEntry(rf.logs[dummyIndex].Term, server, taskBroadcast[server])
		}(i, taskBroadcast[i])
	}
	lastDistribute := int32(dummyIndex)
	for {
		rf.mu.Lock()
		if rf.identity != leaderServer {
			rf.mu.Unlock()
			dlog.Printf(dlog.DLeader, "S%d know not leader", rf.me)
			break
		}
		if rf.commitIndex == rf.lastApplied {
			rf.mu.Unlock()
			dlog.Printf(dlog.DLog, "S%d has no task sleep", rf.me)
			time.Sleep(pollPause * 20 * time.Millisecond)
			continue
		}
		if lastDistribute == rf.commitIndex+1 {
			count := 0
			for i, v := range rf.matchIndex {
				if i == rf.me {
					continue
				}
				if v >= int(lastDistribute) {
					count++
				}

			}
			if count < len(rf.peers)/2 {
				time.Sleep(pollPause * time.Millisecond)
			} else {
				rf.commitIndex++
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[lastDistribute].Command,
					CommandIndex: int(lastDistribute),
				}
				dlog.Printf(dlog.DCommit, "S%d commit %d task", rf.me, lastDistribute)
			}
			rf.mu.Unlock()
			continue
		}

		lastDistribute = rf.commitIndex + 1
		dlog.Printf(dlog.DLog, "S%d distribute new task %d", rf.me, lastDistribute)
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			select {
			case taskBroadcast[i] <- int(lastDistribute):
			default:
				dlog.Printf(dlog.DError, "msg inbox full!!")
			}
		}
		rf.mu.Unlock()
	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) sendHeartbeat(server int, heartBeatTerm int32, appendArgs AppendEntryArgs) {
	reply := AppendEntryReply{}
	ok := rf.sendAppendEntry(server, &appendArgs, &reply)
	if !ok {
		// dlog.Printf(dlog.DInfo, "S%d failed to send heartBeat S%d", rf.me, server)
		return
		// Heartbeat sending failed and needs to be retransmitted immediately.
	}
	rf.mu.Lock()
	if reply.Term > heartBeatTerm && rf.identity == leaderServer {
		dlog.Printf(dlog.DLeader, "S%d find S%d's term(%d) bigger", rf.me, server, reply.Term)
		rf.identity = followerServer
		rf.votedFor = voteForNothing
		rf.currentTerm = reply.Term
		rf.electionTicker.Reset(getElectionTime())
		go rf.ticker()
	}
	rf.mu.Unlock()
}

func (rf *Raft) maintainLeader() {
	// only a leader has a maintainLeader goroutine
	dlog.Printf(dlog.DLeader, "S%d start to send heartbeat", rf.me)
	for idx := 0; idx < len(rf.peers); idx++ {
		if rf.me == idx {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			appendArgs := AppendEntryArgs{
				LeadId: rf.me,
				Term:   rf.currentTerm,
			}
			rf.mu.Unlock()
			for {
				rf.mu.Lock()
				if rf.identity != leaderServer {
					rf.mu.Unlock()
					return
				}
				appendArgs.LeaderCommit = int(rf.commitIndex)
				go rf.sendHeartbeat(server, rf.currentTerm, appendArgs)
				rf.mu.Unlock()
				time.Sleep(heartbeatTimeout * time.Millisecond)
			}
		}(idx)
	}
}

func (rf *Raft) startLead() {
	dlog.Printf(dlog.DLeader, "S%d become a leader", rf.me)
	rf.mu.Lock()
	dummyLog := LogEntry{
		Term:    int(rf.currentTerm),
		Command: "dummy msg",
	}
	dummyIndex := rf.lastApplied + 1
	if len(rf.logs) == int(dummyIndex) {
		rf.logs = append(rf.logs, dummyLog)
	} else {
		rf.logs[dummyIndex] = dummyLog
	}
	rf.lastApplied++
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = int(dummyIndex)
		rf.matchIndex[i] = 0
	}

	rf.identity = leaderServer
	rf.mu.Unlock()
	go rf.copyLogs(int(dummyIndex))
	go rf.maintainLeader()
}

func (rf *Raft) startElection(electionTerm int) {
	// only a candidate has a startElection goroutine
	dlog.Printf(dlog.DLog, "S%d start %d term election", rf.me, electionTerm)

	rf.mu.Lock()
	voteArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  int32(rf.me),
		LastLogIndex: len(rf.logs) - 1,
	}
	if len(rf.logs) > 0 {
		voteArgs.LastlogTerm = rf.logs[len(rf.logs)-1].Term
	}
	rf.mu.Unlock()

	wg := sync.WaitGroup{}
	voteChannel := make(chan bool, len(rf.peers))
	for idx := 0; idx < len(rf.peers); idx++ {
		if rf.me == idx {
			continue
		}
		wg.Add(1)
		go func(server int) {
			defer wg.Done()
			reply := RequestVoteReply{}
			// dlog.Printf(dlog.DVote, "S%d prepare send request vote S%d", rf.me, server)
			ok := rf.sendRequestVote(server, &voteArgs, &reply)
			if !ok {
				// dlog.Printf(dlog.DError, "S%d fail send request vote S%d", rf.me, server)
				voteChannel <- false
				return
			}
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				dlog.Printf(dlog.DVote, "S%d find S%d's term(%d) bigger than itself -> follower", rf.me, server, reply.Term)
				voteChannel <- false
				rf.currentTerm = reply.Term
				rf.identity = followerServer // return to the loop of election ticker
				rf.votedFor = voteForNothing
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			voteChannel <- reply.VoteGranted

		}(idx)
	}

	vote := 1
	count := 1
LOOP:
	for {
		select {
		case flag := <-voteChannel:
			count++
			if flag {
				vote++
			}
			if vote > len(rf.peers)/2 {
				rf.mu.Lock()
				if rf.identity != candidateServer || rf.currentTerm != int32(electionTerm) {
					rf.mu.Unlock()
					break LOOP
				}
				rf.mu.Unlock()
				// success to be elected to leader
				rf.startLead()
				break LOOP
			}
			if count == len(rf.peers) {
				dlog.Printf(dlog.DVote, "S%d receive %v votes(< %v), failed", rf.me, vote, len(rf.peers)/2+1)
				break LOOP
			}
		default:
			rf.mu.Lock()
			if rf.identity != candidateServer || rf.currentTerm != int32(electionTerm) {
				rf.mu.Unlock()
				break LOOP
			}
			rf.mu.Unlock()
			time.Sleep(pollPause * time.Millisecond) // avoid excessive use of cpu
		}
	}

	wg.Wait()
	close(voteChannel)
	for range voteChannel {
		// clear channel
	}
}

func (rf *Raft) ticker() {
	// leader doesn't have a ticker goroutine
	rf.mu.Lock()
	if rf.identity != followerServer {
		rf.mu.Unlock()
		dlog.Printf(dlog.DError, "S%d not follower but ticker!", rf.me)
		return
	}
	rf.mu.Unlock()
	dlog.Printf(dlog.DLog, "S%d start ticker", rf.me)

	for !rf.killed() {

		<-rf.electionTicker.C
		rf.electionTicker.Stop()
		rf.mu.Lock()
		if rf.identity == leaderServer {
			rf.mu.Unlock()
			return
		}
		rf.identity = candidateServer
		rf.currentTerm++
		rf.votedFor = int32(rf.me)
		go rf.startElection(int(rf.currentTerm))
		rf.mu.Unlock()

		rf.electionTicker.Reset(getElectionTime())
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.leaderId = -1
	rf.applyCh = applyCh
	rf.identity = followerServer
	rf.electionTicker = time.NewTicker(getElectionTime())
	rf.dead = 0
	rf.votedFor = voteForNothing
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
