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
	Term int
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
	electionTicker *time.Ticker
	logs           []LogEntry
	nextIndex      []int
	matchIndex     []int
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
		dlog.Printf(dlog.DVote, "S%d refused to vote S%d\n(%d > %d)", rf.me, args.CandidateID, rf.currentTerm, args.Term)
		return
	}
	if rf.currentTerm == args.Term {
		if rf.votedFor == int32(rf.me) || rf.votedFor == args.CandidateID || rf.votedFor != voteForNothing {
			dlog.Printf(dlog.DVote, "S%d refused to vote S%d(vote for %d))", rf.me, args.CandidateID, rf.votedFor)
			return
		}
		if len(rf.logs) > 0 && (rf.logs[len(rf.logs)-1].Term > args.LastlogTerm ||
			(rf.logs[len(rf.logs)-1].Term == args.LastlogTerm &&
				len(rf.logs)-1 > args.LastLogIndex)) {
			return
		}

	}
	// rf.currentTerm < args.Term || rf.currentTerm == args.Term and can vote
	dlog.Printf(dlog.DVote, "S%d -> S%d", rf.me, args.CandidateID)

	reply.VoteGranted = true
	rf.votedFor = args.CandidateID
	rf.currentTerm = args.Term

	if rf.identity == leaderServer && rf.currentTerm < args.Term {
		rf.identity = followerServer // for the safty in the heart
		rf.electionTicker.Reset(getElectionTime())
		go rf.ticker()
	}

	rf.identity = followerServer               // candidate feel bigger term
	rf.electionTicker.Reset(getElectionTime()) // reset election timer
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

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	// 收到心跳消息之后需要：原子将isConnectLeader 变成true
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.currentTerm

	if rf.currentTerm > args.Term {
		dlog.Printf(dlog.DInfo, "S%d refused to appendEntries from S%d(%d > %d)", rf.me, args.LeadId, rf.currentTerm, args.Term)
		return
	}
	dlog.Printf(dlog.DInfo, "S%d receive heartbeat S%d", rf.me, args.LeadId)
	if rf.currentTerm == args.Term {
		rf.identity = followerServer // candidate feel bigger term
		rf.electionTicker.Reset(getElectionTime())
		reply.Success = true
		return
	}
	rf.currentTerm = args.Term

	if rf.identity == leaderServer {
		rf.identity = followerServer
		go rf.ticker()
	}
	rf.identity = followerServer // candidate feel bigger term
	rf.electionTicker.Reset(getElectionTime())
	reply.Success = true
	rf.votedFor = voteForNothing

	// TODO 日志复制

}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	// dlog.Printf(dlog.DTest, "S%d send heartbeat !!!!", rf.me)
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
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

func (rf *Raft) sendHeartbeat(server int, appendArgs *AppendEntryArgs) {
	rf.mu.Lock()
	appendArgs.Term = rf.currentTerm
	appendArgs.PrevLogIndex = len(rf.logs) - 1
	if appendArgs.PrevLogIndex >= 0 {
		appendArgs.PrevLogTerm = rf.logs[len(rf.logs)-1].Term
	} else {
		appendArgs.PrevLogTerm = -1 // ???
	}
	rf.mu.Unlock()
	reply := AppendEntryReply{}
	ok := rf.sendAppendEntry(server, appendArgs, &reply)
	if !ok {
		dlog.Printf(dlog.DError, "S%d failed to connect S%d", rf.me, server)
		return
		// Heartbeat sending failed and needs to be retransmitted immediately.
	}
	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		dlog.Printf(dlog.DLeader, "S%d find S%d's term bigger", rf.me, server)
		rf.identity = followerServer
		rf.votedFor = voteForNothing
		rf.currentTerm = reply.Term
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
			appendArgs := AppendEntryArgs{
				LeadId: rf.me,
			}
			for atomic.LoadInt32(&rf.identity) == leaderServer {
				go rf.sendHeartbeat(server, &appendArgs)
				time.Sleep(heartbeatTimeout * time.Millisecond)
			}
		}(idx)
	}
}

func (rf *Raft) startElection() {
	// only a candidate has a startElection goroutine
	dlog.Printf(dlog.DLog, "S%d start election", rf.me)

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
				dlog.Printf(dlog.DError, "S%d cannot send vote request S%d", rf.me, server)
				voteChannel <- false
				return
			}
			if reply.Term > rf.currentTerm {
				dlog.Printf(dlog.DVote, "S%d find S%d's term bigger than itself -> follower", rf.me, server)
				voteChannel <- false
				rf.mu.Lock()
				rf.currentTerm = reply.Term
				rf.identity = followerServer // return to the loop of election ticker
				rf.votedFor = voteForNothing
				rf.mu.Unlock()
				return
			}

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
				// success to be elected to leader
				dlog.Printf(dlog.DLeader, "S%d become a leader", rf.me)
				rf.identity = leaderServer
				i := 0
				lastLogIndex := len(rf.logs)
				for ; i < len(rf.nextIndex); i++ {
					rf.nextIndex[i] = lastLogIndex
					rf.matchIndex[i] = 0
				}
				for ; i < len(rf.logs); i++ {
					rf.nextIndex = append(rf.nextIndex, lastLogIndex)
					rf.matchIndex = append(rf.matchIndex, 0)
				}

				go rf.maintainLeader()
				break LOOP
			}
			if count == len(rf.peers) {
				dlog.Printf(dlog.DVote, "S%d receive %v votes(< %v), failed", rf.me, vote, len(rf.peers)/2+1)
				break LOOP
			}
		default:
			if atomic.LoadInt32(&rf.identity) != candidateServer {
				break LOOP
			}

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
	if atomic.LoadInt32(&rf.identity) != followerServer {
		dlog.Printf(dlog.DError, "S%d not follower but ticker!", rf.me)
		return
	}
	dlog.Printf(dlog.DLog, "S%d start ticker", rf.me)

	for !rf.killed() {

		<-rf.electionTicker.C
		rf.electionTicker.Stop()
		if atomic.LoadInt32(&rf.identity) == leaderServer {
			return
		}
		rf.mu.Lock()
		rf.identity = candidateServer
		rf.currentTerm++
		rf.mu.Unlock()
		go rf.startElection()

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
	rf.identity = followerServer
	rf.electionTicker = time.NewTicker(getElectionTime())
	rf.dead = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
