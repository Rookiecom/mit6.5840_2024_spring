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
	invalidTerm    = -100
	invalidIdx     = -1
	oneTimeCopyNum = 500

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
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
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

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	term = int(rf.currentTerm)
	isleader = rf.identity == leaderServer
	return term, isleader
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

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
		rf.logs = rf.logs[0 : index+1]
	}
	rf.persist()
	rf.lastApplied++
	dlog.Printf(dlog.DLeader, "S%d accept new task %d %v", rf.me, index, command)
	return
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
	// return false
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
	if rf.killed() {
		return
	}
	if reply.Term > heartBeatTerm && rf.identity == leaderServer {
		dlog.Printf(dlog.DLeader, "S%d find S%d's term(%d) bigger", rf.me, server, reply.Term)
		rf.identity = followerServer
		rf.votedFor = voteForNothing
		rf.currentTerm = reply.Term
		rf.persist()
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
			rf.mu.RLock()
			appendArgs := AppendEntryArgs{
				LeadId: rf.me,
				Term:   rf.currentTerm,
			}
			rf.mu.RUnlock()
			for !rf.killed() {
				rf.mu.Lock()
				if rf.identity != leaderServer {
					rf.mu.Unlock()
					return
				}
				appendArgs.PrevLogIndex = int(rf.lastApplied)
				appendArgs.PrevLogTerm = rf.logs[rf.lastApplied].Term
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
	rf.mu.RLock()
	go rf.copyLogs(int(rf.lastApplied) + 1)
	rf.identity = leaderServer
	rf.mu.RUnlock()
	go rf.maintainLeader()
}

func (rf *Raft) ticker() {
	// leader doesn't have a ticker goroutine
	rf.mu.RLock()
	if rf.identity != followerServer {
		rf.mu.RUnlock()
		dlog.Printf(dlog.DError, "S%d not follower but ticker!", rf.me)
		return
	}
	rf.mu.RUnlock()
	dlog.Printf(dlog.DLog, "S%d start ticker", rf.me)

	for !rf.killed() {

		<-rf.electionTicker.C
		rf.electionTicker.Stop()
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		if rf.identity == leaderServer {
			rf.mu.Unlock()
			return
		}
		rf.identity = candidateServer
		rf.currentTerm++
		rf.votedFor = int32(rf.me)
		rf.persist()
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

	rf.leaderId = -1
	rf.applyCh = applyCh
	rf.identity = followerServer
	rf.electionTicker = time.NewTicker(getElectionTime())
	rf.dead = 0
	rf.votedFor = voteForNothing
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	if len(rf.logs) == 0 {
		rf.commitIndex = 0 // just for test
		rf.lastApplied = 0 // just for test, first log must in 1 index
		rf.logs = append(rf.logs, LogEntry{
			Term:    -1,
			Command: "你好",
		})
	} else {
		rf.commitIndex = 0
		rf.lastApplied = int32(len(rf.logs) - 1)
	}
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
