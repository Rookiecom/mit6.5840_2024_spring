package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	dlog "6.5840/debug"
)

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

func getElectionTime() time.Duration {
	ms := electionTimeoutBase + rand.Int()%electionTImeoutDelta
	return time.Duration(ms) * time.Millisecond
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
		rf.currentTerm = args.Term
		if rf.identity == leaderServer {
			rf.identity = followerServer // for the safty in the heart
			rf.electionTicker.Reset(getElectionTime())
			rf.votedFor = voteForNothing
			go rf.ticker()
		} else {
			rf.identity = followerServer // there is no need to reset election ticker
		}
		rf.persist()
		return
	} else {
		debugMsg := fmt.Sprintf("S%d vote S%d reason ", rf.me, args.CandidateID)
		if len(rf.logs) == 0 {
			debugMsg += "rf.logs len = 0"
		} else {
			debugMsg += fmt.Sprintf("lastTerm %d args.lastTerm %d lastIndex %d args.lastIndex %d", rf.logs[len(rf.logs)-1].Term, args.LastlogTerm, len(rf.logs)-1, args.LastLogIndex)
		}
		dlog.Printf(dlog.DInfo, debugMsg)
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
	rf.persist()
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

func (rf *Raft) startElection(electionTerm int) {
	// only a candidate has a startElection goroutine
	dlog.Printf(dlog.DLog, "S%d start %d term election", rf.me, electionTerm)

	rf.mu.RLock()
	voteArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  int32(rf.me),
		LastLogIndex: len(rf.logs) - 1,
	}
	if len(rf.logs) > 0 {
		voteArgs.LastlogTerm = rf.logs[len(rf.logs)-1].Term
	}
	rf.mu.RUnlock()

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
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				dlog.Printf(dlog.DVote, "S%d find S%d's term(%d) bigger than itself -> follower", rf.me, server, reply.Term)
				voteChannel <- false
				rf.currentTerm = reply.Term
				rf.identity = followerServer // return to the loop of election ticker
				rf.votedFor = voteForNothing
				rf.persist()
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
	for !rf.killed() {
		select {
		case flag := <-voteChannel:
			count++
			if flag {
				vote++
			}
			if vote > len(rf.peers)/2 {
				rf.mu.RLock()
				if rf.identity != candidateServer || rf.currentTerm != int32(electionTerm) {
					rf.mu.RUnlock()
					break LOOP
				}
				rf.mu.RUnlock()
				// success to be elected to leader
				rf.startLead()
				break LOOP
			}
			if count == len(rf.peers) {
				dlog.Printf(dlog.DVote, "S%d receive %v votes(< %v), failed", rf.me, vote, len(rf.peers)/2+1)
				break LOOP
			}
		default:
			rf.mu.RLock()
			if rf.identity != candidateServer || rf.currentTerm != int32(electionTerm) {
				rf.mu.RUnlock()
				break LOOP
			}
			rf.mu.RUnlock()
			time.Sleep(pollPause * time.Millisecond) // avoid excessive use of cpu
		}
	}

	wg.Wait()
	close(voteChannel)
	for range voteChannel {
		// clear channel
	}
}
