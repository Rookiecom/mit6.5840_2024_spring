package raft

import (
	"math"
	"sort"
	"time"

	dlog "6.5840/debug"
)

type AppendEntryArgs struct {
	Term         int32
	LeadId       int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry

	LeaderCommit int
}

type AppendEntryReply struct {
	Term             int32
	Success          bool
	ConflictTerm     int
	ConflictMinIndex int
}

func (rf *Raft) commit(start, end int32) {
	// rf.mu.Lock()
	commitEntries := make([]LogEntry, end-start+1)
	copy(commitEntries, rf.logs[start:end+1])
	// rf.mu.Unlock()
	for i := 0; i < len(commitEntries) && !rf.killed(); i++ {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      commitEntries[i].Command,
			CommandIndex: i + int(start),
		}
		dlog.Printf(dlog.DCommit, "S%d commit %d", rf.me, i+int(start))
	}
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

	if !(len(rf.logs) <= args.PrevLogIndex || (args.PrevLogIndex > 0 && rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm)) {
		// avoid commit the situation: leader receive task but can not connnet follower, after a while, leader turned to follower;
		// when the real leader send heartbeat to this one, if not limited and rf.commitIndex < args.LeaderCommit < rf.lastApplied
		// it will commit rf.commitIndex ~ args.LeaderCommit(it should not)
		updateCommitIndex := min(int32(args.LeaderCommit), rf.lastApplied)

		if rf.commitIndex < int32(args.LeaderCommit) && rf.commitIndex < updateCommitIndex {
			dlog.Printf(dlog.DCommit, "S%d need commit something, updatecommit %d -> %d", rf.me, rf.commitIndex, updateCommitIndex)
			rf.commit(rf.commitIndex+1, updateCommitIndex)
			rf.commitIndex = updateCommitIndex
		}
	}
	// else {
	// 	// // dlog.Printf(dlog.DInfo, "S%d not commit, logLen %d args preIndex %d preTerm %d rf.logs[args.PrevLogIndex].Term: %d", rf.me, len(rf.logs), args.PrevLogIndex, args.Term, rf.logs[args.PrevLogIndex].Term)
	// 	// debugMsg := fmt.Sprintf("S%d not commit for ", rf.me)
	// 	// if len(rf.logs) <= args.PrevLogIndex {
	// 	// 	debugMsg += fmt.Sprintf("logLen %d < preIndex %d", len(rf.logs), args.PrevLogIndex)
	// 	// } else {
	// 	// 	debugMsg += fmt.Sprintf("rf.logs[args.PrevLogIndex].Term(%d) != args.PrevLogTerm(%d)", rf.logs[args.PrevLogIndex].Term, args.PrevLogTerm)
	// 	// }
	// 	// dlog.Printf(dlog.DInfo, debugMsg)
	// }
	dlog.Printf(dlog.DInfo, "S%d receive heartbeat S%d leadercommit %d, self lastApplied %d", rf.me, args.LeadId, args.LeaderCommit, rf.lastApplied)

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
		rf.persist()
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
	reply.ConflictTerm = invalidTerm
	reply.ConflictMinIndex = invalidIdx

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
		rf.persist()
	}

	if len(rf.logs) <= args.PrevLogIndex {
		dlog.Printf(dlog.DLog, "S%d refused appendEndtrys from S%d, index %d", rf.me, args.LeadId, args.PrevLogIndex+1)
		reply.Success = false
		reply.ConflictMinIndex = len(rf.logs) - 1
		return
	}
	if args.PrevLogIndex > 0 && rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		dlog.Printf(dlog.DLog, "S%d refused appendEndtrys from S%d, index %d", rf.me, args.LeadId, args.PrevLogIndex+1)
		reply.Success = false

		reply.ConflictTerm = rf.logs[args.PrevLogIndex].Term
		for i := args.PrevLogIndex; i >= 0; i-- {
			if rf.logs[i].Term != reply.ConflictTerm {
				reply.ConflictMinIndex = i + 1
				break
			}
		}
		return
	}
	dlog.Printf(dlog.DLog, "S%d(len %d preidx %d preTerm %d args.leadercommit %d) accept S%d(%d) entry(%d)", rf.me, len(rf.logs), args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.LeadId, args.Term, len(args.Entries))
	reply.Success = true

	i := 0
	for ; i < len(args.Entries); i++ {
		if i+args.PrevLogIndex+1 == len(rf.logs) {
			break
		}
		rf.logs[i+args.PrevLogIndex+1] = args.Entries[i]
	}
	rf.logs = append(rf.logs, args.Entries[i:]...)

	rf.logs = rf.logs[0 : args.PrevLogIndex+len(args.Entries)+1]

	rf.persist()

	rf.lastApplied = int32(args.PrevLogIndex + len(args.Entries))

	updateCommitIndex := min(int32(args.LeaderCommit), rf.lastApplied)

	if rf.commitIndex < int32(args.LeaderCommit) && rf.commitIndex < updateCommitIndex {
		dlog.Printf(dlog.DCommit, "S%d need commit something, updatecommit %d -> %d", rf.me, rf.commitIndex, updateCommitIndex)
		rf.commit(rf.commitIndex+1, updateCommitIndex)
		rf.commitIndex = updateCommitIndex
	}
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

func (rf *Raft) sendPreviousLogEntry(startTerm int, firstLogIndex int, server int) {
	args := AppendEntryArgs{
		LeadId: rf.me,
	}
	copyNum := 1
	reply := AppendEntryReply{}
	for rf.nextIndex[server] != firstLogIndex {
		// at first rf.nextIndex[server] = firstLogIndex-1
		rf.mu.RLock()
		args.PrevLogIndex = rf.nextIndex[server] - 1
		if args.PrevLogIndex >= 1 {
			args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
		}
		// dlog.Printf(dlog.DLog, "S%d send dummy S%d, log index %d", rf.me, server, rf.nextIndex[server])
		args.Entries = rf.logs[rf.nextIndex[server]:min(rf.nextIndex[server]+copyNum, firstLogIndex)]
		dlog.Printf(dlog.DLog2, "S%d --> S%d [%d, %d] target %d", rf.me, server, rf.nextIndex[server], min(rf.nextIndex[server]+copyNum, firstLogIndex)-1, firstLogIndex)
		rf.mu.RUnlock()
		for !rf.killed() {
			rf.mu.RLock()

			if rf.identity != leaderServer || startTerm != int(rf.currentTerm) {
				rf.mu.RUnlock()
				return
			}
			args.Term = rf.currentTerm
			args.LeaderCommit = int(rf.commitIndex)
			rf.mu.RUnlock()
			ok := rf.sendAppendEntry(server, &args, &reply)
			if ok {
				break
			}
		}
		if rf.killed() {
			return
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
			rf.persist()
			rf.electionTicker.Reset(getElectionTime())
			go rf.ticker()
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		if reply.Success {
			dlog.Printf(dlog.DLog2, "S%d nextIndex S%d %d += %d", rf.me, server, rf.nextIndex[server], len(args.Entries))
			rf.nextIndex[server] += len(args.Entries)
			copyNum = oneTimeCopyNum
		} else {
			copyNum = 1
			dlog.Printf(dlog.DLog, "S%d fail send dummy S%d, log index %d, conflict (idx %d term %d)", rf.me, server, rf.nextIndex[server], reply.ConflictMinIndex, reply.ConflictTerm)
			if reply.ConflictTerm == invalidTerm {
				if reply.ConflictMinIndex == invalidIdx {
					rf.nextIndex[server]--
				} else {
					rf.nextIndex[server] = reply.ConflictMinIndex
				}
			} else {
				for i := reply.ConflictMinIndex; i <= rf.nextIndex[server]; i++ {
					if rf.logs[i].Term != reply.ConflictTerm {
						rf.nextIndex[server] = i
					}
				}
			}
			if rf.nextIndex[server] < 0 {
				dlog.Printf(dlog.DInfo, "S%d %d nextIndex %d", rf.me, server, rf.nextIndex[server])
			}
		}
	}
	rf.mu.Lock()
	rf.matchIndex[server] = firstLogIndex - 1
	rf.mu.Unlock()
	// dlog.Printf(dlog.DLeader, "S%d --> S%d first log finish", rf.me, server)
	// dlog.Printf(dlog.DLog, "S%d --> S%d dummy goroutine panic dummyidx %d startDummyidx %d sendIdx %d startNext: %d", rf.me, server, dummyIndex, startDummyidx, rf.nextIndex[server], startNext)

}

func (rf *Raft) sendLeaderLogEntry(startTerm int, server int, task chan int) {
	defer func() {
		if r := recover(); r != nil {
			dlog.Printf(dlog.DError, "S%d --> S%d leader log send panic %v", rf.me, server, r)
			panic(r)
		}
	}()
	rf.mu.RLock()
	args := AppendEntryArgs{
		Term:    rf.currentTerm,
		LeadId:  rf.me,
		Entries: make([]LogEntry, 1),
	}
	rf.mu.RUnlock()
	reply := AppendEntryReply{}
	for !rf.killed() {
		start := <-task
		if start == 0 {
			// task close
			break
		}
		rf.mu.RLock()
		if rf.killed() || rf.identity != leaderServer {
			rf.mu.RUnlock()
			return
		}
		rf.mu.RUnlock()
		end := start
		if len(task) != 0 {
			end += min(oneTimeCopyNum-1, len(task))
			for i := start + 1; i <= end; i++ {
				<-task
			}
		}

		rf.mu.RLock()

		args.PrevLogIndex = start - 1
		args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
		args.Entries = rf.logs[start : end+1]
		rf.mu.RUnlock()
		for !rf.killed() {
			rf.mu.RLock()
			if rf.identity != leaderServer || startTerm != int(rf.currentTerm) {
				rf.mu.RUnlock()
				return
			}
			args.Term = rf.currentTerm
			args.LeaderCommit = int(rf.commitIndex)
			rf.mu.RUnlock()
			// dlog.Printf(dlog.DLog, "S%d --> S%d ing ing", rf.me, server)
			ok := rf.sendAppendEntry(server, &args, &reply)
			if ok {
				break
			}
		}
		if rf.killed() {
			return
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
			rf.persist()
			rf.electionTicker.Reset(getElectionTime())
			go rf.ticker()
			rf.mu.Unlock()
			return
		}
		if !reply.Success {
			dlog.Printf(dlog.DInfo, "S%d refuse to append %v", server, args.Entries)
		} else {
			rf.matchIndex[server] = end // there is no need to update nextIndex
			dlog.Printf(dlog.DLog, "S%d --> S%d matchidx %d", rf.me, server, rf.matchIndex[server])
		}
		// dlog.Printf(dlog.DInfo, "S%d --> S%d update matchIndex %d", rf.me, server, index)
		rf.mu.Unlock()
	}
}

func (rf *Raft) copyLogs(startApplied int) {
	// startApplied is the firstLog index
	rf.mu.RLock()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = startApplied - 1 // need to finish the previous log first
		rf.matchIndex[i] = -1              // at the beginning, it is set to 1
		// it will send real log to the server which has not finished first log copy
	}
	fistTerm := int(rf.currentTerm)
	rf.mu.RUnlock()
	taskBroadcast := make([]chan int, len(rf.peers))
	defer func() {
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			for range taskBroadcast[i] {
			}
			close(taskBroadcast[i])
		}
	}()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		taskBroadcast[i] = make(chan int, 1024) // Assuming it won't jam.
		go func(server int) {
			rf.sendPreviousLogEntry(fistTerm, int(startApplied), server)
			rf.sendLeaderLogEntry(fistTerm, server, taskBroadcast[server])
		}(i)
	}

	dlog.Printf(dlog.DLog, "S%d start to accept request", rf.me)
	go rf.detectCommit(startApplied)
	lastDistribute := int32(startApplied - 1)
	for !rf.killed() {
		rf.mu.RLock()
		if rf.identity != leaderServer {
			rf.mu.RUnlock()
			dlog.Printf(dlog.DLeader, "S%d know not leader", rf.me)
			break
		}
		if lastDistribute >= rf.lastApplied {
			rf.mu.RUnlock()
			// dlog.Printf(dlog.DLog, "S%d has no task sleep", rf.me)
			time.Sleep(pollPause * 5 * time.Millisecond)
			continue
		}
		for lastDistribute += 1; lastDistribute <= rf.lastApplied; lastDistribute++ {
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
		}
		lastDistribute--

		rf.mu.RUnlock()
	}
}

func (rf *Raft) detectCommit(firstLogIndex int) {
	matchIndex := make([]int, len(rf.peers))
	for !rf.killed() {
		rf.mu.Lock()
		if rf.identity != leaderServer {
			rf.mu.Unlock()
			return
		}
		copy(matchIndex, rf.matchIndex)
		matchIndex[rf.me] = math.MaxInt
		sort.Ints(matchIndex)
		minCommit := matchIndex[len(rf.peers)/2]
		if minCommit > int(rf.commitIndex) && minCommit >= firstLogIndex && !rf.killed() {
			for i := rf.commitIndex + 1; i <= int32(minCommit); i++ {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[i].Command,
					CommandIndex: int(i),
				}
				dlog.Printf(dlog.DCommit, "S%d commit %d", rf.me, i)
			}
			rf.commitIndex = int32(minCommit)
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
			time.Sleep(pollPause * time.Millisecond)
		}
	}
}
