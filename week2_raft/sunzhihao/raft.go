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
	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	"math/rand"
	"sync"
	//	"bytes"
	"sync/atomic"
	"time"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	FOLLOWER     = 0
	CANDIDATE    = 1
	LEADER       = 2
	TO_FOLLOWER  = 0
	TO_CANDIDATE = 1
	TO_LEADER    = 2

	ELECTION_TIMEOUT_MAX = 100
	ELECTION_TIMEOUT_MIN = 50

	HEARTBEAT_TIMEOUT = 27
	APPLIED_TIMEOUT   = 28
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm int
	votedFor    int
	getVoteNum  int
	log         []Entry

	commitIndex int
	lastApplied int

	//leaderId int // reject kvserver and tell it who is the leader

	state int
	//election_timer *time.Timer
	//heartbeat_timer *time.Timer // also the append entries timer ,for 2A is heartbeat
	lastResetElectionTime time.Time

	/*
		TODO: THIS is a instruction for my Index and len
		TODO: init log[] with a entry{term:0,commmand:null} (shouldn't be applied or persist in snapshot)
		TODO: so "len(rf.log) - 1" is the index, and index is same with the arrayindex
		TODO: rf.lastSSpointIndex + arrayindex = e.GlobalIndex || e = rf.log[rf.GlobalIndex-rf.lastSSPointIndex]
		TODO: nextIndex is "NExt entry to send to that sever" positive "you have all leader's entries"
		TODO: matchIndex is "highest known to be replicated on server" pessmistive "you have nothing"
	*/
	nextIndex  []int
	matchIndex []int

	applyCh chan ApplyMsg
	// SnapShot Point use
	lastSSPointIndex int
	lastSSPointTerm  int
}

type Entry struct {
	Term    int
	Command interface{}
}

//func (rf *Raft) resetElectionTimeout(){
//	randtime := getRand(int64(rf.me))
//
//	if !rf.election_timer.Stop(){
//		DPrintf("%d time stop result fault!!!!!",rf.me)
//		<-rf.election_timer.C
//	}
//	rf.election_timer.Reset(time.Duration(randtime)*time.Millisecond)
//	DPrintf("%d reset th elecition time of %d",rf.me,randtime)
//}
//
//func (rf *Raft) stopElectionTimer(){
//	if !rf.election_timer.Stop(){
//		<-rf.election_timer.C
//	}
//}
//
//func (rf *Raft) resetHearbeatTimeout(){
//	//rf.heartbeat_timer.Stop()
//	if !rf.heartbeat_timer.Stop(){
//		<- rf.heartbeat_timer.C
//	}
//	rf.heartbeat_timer.Reset(HEARTBEAT_TIMEOUT*time.Millisecond)
//}
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
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

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() == true {
		return -1, -1, false
	}
	if rf.state != LEADER {
		return -1, -1, false
	} else {
		index := rf.getLastIndex() + 1
		term := rf.currentTerm
		rf.log = append(rf.log, Entry{Term: term, Command: command})
		DPrintf("[StartCommand] Leader %d get command %v,index %d", rf.me, command, index)
		rf.persist()
		return index, term, true
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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

// Append Entries RPC structure
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term             int
	Success          bool
	ConflictingIndex int // optimizer func for find the nextIndex
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[GetHeartBeat]Sever %d, from Leader %d(term %d), mylastindex %d, leader.preindex %d", rf.me, args.LeaderId, args.Term, rf.getLastIndex(), args.PrevLogIndex)

	// rule 1
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictingIndex = -1
		return
	}

	rf.currentTerm = args.Term
	// rf.leaderId = args.LeaderId
	reply.Term = rf.currentTerm
	reply.Success = true
	reply.ConflictingIndex = -1

	if rf.state != FOLLOWER {
		rf.changeState(TO_FOLLOWER, true)
	} else {
		rf.lastResetElectionTime = time.Now()
		rf.persist()
	}

	// confilict
	// for my snapshot until 15,and len 3, you give me 10 - 20,I give you 16
	if rf.lastSSPointIndex > args.PrevLogIndex {
		reply.Success = false
		reply.ConflictingIndex = rf.getLastIndex() + 1
		return
	}

	if rf.getLastIndex() < args.PrevLogIndex {
		reply.Success = false
		reply.ConflictingIndex = rf.getLastIndex()
		DPrintf("[AppendEntries ERROR1]Sever %d ,prevLogIndex %d,Term %d, rf.getLastIndex %d, rf.getLastTerm %d, Confilicing %d", rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.getLastIndex(), rf.getLastTerm(), reply.ConflictingIndex)
		return
	} else {
		if rf.getLogTermWithIndex(args.PrevLogIndex) != args.PrevLogTerm {
			reply.Success = false
			tempTerm := rf.getLogTermWithIndex(args.PrevLogIndex)
			for index := args.PrevLogIndex; index >= rf.lastSSPointIndex; index-- {
				if rf.getLogTermWithIndex(index) != tempTerm {
					reply.ConflictingIndex = index + 1
					DPrintf("[AppendEntries ERROR2]Sever %d ,prevLogIndex %d,Term %d, rf.getLastIndex %d, rf.getLastTerm %d, Confilicing %d", rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.getLastIndex(), rf.getLastTerm(), reply.ConflictingIndex)
					break
				}
			}
			return
		}
	}

	//rule 3 & rule 4
	rf.log = append(rf.log[:args.PrevLogIndex+1-rf.lastSSPointIndex], args.Entries...)
	rf.persist()
	//if len(args.Entries) != 0{
	//	rf.printLogsForDebug()
	//}

	//rule 5
	if args.LeaderCommit > rf.commitIndex {
		rf.updateCommitIndex(FOLLOWER, args.LeaderCommit)
	}
	DPrintf("[FinishHeartBeat]Server %d, from leader %d(term %d), me.lastIndex %d", rf.me, args.LeaderId, args.Term, rf.getLastIndex())
	return
}

func (rf *Raft) leaderAppendEntries() {
	// send to every server to replicate logs to them
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		// parallel replicate logs to sever

		go func(server int) {
			rf.mu.Lock()
			if rf.state != LEADER {
				rf.mu.Unlock()
				return
			}
			prevLogIndextemp := rf.nextIndex[server] - 1
			// DPrintf("[IfNeedSendSnapShot] leader %d ,lastSSPIndex %d, server %d ,prevIndex %d",rf.me,rf.lastSSPointIndex,server,prevLogIndextemp)
			if prevLogIndextemp < rf.lastSSPointIndex {
				go rf.leaderSendSnapShot(server)
				rf.mu.Unlock()
				return
			}

			aeArgs := AppendEntriesArgs{}

			if rf.getLastIndex() >= rf.nextIndex[server] {
				DPrintf("[LeaderAppendEntries]Leader %d (term %d) to server %d, index %d --- %d", rf.me, rf.currentTerm, server, rf.nextIndex[server], rf.getLastIndex())
				//rf.printLogsForDebug()
				entriesNeeded := make([]Entry, 0)
				entriesNeeded = append(entriesNeeded, rf.log[rf.nextIndex[server]-rf.lastSSPointIndex:]...)
				prevLogIndex, prevLogTerm := rf.getPrevLogInfo(server)
				aeArgs = AppendEntriesArgs{
					rf.currentTerm,
					rf.me,
					prevLogIndex,
					prevLogTerm,
					entriesNeeded,
					rf.commitIndex,
				}
			} else {
				prevLogIndex, prevLogTerm := rf.getPrevLogInfo(server)
				aeArgs = AppendEntriesArgs{
					rf.currentTerm,
					rf.me,
					prevLogIndex,
					prevLogTerm,
					[]Entry{},
					rf.commitIndex,
				}
				DPrintf("[LeaderSendHeartBeat]Leader %d (term %d) to server %d,nextIndex %d, matchIndex %d, lastIndex %d", rf.me, rf.currentTerm, server, rf.nextIndex[server], rf.matchIndex[server], rf.getLastIndex())
			}
			aeReply := AppendEntriesReply{}
			rf.mu.Unlock()

			re := rf.sendAppendEntries(server, &aeArgs, &aeReply)

			//if re == false{
			//	rf.mu.Lock()
			//	DPrintf("[HeartBeat ERROR]Leader %d (term %d) get no reply from server %d",rf.me,rf.currentTerm,server)
			//	rf.mu.Unlock()
			//}

			if re == true {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.state != LEADER {
					return
				}

				if aeReply.Term > rf.currentTerm {
					rf.currentTerm = aeReply.Term
					rf.changeState(TO_FOLLOWER, true)
					return
				}

				DPrintf("[HeartBeatGetReturn] Leader %d (term %d) ,from Server %d, prevLogIndex %d", rf.me, rf.currentTerm, server, aeArgs.PrevLogIndex)

				if aeReply.Success {
					DPrintf("[HeartBeat SUCCESS] Leader %d (term %d) ,from Server %d, prevLogIndex %d", rf.me, rf.currentTerm, server, aeArgs.PrevLogIndex)
					rf.matchIndex[server] = aeArgs.PrevLogIndex + len(aeArgs.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1
					rf.updateCommitIndex(LEADER, 0)
				}

				if !aeReply.Success {
					if aeReply.ConflictingIndex != -1 {
						DPrintf("[HeartBeat CONFLICT] Leader %d (term %d) ,from Server %d, prevLogIndex %d, Confilicting %d", rf.me, rf.currentTerm, server, aeArgs.PrevLogIndex, aeReply.ConflictingIndex)
						rf.nextIndex[server] = aeReply.ConflictingIndex
					}
				}
			}

		}(index)

	}
}

func (rf *Raft) leaderAppendEntriesTicker() {
	for rf.killed() == false {
		time.Sleep(HEARTBEAT_TIMEOUT * time.Millisecond)
		rf.mu.Lock()
		if rf.state == LEADER {
			rf.mu.Unlock()
			rf.leaderAppendEntries()
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) committedToAppliedTicker() {
	// put the committed entry to apply on the state machine
	for rf.killed() == false {
		time.Sleep(APPLIED_TIMEOUT * time.Millisecond)
		rf.mu.Lock()

		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			continue
		}

		Messages := make([]ApplyMsg, 0)
		// log.Printf("[!!!!!!--------!!!!!!!!-------]Restart, LastSSP: %d, LastApplied :%d, commitIndex %d",rf.lastSSPointIndex,rf.lastApplied,rf.commitIndex)
		//log.Printf("[ApplyEntry] LastApplied %d, commitIndex %d, lastSSPindex %d, len %d, lastIndex %d",rf.lastApplied,rf.commitIndex,rf.lastSSPointIndex, len(rf.log),rf.getLastIndex())
		for rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.getLastIndex() {
			//for rf.lastApplied < rf.commitIndex {
			rf.lastApplied += 1
			//DPrintf("[ApplyEntry---] %d apply entry index %d, command %v, term %d, lastSSPindex %d",rf.me,rf.lastApplied,rf.getLogWithIndex(rf.lastApplied).Command,rf.getLogWithIndex(rf.lastApplied).Term,rf.lastSSPointIndex)
			Messages = append(Messages, ApplyMsg{
				CommandValid:  true,
				SnapshotValid: false,
				CommandIndex:  rf.lastApplied,
				Command:       rf.getLogWithIndex(rf.lastApplied).Command,
			})
		}
		rf.mu.Unlock()

		for _, messages := range Messages {
			rf.applyCh <- messages
		}
	}

}

func (rf *Raft) updateCommitIndex(role int, leaderCommit int) {

	if role != LEADER {
		if leaderCommit > rf.commitIndex {
			lastNewIndex := rf.getLastIndex()
			if leaderCommit >= lastNewIndex {
				rf.commitIndex = lastNewIndex
			} else {
				rf.commitIndex = leaderCommit
			}
		}
		DPrintf("[CommitIndex] Fllower %d commitIndex %d", rf.me, rf.commitIndex)
		return
	}

	if role == LEADER {
		rf.commitIndex = rf.lastSSPointIndex
		//for index := rf.commitIndex+1;index < len(rf.log);index++ {
		//for index := rf.getLastIndex();index>=rf.commitIndex+1;index--{
		for index := rf.getLastIndex(); index >= rf.lastSSPointIndex+1; index-- {
			sum := 0
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					sum += 1
					continue
				}
				if rf.matchIndex[i] >= index {
					sum += 1
				}
			}

			//log.Printf("lastSSP:%d, index: %d, commitIndex: %d, lastIndex: %d",rf.lastSSPointIndex, index, rf.commitIndex, rf.getLastIndex())
			if sum >= len(rf.peers)/2+1 && rf.getLogTermWithIndex(index) == rf.currentTerm {
				rf.commitIndex = index
				break
			}

		}
		DPrintf("[CommitIndex] Leader %d(term%d) commitIndex %d", rf.me, rf.currentTerm, rf.commitIndex)
		return
	}

}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//rule 1 ------------
	if args.Term < rf.currentTerm {
		DPrintf("[ElectionReject++++++]Server %d reject %d, MYterm %d, candidate term %d", rf.me, args.CandidateId, rf.currentTerm, args.Term)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	reply.Term = rf.currentTerm

	if args.Term > rf.currentTerm {
		DPrintf("[ElectionToFollower++++++]Server %d(term %d) into follower,candidate %d(term %d) ", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		rf.currentTerm = args.Term
		rf.changeState(TO_FOLLOWER, false)
		rf.persist()
	}

	//rule 2 ------------
	if rf.UpToDate(args.LastLogIndex, args.LastLogTerm) == false {
		DPrintf("[ElectionReject+++++++]Server %d reject %d, UpToDate", rf.me, args.CandidateId)
		//rf.printLogsForDebug()
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// arg.Term == rf.currentTerm
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId && args.Term == reply.Term {
		DPrintf("[ElectionReject+++++++]Server %d reject %d, Have voter for %d", rf.me, args.CandidateId, rf.votedFor)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	} else {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.lastResetElectionTime = time.Now()
		rf.persist()
		DPrintf("[ElectionSUCCESS+++++++]Server %d voted for %d!", rf.me, args.CandidateId)
		return
	}

	//if args.Term == rf.currentTerm {
	//	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
	//		reply.VoteGranted = false
	//		reply.Term = rf.currentTerm
	//		return
	//	}else {
	//		// TODO : have vote for you ? what should I do? vote you again???
	//		//DPrintf("%d vote for %d!",rf.me,args.CandidateId)
	//		rf.votedFor = args.CandidateId
	//		reply.VoteGranted = true
	//		reply.Term = rf.currentTerm
	//		rf.lastResetElectionTime = time.Now()
	//		rf.persist()
	//		return
	//	}
	//
	//}

	return

}

func (rf *Raft) candidateJoinElection() {
	DPrintf("[JoinElection+++++++] Sever %d, term %d", rf.me, rf.currentTerm)
	for index := range rf.peers {
		if index == rf.me {
			continue
		}

		go func(server int) {
			rf.mu.Lock()
			rvArgs := RequestVoteArgs{
				rf.currentTerm,
				rf.me,
				rf.getLastIndex(),
				rf.getLastTerm(),
			}
			rvReply := RequestVoteReply{}
			rf.mu.Unlock()
			// waiting code should free lock first.
			re := rf.sendRequestVote(server, &rvArgs, &rvReply)
			if re == true {
				rf.mu.Lock()
				if rf.state != CANDIDATE || rvArgs.Term < rf.currentTerm {
					rf.mu.Unlock()
					return
				}

				if rvReply.VoteGranted == true && rf.currentTerm == rvArgs.Term {
					rf.getVoteNum += 1
					if rf.getVoteNum >= len(rf.peers)/2+1 {
						DPrintf("[LeaderSuccess+++++] %d got votenum: %d, needed >= %d, become leader for term %d", rf.me, rf.getVoteNum, len(rf.peers)/2+1, rf.currentTerm)
						rf.changeState(TO_LEADER, true)
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
					return
				}

				if rvReply.Term > rvArgs.Term {
					if rf.currentTerm < rvReply.Term {
						rf.currentTerm = rvReply.Term
					}
					rf.changeState(TO_FOLLOWER, false)
					rf.mu.Unlock()
					return
				}

				rf.mu.Unlock()
				return
			}

		}(index)

	}

}

// The ticker go routine starts a new election if this peer hasn't received
// use candidateJoinElection()

func (rf *Raft) candidateElectionTicker() {
	for rf.killed() == false {
		nowTime := time.Now()
		timet := getRand(int64(rf.me))
		time.Sleep(time.Duration(timet) * time.Millisecond)
		rf.mu.Lock()
		if rf.lastResetElectionTime.Before(nowTime) && rf.state != LEADER {
			// DPrintf("[ElectionTrickerRand++++++]Server %d, term %d, randTime %d",rf.me,rf.currentTerm,timet)
			rf.changeState(TO_CANDIDATE, true)
		}
		rf.mu.Unlock()

	}
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) persistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	// e.Encode(rf.lastApplied)
	e.Encode(rf.lastSSPointIndex)
	e.Encode(rf.lastSSPointTerm)
	//e.Encode(rf.persister.ReadSnapshot())
	data := w.Bytes()
	return data
}

func (rf *Raft) persist() {
	data := rf.persistData()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var persist_currentTrem int
	var persist_voteFor int
	var persist_log []Entry
	//var persist_lastApplied int
	var persist_lastSSPointIndex int
	var persist_lastSSPointTerm int
	//var persist_snapshot []byte

	if d.Decode(&persist_currentTrem) != nil ||
		d.Decode(&persist_voteFor) != nil ||
		d.Decode(&persist_log) != nil ||
		//d.Decode(&persist_lastApplied) != nil ||
		d.Decode(&persist_lastSSPointIndex) != nil ||
		d.Decode(&persist_lastSSPointTerm) != nil {
		//d.Decode(&persist_snapshot) != nil{
		DPrintf("%d read persister got a problem!!!!!!!!!!", rf.me)
	} else {
		rf.currentTerm = persist_currentTrem
		rf.votedFor = persist_voteFor
		rf.log = persist_log
		// rf.lastApplied = persist_lastApplied
		rf.lastSSPointIndex = persist_lastSSPointIndex
		rf.lastSSPointTerm = persist_lastSSPointTerm
		// rf.persister.SaveStateAndSnapshot(rf.persistData(),persist_snapshot)
	}
}

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Data             []byte
	//Done bool
}

type InstallSnapshotReply struct {
	Term int
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.lastSSPointIndex >= index || index > rf.commitIndex {
		return
	}
	// snapshot the entrier form 1:index(global)
	tempLog := make([]Entry, 0)
	tempLog = append(tempLog, Entry{})

	for i := index + 1; i <= rf.getLastIndex(); i++ {
		tempLog = append(tempLog, rf.getLogWithIndex(i))
	}

	// TODO fix it in lab 4
	if index == rf.getLastIndex()+1 {
		rf.lastSSPointTerm = rf.getLastTerm()
	} else {
		rf.lastSSPointTerm = rf.getLogTermWithIndex(index)
	}

	rf.lastSSPointIndex = index

	rf.log = tempLog
	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}
	DPrintf("[SnapShot]Server %d sanpshot until index %d, term %d, loglen %d", rf.me, index, rf.lastSSPointTerm, len(rf.log)-1)
	rf.persister.SaveStateAndSnapshot(rf.persistData(), snapshot)
}

// InstallSnapShot RPC Handler
func (rf *Raft) InstallSnapShot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	// DPrintf("[lock] sever %d get the lock",rf.me)
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	rf.currentTerm = args.Term
	reply.Term = args.Term
	if rf.state != FOLLOWER {
		rf.changeState(TO_FOLLOWER, true)
	} else {
		rf.lastResetElectionTime = time.Now()
		rf.persist()
	}

	if rf.lastSSPointIndex >= args.LastIncludeIndex {
		DPrintf("[HaveSnapShot] sever %d , lastSSPindex %d, leader's lastIncludeIndex %d", rf.me, rf.lastSSPointIndex, args.LastIncludeIndex)
		rf.mu.Unlock()
		return
	}

	index := args.LastIncludeIndex
	tempLog := make([]Entry, 0)
	tempLog = append(tempLog, Entry{})

	for i := index + 1; i <= rf.getLastIndex(); i++ {
		tempLog = append(tempLog, rf.getLogWithIndex(i))
	}

	rf.lastSSPointTerm = args.LastIncludeTerm
	rf.lastSSPointIndex = args.LastIncludeIndex

	rf.log = tempLog
	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}
	rf.persister.SaveStateAndSnapshot(rf.persistData(), args.Data)
	//rf.persist()

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.lastSSPointTerm,
		SnapshotIndex: rf.lastSSPointIndex,
	}
	rf.mu.Unlock()

	rf.applyCh <- msg
	DPrintf("[FollowerInstallSnapShot]server %d installsnapshot from leader %d, index %d", rf.me, args.LeaderId, args.LastIncludeIndex)

}

func (rf *Raft) leaderSendSnapShot(server int) {
	rf.mu.Lock()
	DPrintf("[LeaderSendSnapShot]Leader %d (term %d) send snapshot to server %d, index %d", rf.me, rf.currentTerm, server, rf.lastSSPointIndex)
	ssArgs := InstallSnapshotArgs{
		rf.currentTerm,
		rf.me,
		rf.lastSSPointIndex,
		rf.lastSSPointTerm,
		rf.persister.ReadSnapshot(),
	}
	ssReply := InstallSnapshotReply{}
	rf.mu.Unlock()

	re := rf.sendSnapShot(server, &ssArgs, &ssReply)

	if !re {
		DPrintf("[InstallSnapShot ERROR] Leader %d don't recive from %d", rf.me, server)
	}
	if re == true {
		rf.mu.Lock()
		if rf.state != LEADER || rf.currentTerm != ssArgs.Term {
			rf.mu.Unlock()
			return
		}
		if ssReply.Term > rf.currentTerm {
			rf.changeState(FOLLOWER, true)
			rf.mu.Unlock()
			return
		}

		DPrintf("[InstallSnapShot SUCCESS] Leader %d from sever %d", rf.me, server)
		rf.matchIndex[server] = ssArgs.LastIncludeIndex
		rf.nextIndex[server] = ssArgs.LastIncludeIndex + 1

		rf.mu.Unlock()
		return
	}
}

// change the raft server state and do something init
func (rf *Raft) changeState(howtochange int, resetTime bool) {

	if howtochange == TO_FOLLOWER {
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.getVoteNum = 0
		rf.persist()
		if resetTime {
			rf.lastResetElectionTime = time.Now()
		}
	}

	if howtochange == TO_CANDIDATE {
		rf.state = CANDIDATE
		rf.votedFor = rf.me
		rf.getVoteNum = 1
		rf.currentTerm += 1
		rf.persist()
		rf.candidateJoinElection()
		rf.lastResetElectionTime = time.Now()
	}

	if howtochange == TO_LEADER {
		rf.state = LEADER
		rf.votedFor = -1
		rf.getVoteNum = 0
		rf.persist()

		rf.nextIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			//rf.nextIndex[i] = len(rf.log)
			rf.nextIndex[i] = rf.getLastIndex() + 1
		}

		rf.matchIndex = make([]int, len(rf.peers))
		//rf.matchIndex[rf.me] = len(rf.log) - 1
		rf.matchIndex[rf.me] = rf.getLastIndex()
		rf.lastResetElectionTime = time.Now()
		//rf.leaderAppendEntries()
	}
}

func getRand(server int64) int {
	rand.Seed(time.Now().Unix() + server)
	return rand.Intn(ELECTION_TIMEOUT_MAX-ELECTION_TIMEOUT_MIN) + ELECTION_TIMEOUT_MIN
}

func (rf *Raft) printLogsForDebug() {
	DPrintf("[PrintLog]Print server %d Logs, lastSSPindex %d", rf.me, rf.lastSSPointIndex)
	for index := 1; index < len(rf.log); index++ {
		DPrintf("[Logs...]Index %d, command %v, term %d", index+rf.lastSSPointIndex, rf.log[index].Command, rf.log[index].Term)
	}

}

//func (rf* Raft) UpToDate(index int, term int) bool {
//	lastEntry := rf.log[len(rf.log)-1]
//	return term > lastEntry.Term || (term == lastEntry.Term && index >= len(rf.log)-1)
//}

func (rf *Raft) UpToDate(index int, term int) bool {
	//lastEntry := rf.log[len(rf.log)-1]
	lastIndex := rf.getLastIndex()
	lastTerm := rf.getLastTerm()
	return term > lastTerm || (term == lastTerm && index >= lastIndex)
}

func (rf *Raft) getLogWithIndex(globalIndex int) Entry {

	return rf.log[globalIndex-rf.lastSSPointIndex]
}

func (rf *Raft) getLogTermWithIndex(globalIndex int) int {
	//log.Printf("[GetLogTermWithIndex] Sever %d,lastSSPindex %d ,len %d",rf.me,rf.lastSSPointIndex,len(rf.log))
	if globalIndex-rf.lastSSPointIndex == 0 {
		return rf.lastSSPointTerm
	}
	return rf.log[globalIndex-rf.lastSSPointIndex].Term
}

func (rf *Raft) getLastIndex() int {
	return len(rf.log) - 1 + rf.lastSSPointIndex
}

func (rf *Raft) getLastTerm() int {
	if len(rf.log)-1 == 0 {
		return rf.lastSSPointTerm
	} else {
		return rf.log[len(rf.log)-1].Term
	}
}

func (rf *Raft) getPrevLogInfo(server int) (int, int) {
	newEntryBeginIndex := rf.nextIndex[server] - 1
	// TODO fix it in lab4
	lastIndex := rf.getLastIndex()
	if newEntryBeginIndex == lastIndex+1 {
		newEntryBeginIndex = lastIndex
	}
	return newEntryBeginIndex, rf.getLogTermWithIndex(newEntryBeginIndex)
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendSnapShot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	return ok
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.mu.Lock()
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.getVoteNum = 0
	rf.votedFor = -1
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.lastSSPointIndex = 0
	rf.lastSSPointTerm = 0
	rf.log = []Entry{}
	rf.log = append(rf.log, Entry{})
	rf.applyCh = applyCh
	rf.mu.Unlock()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if rf.lastSSPointIndex > 0 {
		rf.lastApplied = rf.lastSSPointIndex
	}

	DPrintf("[Init&ReInit] Sever %d, term %d, lastSSPindex %d , term %d", rf.me, rf.currentTerm, rf.lastSSPointIndex, rf.lastSSPointTerm)
	// start ticker goroutine to start elections
	go rf.candidateElectionTicker()

	go rf.leaderAppendEntriesTicker()

	go rf.committedToAppliedTicker()

	return rf
}
