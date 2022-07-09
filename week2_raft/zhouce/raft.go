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
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

// import "bytes"
// import "../labgob"

type Identity int //表明身份

const (
	Leader Identity = iota
	Candidate
	Follower
)

const (
	randTime        = 300
	heartbeatPeriod = 110
)

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

type LogEntry struct { // 日志集
	Term    int
	Index   int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	applyCh         chan ApplyMsg
	applyCond       *sync.Cond //used to wake up applier goroutine after committing new entries
	electionTimeout int
	electionTimer   *time.Timer
	LeaderID        int        //领导者的id
	identity        Identity   //此时的身份
	CurrentTerm     int        //当前任期
	VotedFor        int        //当前任期投票投给谁
	Logs            []LogEntry //日志
	commitedIndex   int        //已提交的最高的日志索引
	lastApplied     int        //当前应用到状态机的索引
	nextIndex       []int      //发送到该服务器的下一个日志条目的索引
	matchIndex      []int      //已经复制到该条目的索引
	SnapshotIndex   int        //used for snapshot
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	gid int
}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) { //
	var term int
	var isleader bool

	rf.mu.Lock()
	term = rf.CurrentTerm
	if rf.iedentity == Leader {
		isleader = true
	} else {
		isleader = false
	}
	rf.mu.Unlock()

	return term, isleader
}

func max(x int, y int) int { //一个简单的比较函数
	if x >= y {
		return x
	} else {
		return y
	}
}

func (rf *Raft) getRelativeLogIndex(index int) int {
	return index - rf.SnapshotIndex
}

func (rf *Raft) getLastIndexOfLogs() int {
	return rf.SnapshotIndex + len(rf.Logs) - 1
}

func (rf *Raft) getLastLogTerm() int { //获得最新的logterm
	return rf.Logs[len(rf.Logs)-1].Term
}

func (rf *Raft) PersisterSize() int {
	return rf.persister.RaftStateSize()
}

//把raft作为字节数组保存

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Logs)
	e.Encode(rf.SnapshotIndex)
	return w.Bytes()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	rf.persister.SaveRaftState(rf.encodeState())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CurrentTerm int
	var VotedFor int
	var Logs []LogEntry
	var SnapshotIndex int
	if d.Decode(&CurrentTerm) != nil || d.Decode(&VotedFor) != nil || d.Decode(&Logs) != nil || d.Decode(&SnapshotIndex) != nil {
		DPrintf("[readPersist]: Decode error")
	} else {
		rf.CurrentTerm = CurrentTerm
		rf.VotedFor = VotedFor
		rf.Logs = Logs
		rf.SnapshotIndex = SnapshotIndex
		rf.commitedIndex = SnapshotIndex
		rf.lastApplied = SnapshotIndex
	}
	//DPrintf("[readPersist]: peer[%d] Term[%d] Identity[%s] read persistent state from Persister", rf.me, rf.CurrentTerm, rf.getIdentity())
}

func (rf *Raft) SnapShot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[Snapshot]: server[%d] index[%d]", rf.me, index)

	if index <= rf.SnapshotIndex || index > rf.lastApplied {
		return
	}

	newLog := []LogEntry{}
	newLog = append(newLog, rf.Logs[rf.getRelativeLogIndex(index):]...)
	rf.Logs = newLog
	rf.Logs[0].Command = nil

	rf.SnapshotIndex = index
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
}

//超时计时器到了要重置
func (rf *Raft) resetElectionTimer() {
	rand.Seed(time.Now().UnixNano())
	rf.electionTimeout = rand.Intn(300-150) + randTime
	rf.electionTimer.Reset(time.Millisecond * time.Duration(rf.electionTimeout))
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
//发起请求投票
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //candidate's term
	CandidateID  int
	LastLogIndex int //candidate's last log index
	LastLogTerm  int //candidate's last log term
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool //是否会投票给请求者
}

func (rf *Raft) isMoreUpToDate(args *RequestVoteArgs) bool {
	lastIndex := len(rf.Logs) - 1
	return rf.Logs[lastIndex].Term > args.LastLogTerm || (rf.Logs[lastIndex].Term == args.LastLogTerm && rf.getLastIndexOfLogs() > args.LastLogIndex)
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[RequestVote]: peer[%v] identity[%v] receive requestVote RPC: term[%v], candidateID[%v], CurrentTerm[%v], VotedFor[%v], timestamp[%v]",
		rf.me, rf.GetIdentity(), args.Term, args.CandidateID, rf.CurrentTerm, rf.VotedFor, time.Now().UnixNano())
	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false //默认投反对票
	//如果当前任期不如我大
	if rf.CurrentTerm > args.Term {
		DPrintf("[RequestVote rejection]: peer[%d]'s CurrentTerm is larger than peer[%d], timestamp[%v]", rf.me, args.CandidateID, time.Now().UnixNano())
		return
	}
	//比我大，转为Follower
	if rf.CurrentTerm < args.Term {
		rf.changeTo(Follower)
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.persist()
	}
	//每个任期，只能投票给一人
	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateID {
		//check whether current log is more up-to-date than candidate, if so, reject vote
		//检查当前的日志是不是比candidate的还新，如果海新，则拒绝
		if rf.isMoreUpToDate(args) {
			DPrintf("[RequestVote rejection]: peer[%d]'s log is more update than peer[%d], lastIndex[%d], lastTerm[%d], timestamp[%v]",
				rf.me, args.CandidateID, rf.getLastIndexOfLogs(), rf.getLastLogTerm(), time.Now().UnixNano())
			return
		}
		//否则的话更新
		rf.CurrentTerm = args.Term
		rf.VotedFor = args.CandidateID
		rf.changeTo(Follower)
		DPrintf("[RequestVote]: peer[%d] reset electionTimer[%d] because of vote for Candidate peer[%d], timestamp[%v]",
			rf.me, rf.electionTimeout, args.CandidateID, time.Now().UnixNano())

		rf.persist()

		reply.Term = rf.CurrentTerm
		reply.VoteGranted = true
	}
}

//向follow节点发起同步操作日志模块
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int //index of Log entry immediately preceding new ones
	PrevLogTerm  int //term of PrevLogIndex entry
	Entries      []LogEntry
	LeaderCommit int //leader's commitIndex
}

//follow节点的回复操作
type AppendEntriesReply struct {
	CurrentTerm   int
	ConflictTerm  int //the term of conflict entry
	ConflictIndex int //the first index it stores for the term of the conflict entry
	Success       bool
}

func (rf *Raft) checkLogConsistency(args *AppendEntriesArgs) bool {
	return rf.getLastIndexOfLogs() >= args.PrevLogIndex && rf.Logs[rf.getRelativeLogIndex(args.PrevLogIndex)].Term == args.PrevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.CurrentTerm = rf.CurrentTerm
	reply.Success = false
	DPrintf("[AppendEntries]: term[%d] peer[%d] receive a RPC from leader peer[%d], prevLogIndex[%d], LeaderCommit:[%v], timestamp[%v], log[%v]",
		rf.CurrentTerm, rf.me, args.LeaderID, args.PrevLogIndex, args.LeaderCommit, time.Now().UnixNano(), rf.Logs)

	//check term and log match
	if rf.CurrentTerm > args.Term {
		DPrintf("[AppendEntries]: peer[%d] reject peer[%d]'s RPC, because term[%d] is larger than args.Term[%d], timestamp[%v]",
			rf.me, args.LeaderID, rf.CurrentTerm, args.Term, time.Now().UnixNano())
		return
	}

	termIsEqual := rf.CurrentTerm == args.Term

	if rf.CurrentTerm < args.Term {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.changeTo(Follower)
		rf.persist()
		//DPrintf("[AppendEntries]: peer[%d] resetElectionTimer[%d] because of receving RPC from Leader peer[%d], timestamp[%v] 1", rf.me, rf.electionTimeout, args.LeaderID, time.Now().UnixNano())
	}
	//DPrintf("term[%d], peer[%d] receive an AppendEntries RPC from Leader peer[%d]", args.Term, rf.me, args.LeaderID)

	//if follower's log is inconsistent with leader, return the conflictTerm and conflictFirstIndex to leader
	//so that leader can decrease nextIndex and retry
	if args.PrevLogIndex >= rf.SnapshotIndex {
		if !rf.checkLogConsistency(args) {
			DPrintf("[AppendEntries]: peer[%d] reject peer[%d]'s RPC because of log inConsistency!, timestamp[%v]", rf.me, args.LeaderID, time.Now().UnixNano())
			//if a follower does not have prevLogIndex in its log, it should return with
			//conflictFirstIndex = len(log), and conflictTerm = None
			if rf.getLastIndexOfLogs() < args.PrevLogIndex {
				reply.ConflictTerm = 0 // conflictTerm = 0, represent illegal
				reply.ConflictIndex = rf.getLastIndexOfLogs() + 1
				DPrintf("[AppendEntries]: peer[%d] Term[%d] identity[%s] reply's conflictFirstIndex[%d] and conflictTerm is None, timestamp[%v]",
					rf.me, rf.CurrentTerm, rf.GetIdentity(), reply.ConflictIndex, time.Now().UnixNano())
			} else {
				reply.ConflictTerm = rf.Logs[rf.getRelativeLogIndex(args.PrevLogIndex)].Term
				index := args.PrevLogIndex
				for index >= rf.SnapshotIndex && rf.Logs[rf.getRelativeLogIndex(index)].Term == reply.ConflictTerm {
					index--
				}
				reply.ConflictIndex = index + 1
				DPrintf("[AppendEntries]: peer[%d] Term[%d] identity[%s] reply's conflictFirstIndex[%d] and conflictTerm [%d]",
					rf.me, rf.CurrentTerm, rf.GetIdentity(), reply.ConflictIndex, reply.ConflictTerm)
			}
			return
		}
		if termIsEqual {
			rf.changeTo(Follower)
		}
		//find the latest non-match log, delete all log follow it and append new entry to it's log
		logIndex := args.PrevLogIndex + 1
		for entriesIndex, entry := range args.Entries {
			if rf.getRelativeLogIndex(logIndex) >= len(rf.Logs) || rf.Logs[rf.getRelativeLogIndex(logIndex)].Term != entry.Term {
				rf.Logs = append(rf.Logs[:rf.getRelativeLogIndex(logIndex)], args.Entries[entriesIndex:]...)
				break
			}
			logIndex += 1
		}
		rf.persist()
	}

	oldCommit := rf.commitedIndex
	indexOfLastEntry := rf.getLastIndexOfLogs()
	if args.LeaderCommit > rf.commitedIndex {
		rf.commitedIndex = args.LeaderCommit
		if rf.commitedIndex > indexOfLastEntry {
			rf.commitedIndex = indexOfLastEntry
		}
	}
	if rf.commitedIndex > oldCommit {
		rf.applyCond.Broadcast()
	}
	rf.LeaderID = args.LeaderID
	reply.CurrentTerm = rf.CurrentTerm
	reply.Success = true
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte //raw bytes of snapshot
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) cutLog(lastIncludedIndex int, lastIncludedTerm int) {
	if lastIncludedIndex <= rf.getLastIndexOfLogs() {
		rf.Logs = rf.Logs[rf.getRelativeLogIndex(lastIncludedIndex):]
		rf.Logs[0].Command = nil
	} else {
		rf.Logs = []LogEntry{{Term: lastIncludedTerm, Index: lastIncludedIndex, Command: nil}}
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[InstallSnapshot] 1: server[%d]'s SnapshotIndex[%d], leader[%d]'s SnapshotIndex[%d], commitedIndex[%d], old Log[%v]",
		rf.me, rf.SnapshotIndex, args.LeaderId, args.LastIncludedIndex, rf.commitedIndex, rf.Logs)
	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		return
	}

	if args.Term > rf.CurrentTerm {
		reply.Term = args.Term
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.persist()
	}
	rf.changeTo(Follower)
	rf.resetElectionTimer()

	//outdated snapshot
	if args.LastIncludedIndex < rf.SnapshotIndex || args.LastIncludedIndex <= rf.lastApplied {
		return
	}

	//cut Log because of Leader's InstallSnapshot
	rf.cutLog(args.LastIncludedIndex, args.LastIncludedTerm)

	rf.SnapshotIndex = args.LastIncludedIndex
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), args.Data)

	//force follower catch up with leader
	rf.commitedIndex = args.LastIncludedIndex
	rf.lastApplied = max(rf.lastApplied, args.LastIncludedIndex)

	DPrintf("[InstallSnapshot]  2: server[%d]'s SnapshotIndex[%d], leader[%d]'s SnapshotIndex[%d], commitedIndex[%d], new Log[%v]",
		rf.me, rf.SnapshotIndex, args.LeaderId, args.LastIncludedIndex, rf.commitedIndex, rf.Logs)

	go func() {
		rf.applyCh <- ApplyMsg{
			CommandValid: false,
			Command:      args.Data,
		}
	}()
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) electionTicker() {
	for !rf.killed() {
		select {
		case <-rf.electionTimer.C:
			rf.startElection()
		default:
			time.Sleep(time.Millisecond * 10)
		}
	}
}

func (rf *Raft) heartBeatTicker() {
	for !rf.killed() {
		rf.updateLeaderCommit()
		rf.broadcastAppendEntries()
		time.Sleep(time.Millisecond * heartbeatPeriod)
	}
}

//改变身份函数
func (rf *Raft) changeTo(newIdentity Identity) {
	rf.iedentity = newIdentity
	if newIdentity != Leader {
		rf.resetElectionTimer()
	}
}

//获取身份函数
func (rf *Raft) GetIdentity() string {
	var identity string
	if rf.iedentity == Leader {
		identity = "Leader"
	} else if rf.iedentity == Candidate {
		identity = "Candidate"
	} else if rf.iedentity == Follower {
		identity = "Follower"
	} else {
		identity = "Invalid"
	}
	return identity
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.iedentity == Leader { //
		rf.mu.Unlock()
		return
	}
	rf.changeTo(Candidate)
	rf.VotedFor = rf.me
	rf.resetElectionTimer()
	rf.LeaderID = -1
	rf.CurrentTerm += 1
	rf.persist()
	DPrintf("[StartElection] term[%d], peer[%d] start to elect as a candidate, timestamp[%v], log[%v]",
		rf.CurrentTerm, rf.me, time.Now().UnixNano(), rf.Logs)
	rf.mu.Unlock()

	var voteNum int = 1 // first, vote for self

	for serverID := 0; serverID < len(rf.peers); serverID++ {
		if serverID == rf.me {
			continue
		}
		go func(serverID int) {
			rf.mu.Lock()
			if rf.killed() || rf.iedentity != Candidate {
				rf.mu.Unlock()
				return
			}
			args := RequestVoteArgs{
				Term:         rf.CurrentTerm,
				CandidateID:  rf.me,
				LastLogIndex: rf.getLastIndexOfLogs(),
				LastLogTerm:  rf.Logs[len(rf.Logs)-1].Term,
			}
			rf.mu.Unlock()
			reply := RequestVoteReply{}
			if ok := rf.sendRequestVote(serverID, &args, &reply); ok {
				rf.mu.Lock()
				if rf.CurrentTerm != args.Term || rf.iedentity != Candidate {
					rf.mu.Unlock()
					return
				}
				if rf.iedentity == Candidate {
					if reply.VoteGranted {
						DPrintf("term[%d], peer[%d] vote for peer[%d], timestamp[%v]", rf.CurrentTerm, serverID, rf.me, time.Now().UnixNano())
						voteNum += 1
						if voteNum > len(rf.peers)/2 {
							DPrintf("term[%d], peer[%d] become Leader, timestamp[%v]", rf.CurrentTerm, rf.me, time.Now().UnixNano())
							rf.changeTo(Leader)
							rf.LeaderID = rf.me
							rf.persist()
							//init nextIndex for each peer
							for i := 0; i < len(rf.peers); i++ {
								rf.matchIndex[i] = 0
								rf.nextIndex[i] = rf.getLastIndexOfLogs() + 1
							}
							//TODO : append an no-op log to commit previous term's log
							go rf.broadcastAppendEntries()
							DPrintf("Leader peer[%d] reset heartbeatTimer because of sending heartbeat after win election, timestamp[%v]",
								rf.me, time.Now().UnixNano())
						}
					} else if reply.Term > rf.CurrentTerm {
						DPrintf("peer[%d] reject to vote for peer[%d], because term[%d] is smaller than term[%d], timestamp[%v]",
							serverID, rf.me, rf.CurrentTerm, reply.Term, time.Now().UnixNano())
						rf.changeTo(Follower)
						rf.CurrentTerm = reply.Term
						rf.VotedFor = -1
						rf.persist()
						rf.resetElectionTimer()
					}
				}
				rf.mu.Unlock()
			} else {
				DPrintf("Candidate peer[%d] can't communicate with peer[%d], timestamp[%v]", rf.me, serverID, time.Now().UnixNano())
			}
		}(serverID)
	}
}

// If desired, the protocol can be optimized to reduce the number of rejected AppendEntries
// RPCs. For example,  when rejecting an AppendEntries request, the follower can include the
// term of the conflicting entry and the first index it stores for that term. With this
// information, the leader can decrement nextIndx to bypass all of the conflicting entries
// in that term; one AppendEntries RPC will be required for each term with conflicting entries,
// rather than one RPC per entry.
func (rf *Raft) getNextIndex(reply *AppendEntriesReply) (nextIndex int) {
	//reply's conflictTerm = 0, 说明peer的log长度小于nextIndex
	nextIndex = reply.ConflictIndex
	if reply.ConflictTerm != 0 {
		conflictTerm := reply.ConflictTerm
		index := rf.SnapshotIndex
		found := false
		for ; index < rf.getLastIndexOfLogs(); index++ {
			if rf.Logs[rf.getRelativeLogIndex(index)].Term != conflictTerm {
				continue
			}
			found = true
			for index < rf.getLastIndexOfLogs() && rf.Logs[rf.getRelativeLogIndex(index)].Term == conflictTerm {
				index++
			}
			nextIndex = index
			break
		}
		if !found {
			nextIndex = reply.ConflictIndex
		}
	}
	return nextIndex
}

func (rf *Raft) sendSnapshot(serverID int) {
	rf.mu.Lock()
	args := &InstallSnapshotArgs{
		Term:              rf.CurrentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.SnapshotIndex,
		LastIncludedTerm:  rf.Logs[0].Term,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()

	reply := &InstallSnapshotReply{}
	if ok := rf.sendInstallSnapshot(serverID, args, reply); ok {
		rf.mu.Lock()
		if rf.iedentity != Leader || rf.CurrentTerm != args.Term {
			rf.mu.Unlock()
			return
		}
		if reply.Term > rf.CurrentTerm {
			rf.CurrentTerm = reply.Term
			rf.changeTo(Follower)
			rf.VotedFor = -1
			rf.persist()
		} else {
			rf.matchIndex[serverID] = max(rf.matchIndex[serverID], args.LastIncludedIndex)
			rf.nextIndex[serverID] = rf.matchIndex[serverID] + 1
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendAppend(serverID int) {
	rf.mu.Lock()
	//过期操作
	if rf.iedentity != Leader {
		rf.mu.Unlock()
		return
	}
	if rf.nextIndex[serverID] <= rf.SnapshotIndex {
		rf.mu.Unlock()
		rf.sendSnapshot(serverID)
		return
	}
	prevLogIndex := rf.nextIndex[serverID] - 1
	if prevLogIndex < 0 {
		DPrintf("[BroadcastAppendEntries]: Leader peer[%d] term[%d], invalid prevLogIndex[%d]", rf.me, rf.CurrentTerm, prevLogIndex)
	}
	prevLogTerm := rf.Logs[rf.getRelativeLogIndex(prevLogIndex)].Term
	entries := make([]LogEntry, 0)
	if rf.nextIndex[serverID] <= rf.getLastIndexOfLogs() {
		entries = append(entries, rf.Logs[rf.getRelativeLogIndex(rf.nextIndex[serverID]):]...)
	}
	args := AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitedIndex,
	}
	rf.mu.Unlock()
	reply := AppendEntriesReply{}
	DPrintf("Leader peer [%d] send log [%v] to peer[%d]", rf.me, entries, serverID)
	if rf.sendAppendEntries(serverID, &args, &reply) {
		rf.handleAppendResponse(&reply, &args, serverID)
	} else {
		DPrintf("Leader peer [%d] can't communicate with peer[%d], return, timestamp[%v]", rf.me, serverID, time.Now().UnixNano())
		return
	}
}

func (rf *Raft) handleAppendResponse(reply *AppendEntriesReply, args *AppendEntriesArgs, serverID int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.CurrentTerm != args.Term || rf.iedentity != Leader {
		return
	}
	if reply.Success {
		if len(args.Entries) > 0 {
			rf.matchIndex[serverID] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[serverID] = rf.matchIndex[serverID] + 1
			go rf.updateLeaderCommit()
		}
	} else {
		if reply.CurrentTerm > rf.CurrentTerm {
			DPrintf("[broadcastAppendEntries]: peer[%d]'s term[%d] is larger than peer[%d]'s term[%d], change to follower, timestamp[%v]",
				serverID, reply.CurrentTerm, rf.me, rf.CurrentTerm, time.Now().UnixNano())
			rf.changeTo(Follower)
			rf.CurrentTerm = reply.CurrentTerm
			rf.VotedFor = -1
			rf.persist()
			rf.resetElectionTimer()
		} else {
			DPrintf("[broadcastAppendEntries]: peer[%d]'s log doesn't match with Leader peer[%d] log[%d], so decrease nextIndex and retry",
				serverID, rf.me, rf.nextIndex[serverID])
			nextIndex := rf.getNextIndex(reply)
			rf.nextIndex[serverID] = nextIndex
			if nextIndex <= rf.getLastIndexOfLogs() {
				//send missing logs to this follower immediately
				go rf.sendAppend(serverID)
			}
		}
	}
}

//对日志进行广播
func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	if rf.iedentity != Leader {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	for serverID := 0; serverID < len(rf.peers); serverID++ {
		if serverID == rf.me {
			continue
		}
		go func(serverID int) {

			rf.mu.Lock()
			nextIndex := rf.nextIndex[serverID]
			snapshotIndex := rf.SnapshotIndex
			rf.mu.Unlock()

			if nextIndex <= snapshotIndex {
				rf.sendSnapshot(serverID)
			} else {
				rf.sendAppend(serverID)
			}
		}(serverID)
	}
}

//If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
//and log[N].term == currentTerm: set commitIndex = N
func (rf *Raft) updateLeaderCommit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.iedentity != Leader || rf.commitedIndex == rf.getLastIndexOfLogs() {
		return
	}
	waitCommit := rf.commitedIndex
	for N := rf.getLastIndexOfLogs(); N > rf.commitedIndex && N > rf.SnapshotIndex; N-- {
		nReplica := 1
		for serverID := range rf.peers {
			if serverID == rf.me {
				continue
			}
			if rf.matchIndex[serverID] >= N {
				nReplica += 1
			}
		}
		if nReplica > len(rf.peers)/2 && rf.Logs[rf.getRelativeLogIndex(N)].Term == rf.CurrentTerm {
			waitCommit = N
			break
		}
	}
	if waitCommit > rf.commitedIndex {
		rf.commitedIndex = waitCommit
		rf.applyCond.Broadcast()
	}
}

func (rf *Raft) applier() {
	for !rf.killed() {
		var commitedIndex int
		var lastApplied int

		rf.applyCond.L.Lock()
		for rf.lastApplied >= rf.commitedIndex {
			rf.applyCond.Wait()
		}
		commitedIndex = rf.commitedIndex
		lastApplied = rf.lastApplied
		if lastApplied < rf.SnapshotIndex {
			rf.applyCond.L.Unlock()
			continue
		}
		applyMsgs := make([]ApplyMsg, 0, commitedIndex-lastApplied)
		for i := lastApplied + 1; i <= commitedIndex; i++ {
			if i < rf.SnapshotIndex {
				fmt.Printf("i: [%d], snapshotIndex: [%d]\n", i, rf.SnapshotIndex)
			}
			newApplyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.Logs[rf.getRelativeLogIndex(i)].Command,
				CommandIndex: rf.Logs[rf.getRelativeLogIndex(i)].Index,
			}
			applyMsgs = append(applyMsgs, newApplyMsg)
			rf.lastApplied = newApplyMsg.CommandIndex
		}
		rf.applyCond.L.Unlock()

		for _, newApplyMsg := range applyMsgs {
			rf.applyCh <- newApplyMsg
			DPrintf("[applier]: peer[%d] Term[%d] identity[%s] apply command [%v] of index [%d] to state machine, timestamp[%v]\n",
				rf.me, rf.CurrentTerm, rf.GetIdentity(), newApplyMsg.Command, newApplyMsg.CommandIndex, time.Now().UnixNano())
		}
	}
}

func (rf *Raft) insertLog(command interface{}) (int, int) {
	term := rf.CurrentTerm
	index := rf.getLastIndexOfLogs() + 1
	newEntry := LogEntry{
		Term:    term,
		Index:   index,
		Command: command,
	}
	rf.Logs = append(rf.Logs, newEntry)
	rf.persist()
	DPrintf("leader peer[%d] append log[%v] at index[%d] in term[%d], timestamp[%v]", rf.me, command, index, term, time.Now().UnixNano())
	return term, newEntry.Index
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
	if rf.iedentity != Leader {
		rf.mu.Unlock()
		return -1, -1, false
	}
	DPrintf("[Start], command[%v], timestamp[%v]", command, time.Now().UnixNano())
	term, index = rf.insertLog(command)
	go rf.broadcastAppendEntries()
	rf.mu.Unlock()

	return index, term, isLeader
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg, gid ...int) *Raft {
	rf := &Raft{
		peers:         peers,
		persister:     persister,
		me:            me,
		iedentity:     Follower,
		CurrentTerm:   0,
		VotedFor:      -1,
		LeaderID:      -1,
		applyCh:       applyCh,
		Logs:          make([]LogEntry, 1),
		nextIndex:     make([]int, len(peers)),
		matchIndex:    make([]int, len(peers)),
		commitedIndex: 0,
		lastApplied:   0,
		SnapshotIndex: 0,
	}

	// gid for test
	if len(gid) != 0 {
		rf.gid = gid[0]
	} else {
		rf.gid = -1
	}

	rand.Seed(time.Now().UnixNano())
	rf.electionTimeout = rand.Intn(300-150) + randTime
	rf.electionTimer = time.NewTimer(time.Duration(rf.electionTimeout) * time.Millisecond)

	// Your initialization code here (2A, 2B, 2C).
	rf.Logs[0] = LogEntry{0, 0, nil}
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()

	for serverID := 0; serverID < len(rf.peers); serverID++ {
		if serverID == rf.me {
			continue
		}
		rf.matchIndex[serverID] = 0
		rf.nextIndex[serverID] = rf.getLastIndexOfLogs() + 1
	}

	go rf.electionTicker()
	go rf.heartBeatTicker()
	go rf.applier()

	return rf
}
