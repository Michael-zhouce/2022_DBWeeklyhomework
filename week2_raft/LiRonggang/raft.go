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
	"fmt"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"bytes"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
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

type logEntry struct {
	command interface{}
	term    int
}

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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	//持久性状态
	currentTerm int        //服务器当前任期
	votedFor    int        //获得选票的候选人
	log         []logEntry //日志集合

	//易失性状态
	commitIndex int //已提交日志的最大索引
	lastApplied int //被应用日志的最大索引

	//leader状态
	nextIndex  []int //每个follower待接收日志索引
	matchIndex []int //每个follower已复制日志最大索引

	state          string      //服务器状态
	votes          int         //候选人票数
	heartBeatTimer *time.Timer //心跳周期
	electionTimer  *time.Timer //选举定时器

	applyCh chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func (rf *Raft) resetTimer() {
	//关闭选举计时器以及心跳计时器
	rf.electionTimer.Stop()
	rf.heartBeatTimer.Stop()
	//重置选举计时器以及心跳计时器
	rf.electionTimer.Reset(200 * time.Millisecond)
	rf.heartBeatTimer.Reset(100 * time.Millisecond)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	return term, isleader
}

func (rf *Raft) changeState(state string) {
	if state == "Follower" {
		rf.state = "Follower"
		rf.votedFor = -1
		rf.votes = 0
		rf.persist()
		rf.resetTimer()
	} else if state == "Candidate" {
		rf.state = "Candidate"
		rf.votedFor = rf.me
		rf.votes = 1
		rf.persist()
		rf.resetTimer()
	} else if state == "Leader" {
		rf.state = "Leader"
		rf.votedFor = -1
		rf.votes = 0
		rf.persist()

		for i, _ := range rf.peers {
			rf.nextIndex[i] = len(rf.log)
			rf.matchIndex[i] = 0
		}
		rf.resetTimer()
	} else {
		return
	}
}

func (rf *Raft) commitLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.commitIndex > len(rf.log)-1 {
		fmt.Printf("error : raft.go commitlogs()")
	}

	// 初始化是-1
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{CommandIndex: i + 1, Command: rf.log[i].command}
	}

	rf.lastApplied = rf.commitIndex
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	//w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	return
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var persistedCurrentTerm int
	var persistedVotedFor int
	var persistedLog []logEntry

	if d.Decode(&persistedCurrentTerm) != nil ||
		d.Decode(&persistedVotedFor) != nil ||
		d.Decode(&persistedLog) {
		fmt.Printf("error: rf.readPersist()\n")
	} else {
		rf.currentTerm = persistedCurrentTerm
		rf.votedFor = persistedVotedFor
		rf.log = persistedLog
	}

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

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	term         int //候选人的任期号
	candidateId  int //请求选票的候选人
	lastLogIndex int //候选人的最新日志条目的索引值
	lastLogTerm  int //候选人最新日志条目的任期号
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	term        int  //当前任期号
	voteGranted bool //候选人是否获得选票
}

type AppendEntryArgs struct {
	term         int        // leader的任期号
	leaderId     int        // leaderID
	prevLogIndex int        // 新日志之前日志的索引值
	prevLogTerm  int        // 新日志之前日志的Term
	entries      []logEntry // 存储的日志条目，为空时代表心跳
	leaderCommit int        // leader已经提交的日志的索引
}

type AppendEntryReply struct {
	term        int  // 用于更新leader任期
	success     bool // follower日志条目集是否与PrevLogIndex,PrevLogTerm匹配
	commitIndex int  // 返回与Leader任期对应的匹配项以同步日志
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//当前节点任期大于请求方任期，不投票
	if rf.currentTerm > args.term {
		reply.term = rf.currentTerm
		reply.voteGranted = false
		return
	} else if rf.currentTerm == args.term && rf.votedFor != -1 &&
		rf.votedFor != args.candidateId {
		//双方任期一致但当前节点已投票给其他节点
		reply.term = rf.currentTerm
		reply.voteGranted = false
		return
	} else {
		//双方节点本身满足条件
		length := len(rf.log) - 1
		term1 := rf.log[length].term
		term2 := args.lastLogTerm
		//当前节点日志条目集新于请求方节点，不投票
		if term1 > term2 || (term1 == term2 && length > args.lastLogIndex) {
			reply.term = rf.currentTerm
			reply.voteGranted = false
			return
		} else {
			//日志也满足条件，当前节点转为Fllower进行投票
			rf.currentTerm = args.term
			rf.changeState("Follower")
			rf.votedFor = args.candidateId

			reply.term = args.term
			reply.voteGranted = true
			return
		}
	}
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

func (rf *Raft) AppendEntry(args AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.success = false
	//Leader任期小于当前节点
	if rf.currentTerm > args.term {
		reply.term = rf.currentTerm
		rf.resetTimer()
		return
	} else {
		//当前节点转换状态为Follower
		rf.currentTerm = args.term
		reply.term = rf.currentTerm
		rf.changeState("Follower")
		//Leader与节点日志条目与任期等不匹配
		if len(rf.log)-1 < args.prevLogIndex ||
			rf.log[args.prevLogIndex].term != args.prevLogTerm {
			reply.commitIndex = len(rf.log) - 1
			if reply.commitIndex > args.prevLogIndex {
				reply.commitIndex = args.prevLogIndex
			}
			//同一日志条目的任期不一致，需要重新接受
			for reply.commitIndex >= 0 &&
				rf.log[args.prevLogIndex].term != args.prevLogTerm {
				reply.commitIndex--
			}
		} else if args.entries == nil {
			// 心跳用于节点状态的更新
			if rf.lastApplied+1 <= args.leaderCommit {
				rf.commitIndex = args.leaderCommit
				go rf.commitLogs()
			}
			reply.commitIndex = len(rf.log) - 1
			reply.success = true
		} else {
			//日志项不为空 与leader同步日志
			rf.log = rf.log[:args.prevLogIndex+1]
			rf.log = append(rf.log, args.entries...)
			//提交日志
			if rf.lastApplied+1 <= args.leaderCommit {
				rf.commitIndex = args.leaderCommit
				go rf.commitLogs()
			}

			reply.commitIndex = len(rf.log) - 1
			if args.leaderCommit > rf.commitIndex {
				if args.leaderCommit < len(rf.log)-1 {
					reply.commitIndex = args.leaderCommit
				}
			}
			reply.success = true
		}
		rf.resetTimer()
	}
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
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
	defer rf.mu.Unlock()

	if rf.state != "Leader" {
		return index, term, isLeader
	}

	nlog := logEntry{command, rf.currentTerm}
	isLeader = (rf.state == "Leader")
	rf.log = append(rf.log, nlog)

	fmt.Printf("leader append log [leader=%d], [term=%d], [command=%v]\n",
		rf.me, rf.currentTerm, command)

	index = len(rf.log)
	term = rf.currentTerm

	rf.persist()

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

func (rf *Raft) doElection() { //发起选举
	rf.currentTerm++
	rf.changeState("Candidate") //转换为选举人相关操作

	term := rf.currentTerm //选票请求RPC参数
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].term

	args := RequestVoteArgs{
		term:         term,
		candidateId:  rf.me,
		lastLogIndex: lastLogIndex,
		lastLogTerm:  lastLogTerm,
	}

	for peer := range rf.peers { //对peers内全部节点发送RPC
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(peer, &args, &reply)
			//成功发送RPC并得到返回
			if ok {
				//双方任期一致，且获得了选票
				if rf.currentTerm == reply.term && reply.voteGranted == true {
					rf.votes++
					//获得选票过半，可以确定该节点转为Leader
					if rf.votes >= len(rf.peers)/2 {
						rf.changeState("Leader")
					}
				} else if rf.currentTerm < reply.term {
					//当前候选者任期小于通信方节点任期
					rf.currentTerm = reply.term
					rf.changeState("Follower")
				} else {
					//当前候选者任期与通信方节点未匹配
				}
			}
		}(peer)
	}
}

func (rf *Raft) leaderSendEntries() {
	for peer, _ := range rf.peers {
		//对peers内全部节点发送RPC
		if peer == rf.me {
			continue
		}
		nextIndex := rf.nextIndex[peer]
		if nextIndex <= 0 {
			nextIndex = 1
		}
		preIndex := nextIndex - 1
		preTerm := rf.log[preIndex].term
		logEntries := make([]logEntry, len(rf.log)-nextIndex)
		//若当前节点日志条目集比待发送的peer节点新，将待更新的日志条目集赋给RPC参数
		if len(rf.log)-1 >= rf.nextIndex[peer] {
			logEntries = append(logEntries, rf.log[rf.nextIndex[peer]:]...)
		}

		//待添加日志条目集参数
		args := AppendEntryArgs{
			term:         rf.currentTerm,
			leaderId:     rf.me,
			prevLogIndex: preIndex,
			prevLogTerm:  preTerm,
			entries:      logEntries,
			leaderCommit: rf.commitIndex,
		}

		go func(peer int) {
			reply := AppendEntryReply{}
			//由于发送前未通过锁保护，当前节点状态可能改变，要确保节点为Leader
			if rf.state == "Leader" {
				//发送日志条目集
				ok := rf.sendAppendEntry(peer, &args, &reply)
				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					//接收方任期大于Leader
					if reply.term > rf.currentTerm {
						rf.currentTerm = reply.term
						rf.changeState("Follower")
						return
					}
					//接收成功
					if reply.success {
						//根据接收方的提交日志索引更新nextIndex
						rf.nextIndex[peer] = reply.commitIndex + 1
						rf.matchIndex[peer] = reply.commitIndex

						commitedIndex := 0
						//计算日志成功复制到其他节点的数量
						for i, _ := range rf.peers {
							if i == rf.me {
								continue
							} else {
								if rf.matchIndex[i] >= rf.matchIndex[peer] {
									commitedIndex++
								}
							}
						}
						//成功复制到超过半数节点则可以将日志提交
						if commitedIndex >= len(rf.peers)/2+1 &&
							rf.commitIndex < rf.matchIndex[peer] &&
							rf.log[rf.matchIndex[peer]].term == rf.currentTerm {
							rf.commitIndex = rf.matchIndex[peer]
							go rf.commitLogs()
						}
					}
				} else {
					//Leader日志未与接收方匹配，通过心跳进行更新
					rf.nextIndex[peer] = reply.commitIndex + 1
					args.term = rf.currentTerm
					args.prevLogIndex = rf.nextIndex[peer] - 1
					args.prevLogTerm = rf.log[args.prevLogTerm].term
					args.entries = nil
					args.leaderCommit = rf.commitIndex
					rf.sendAppendEntry(peer, &args, &reply)
				}
				rf.resetTimer()
			}
		}(peer)

	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		select {
		//超时选举
		case <-rf.electionTimer.C:
			rf.doElection()
		//定时发送心跳
		case <-rf.heartBeatTimer.C:
			if rf.state == "Leader" {
				rf.leaderSendEntries()
			}
		}
		rf.mu.Unlock()
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]logEntry, 1)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.state = "Follower"
	rf.votes = 0
	rf.applyCh = applyCh

	rf.heartBeatTimer = time.NewTimer(100 * time.Millisecond)
	rf.electionTimer = time.NewTimer(200 * time.Millisecond)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
