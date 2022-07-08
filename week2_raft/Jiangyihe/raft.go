package raft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

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
	//当前状态
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2

	//改变成这个状态（出错的时候再改变）
	TO_FOLLOWER  = 0
	TO_CANDIDATE = 1
	TO_LEADER    = 2

	//选举的间隔时间（出现failed可调整数值，但要保证大于heartbeat）
	ELECTION_TIMEOUT_MAX = 200
	ELECTION_TIMEOUT_MIN = 100

	HEARTBEAT_TIMEOUT = 50
	APPLIED_TIMEOUT   = 28
)

type Entry struct { //日志集结构体
	Term    int
	Command interface{}
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm int     //服务器已知最新的任期（在服务器首次启动时初始化为0，单调递增）
	votedFor    int     //当前任期内收到选票的 candidateId，如果没有投给任何候选人 则为空
	log         []Entry //日志条目；每个条目包含了用于状态机的命令，以及领导人接收到该条目时的任期（初始索引为1）

	commitIndex int //已知已提交的最高的日志条目的索引（初始值为0，单调递增）
	lastApplied int //已经被应用到状态机的最高的日志条目的索引（初始值为0，单调递增）

	nextIndex  []int //对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导人最后的日志条目的索引+1）
	matchIndex []int //对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增）

	getVoteNum            int       //获得的票数
	state                 int       //状态
	lastResetElectionTime time.Time //重新选举的时间

	applyCh chan ApplyMsg

	lastSnapShotIndex int //最后的快照（snapshot）指向的索引
	lastSnapShotTerm  int //最后的快照（snapshot）指向的任期
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() == true { //如果被kill
		return -1, -1, false
	}
	if rf.state != LEADER { //如果不是leader也无法start
		return -1, -1, false
	} else {
		index := rf.getLastIndex() + 1
		term := rf.currentTerm
		rf.log = append(rf.log, Entry{Term: term, Command: command}) //追加条目

		rf.persist() //持久化2C
		return index, term, true
	}
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) changeState(howtochange int, resetTime bool) {

	//状态改变
	if howtochange == TO_FOLLOWER {
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.getVoteNum = 0
		rf.persist() //持久化2C
		if resetTime {
			rf.lastResetElectionTime = time.Now()
		}
	}

	if howtochange == TO_CANDIDATE {
		rf.state = CANDIDATE
		rf.votedFor = rf.me //候选人给自己投票
		rf.getVoteNum = 1
		rf.currentTerm += 1
		rf.persist()
		rf.candidateJoinElection() //变成候选人之后自动加入投票
		rf.lastResetElectionTime = time.Now()
	}

	if howtochange == TO_LEADER { //状态改为leader
		rf.state = LEADER
		rf.votedFor = -1
		rf.getVoteNum = 0
		rf.persist()

		rf.nextIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			//nextIndex为下一条日志的索引 == len(rf.log)
			//所以len(rf.log)-1为当前日志的索引 == rf.getLastIndex()
			rf.nextIndex[i] = rf.getLastIndex() + 1
		}

		rf.matchIndex = make([]int, len(rf.peers))
		//rf.matchIndex[rf.me]为已经复制到该server的最高日志条目的索引
		rf.matchIndex[rf.me] = rf.getLastIndex()
		rf.lastResetElectionTime = time.Now()
	}
}

func (rf *Raft) getLastIndex() int { //获取最新索引
	return len(rf.log) - 1 + rf.lastSnapShotIndex
}

//追加条目RPC

type AppendEntriesArgs struct {
	Term         int     //领导人的任期
	LeaderId     int     //领导人 ID 因此跟随者可以对客户端进行重定向
	PrevLogIndex int     //紧邻新日志条目之前的那个日志条目的索引
	PrevLogTerm  int     //紧邻新日志条目之前的那个日志条目的任期
	Entries      []Entry //需要被保存的日志条目（被当做心跳使用时，则日志条目内容为空；为了提高效率可能一次性发送多个）
	LeaderCommit int     //领导人的已知已提交的最高的日志条目的索引
}

type AppendEntriesReply struct {
	Term             int  //当前任期，对于领导人而言 它会更新自己的任期
	Success          bool //如果跟随者所含有的条目和 prevLogIndex 以及 prevLogTerm 匹配上了，则为 true
	ConflictingIndex int  // 方便找到下一个索引（是reply的nextIndex）
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//当前节点的任期比请求者的任期还要大----请求者无权成为leader----返回false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictingIndex = -1
		return
	}

	//请求者的任期更大----满足了成为leader的先决条件
	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm //这个任期就是返回的任期（args给的term）
	reply.Success = true
	reply.ConflictingIndex = -1

	if rf.state != FOLLOWER { //如果状态不对了---正常此时rf应为follower
		rf.changeState(TO_FOLLOWER, true) //改变状态
	} else {
		rf.lastResetElectionTime = time.Now()
		rf.persist()
	}

	if rf.lastSnapShotIndex > args.PrevLogIndex { //最后的快照索引也比请求者大-----投票失败
		reply.Success = false
		reply.ConflictingIndex = rf.getLastIndex() + 1
		return
	}

	if rf.getLastIndex() < args.PrevLogIndex { //正常getLastIndex应该最后的快照索引和当前rf的索引
		reply.Success = false
		reply.ConflictingIndex = rf.getLastIndex()
		return
	} else {
		if rf.getLogTermWithIndex(args.PrevLogIndex) != args.PrevLogTerm { //任期不一样
			reply.Success = false
			//确定任期
			tempTerm := rf.getLogTermWithIndex(args.PrevLogIndex)
			for index := args.PrevLogIndex; index >= rf.lastSnapShotIndex; index-- {
				if rf.getLogTermWithIndex(index) != tempTerm {
					//如果任期不相等，返回reply的下一个索引
					reply.ConflictingIndex = index + 1
					break
				}
			}
			return
		}
	}

	//在日志中添加索引和需要被保存的日志条目
	rf.log = append(rf.log[:args.PrevLogIndex+1-rf.lastSnapShotIndex], args.Entries...)
	rf.persist()

	//根据论文，当LeaderCommit大于commitIndex时，进行重置
	if args.LeaderCommit > rf.commitIndex {
		rf.updateCommitIndex(FOLLOWER, args.LeaderCommit)
	}
	return
}

// 请求投票RPC

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //候选人的任期号
	CandidateId  int //请求选票的候选人的ID
	LastLogIndex int //候选人的最后日志条目的索引值
	LastLogTerm  int //候选人最后日志条目的任期号
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //当前任期号，以便于候选人去更新自己的任期号
	VoteGranted bool //候选人赢得了此张选票时为真
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm { //请求者任期小于此rf任期----不用投票
		reply.VoteGranted = false
		reply.Term = rf.currentTerm //给请求者更新任期
		return
	}
	reply.Term = rf.currentTerm //上述情况下将该rf任期返回

	if args.Term > rf.currentTerm { //这是正常情况，请求者任期大于该rf任期
		rf.currentTerm = args.Term
		rf.changeState(TO_FOLLOWER, false) //将该节点变成follower
		rf.persist()
	}

	if rf.UpToDate(args.LastLogIndex, args.LastLogTerm) == false { //不满足“至少比自己新”投票原则，投票无效
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	//投票失败的其他情况
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId && args.Term == reply.Term {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	} else {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true   //投票成功
		rf.currentTerm = args.Term //更新任期
		reply.Term = rf.currentTerm
		rf.lastResetElectionTime = time.Now()
		rf.persist()
		return
	}
	return
}

func (rf *Raft) UpToDate(index int, term int) bool { //peers只会投票给“至少比自己更新”的候选人
	lastIndex := rf.getLastIndex()
	lastTerm := rf.getLastTerm()
	return term > lastTerm || (term == lastTerm && index >= lastIndex)
}

func (rf *Raft) candidateElectionTicker() { //在ticker中进行超时触发
	for rf.killed() == false {
		nowTime := time.Now()
		timet := getRand(int64(rf.me))
		time.Sleep(time.Duration(timet) * time.Millisecond)
		rf.mu.Lock()
		if rf.lastResetElectionTime.Before(nowTime) && rf.state != LEADER { //超时并且不是leader
			rf.changeState(TO_CANDIDATE, true) //该server转为候选人，加入投票
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) candidateJoinElection() { //候选人加入选举
	for index := range rf.peers {
		if index == rf.me {
			continue
		}

		go func(server int) {
			rf.mu.Lock()
			arg := RequestVoteArgs{
				rf.currentTerm,
				rf.me,
				rf.getLastIndex(),
				rf.getLastTerm(),
			}
			reply := RequestVoteReply{}
			rf.mu.Unlock()

			re := rf.sendRequestVote(server, &arg, &reply) //向所有server发送投票请求
			if re == true {
				rf.mu.Lock()
				//如果当前状态不是选举人或者任期不匹配，返回
				if rf.state != CANDIDATE || arg.Term < rf.currentTerm {
					rf.mu.Unlock()
					return
				}

				if reply.VoteGranted == true && rf.currentTerm == arg.Term {
					rf.getVoteNum++                         //成功，票数++（选举人给自己投票）
					if rf.getVoteNum >= len(rf.peers)/2+1 { //票数过半成为leader
						rf.changeState(TO_LEADER, true) //状态改成leader
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
					return
				}

				if reply.Term > arg.Term { //没选上，已经有了新的leader
					if rf.currentTerm < reply.Term {
						rf.currentTerm = reply.Term //更新任期
					}
					rf.changeState(TO_FOLLOWER, false) //状态改成follower
					rf.mu.Unlock()
					return
				}

				rf.mu.Unlock()
				return
			}

		}(index)
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//向peers（所有server）发送投票请求
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//追加条目RPC（由领导人调用，用于日志条目的复制，同时也被当做心跳使用）
func (rf *Raft) leaderAppendEntriesTicker() { //在ticker中进行超时触发
	for rf.killed() == false {
		time.Sleep(HEARTBEAT_TIMEOUT * time.Millisecond)
		rf.mu.Lock()
		if rf.state == LEADER { //状态必须是leader
			rf.mu.Unlock()
			rf.leaderAppendEntries() //触发追加条目（下面的函数）
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) leaderAppendEntries() {
	// 向每个server发送追加条目（日志复制）
	for index := range rf.peers { //遍历peers
		if index == rf.me {
			continue
		}

		go func(server int) {
			rf.mu.Lock()
			if rf.state != LEADER { //这是leader应该做的，不是leader就出去
				rf.mu.Unlock()
				return
			}
			prevLogIndextemp := rf.nextIndex[server] - 1
			if prevLogIndextemp < rf.lastSnapShotIndex { //如果snapshot索引更大
				return
			}

			aeArgs := AppendEntriesArgs{}

			if rf.getLastIndex() >= rf.nextIndex[server] {
				//追加条目
				entriesNeeded := make([]Entry, 0) //初始化
				entriesNeeded = append(entriesNeeded, rf.log[rf.nextIndex[server]-rf.lastSnapShotIndex:]...)
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
				//leader发送空的附加日志（AppendEntries）RPC（心跳）给其他所有的服务器
				prevLogIndex, prevLogTerm := rf.getPrevLogInfo(server)
				aeArgs = AppendEntriesArgs{
					rf.currentTerm,
					rf.me,
					prevLogIndex,
					prevLogTerm,
					[]Entry{}, //空的entry（心跳包）
					rf.commitIndex,
				}
			}
			aeReply := AppendEntriesReply{}
			rf.mu.Unlock()

			//定义发送追加条目变量re
			re := rf.sendAppendEntries(server, &aeArgs, &aeReply)

			if re == true { //发送成功的话
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.state != LEADER { //当前状态不是leader则返回
					return
				}

				if aeReply.Term > rf.currentTerm { //返回的任期比当前状态（leader）任期大
					rf.currentTerm = aeReply.Term     //更新当前状态任期
					rf.changeState(TO_FOLLOWER, true) //状态变成follower
					return
				}

				if aeReply.Success {
					//如果follower所含有的条目和 PrevLogIndex 以及 PrevLogTerm 匹配上了
					//更新matchIndex和nextIndex
					rf.matchIndex[server] = aeArgs.PrevLogIndex + len(aeArgs.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1
					rf.updateCommitIndex(LEADER, 0)
				}

				if !aeReply.Success { //没成功的话
					if aeReply.ConflictingIndex != -1 {
						//用ConflictingIndex来标记
						rf.nextIndex[server] = aeReply.ConflictingIndex
					}
				}
			}
		}(index)
	}
}

func (rf *Raft) committedToAppliedTicker() {
	//把committed entry应用到状态机
	for rf.killed() == false {
		time.Sleep(APPLIED_TIMEOUT * time.Millisecond)
		rf.mu.Lock()

		if rf.lastApplied >= rf.commitIndex { //commitIndex < lastApplied,跳出
			rf.mu.Unlock()
			continue
		}

		Messages := make([]ApplyMsg, 0)

		//如果commitIndex > lastApplied，则 lastApplied 递增，并将log[lastApplied]应用到状态机中
		for rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.getLastIndex() {
			rf.lastApplied++
			Messages = append(Messages, ApplyMsg{
				CommandValid:  true,
				SnapshotValid: false,
				CommandIndex:  rf.lastApplied,
				Command:       rf.getLogWithIndex(rf.lastApplied).Command,
			})
		}
		rf.mu.Unlock()

		for _, messages := range Messages { //遍历，将已提交日志应用到状态机
			rf.applyCh <- messages
		}
	}

}

func (rf *Raft) getLogWithIndex(globalIndex int) Entry { //通过索引获取日志
	return rf.log[globalIndex-rf.lastSnapShotIndex]
}

func (rf *Raft) getPrevLogInfo(server int) (int, int) { //获取紧邻最新的日志的信息
	newEntryBeginIndex := rf.nextIndex[server] - 1 //nextIndex往前去一个就是最新的
	lastIndex := rf.getLastIndex()
	if newEntryBeginIndex == lastIndex+1 { //如果lastIndex正好比newEntryBeginIndex多1
		newEntryBeginIndex = lastIndex //则将lastIndex的值赋给newEntryBeginIndex
	}
	return newEntryBeginIndex, rf.getLogTermWithIndex(newEntryBeginIndex)
}

func (rf *Raft) getLogTermWithIndex(globalIndex int) int { //通过索引获取任期
	if globalIndex-rf.lastSnapShotIndex == 0 {
		return rf.lastSnapShotTerm
	}
	return rf.log[globalIndex-rf.lastSnapShotIndex].Term
}

func (rf *Raft) updateCommitIndex(role int, leaderCommit int) {

	/*	如果领导人的已知已提交的最高日志条目的索引大于接收者的已知已提交最高日志条目的索引（leaderCommit > commitIndex），
		则把接收者的已知已经提交的最高的日志条目的索引commitIndex，重置为领导人的已知已经提交的最高的日志条目的索引leaderCommit，
		或者是上一个新条目的索引。取两者的最小值*/
	if role != LEADER {
		if leaderCommit > rf.commitIndex {
			lastNewIndex := rf.getLastIndex()
			if leaderCommit >= lastNewIndex {
				rf.commitIndex = lastNewIndex
			} else {
				rf.commitIndex = leaderCommit
			}
		}
		return
	}

	if role == LEADER {
		rf.commitIndex = rf.lastSnapShotIndex

		/*		假设存在n满足n > commitIndex，使得大多数的 matchIndex[i] ≥ n以及log[n].term == currentTerm 成立，
				则令 commitIndex = n   */
		for n := rf.getLastIndex(); n >= rf.lastSnapShotIndex+1; n-- {
			sum := 0
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					sum++
					continue
				}
				if rf.matchIndex[i] >= n {
					sum++
				}
			}

			if sum >= len(rf.peers)/2+1 && rf.getLogTermWithIndex(n) == rf.currentTerm {
				rf.commitIndex = n
				break
			}
		}
		return
	}
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) persistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	//论文中提出需要持久化currentTerm, votedFor, log[]三个变量,可以理解成编码
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	//对snapshot也进行持久化
	e.Encode(rf.lastSnapShotIndex)
	e.Encode(rf.lastSnapShotTerm)

	data := w.Bytes()
	return data
}

func (rf *Raft) persist() {
	data := rf.persistData()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var persist_currentTrem int
	var persist_voteFor int
	var persist_log []Entry

	var persist_lastSSPointIndex int
	var persist_lastSSPointTerm int

	//解码
	if d.Decode(&persist_currentTrem) == nil && d.Decode(&persist_voteFor) == nil && d.Decode(&persist_log) == nil &&
		d.Decode(&persist_lastSSPointIndex) == nil && d.Decode(&persist_lastSSPointTerm) == nil {
		//} else {
		rf.currentTerm = persist_currentTrem
		rf.votedFor = persist_voteFor
		rf.log = persist_log

		rf.lastSnapShotIndex = persist_lastSSPointIndex
		rf.lastSnapShotTerm = persist_lastSSPointTerm
	}
}

func getRand(server int64) int { //随机时间
	rand.Seed(time.Now().Unix() + server)
	return rand.Intn(ELECTION_TIMEOUT_MAX-ELECTION_TIMEOUT_MIN) + ELECTION_TIMEOUT_MIN
}

func (rf *Raft) getLastTerm() int { //获取最新任期
	if len(rf.log)-1 == 0 {
		return rf.lastSnapShotTerm
	} else {
		return rf.log[len(rf.log)-1].Term
	}
}

//发送追加条目
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// 初始化
	rf.mu.Lock()
	rf.state = FOLLOWER

	rf.currentTerm = 0
	rf.getVoteNum = 0
	rf.votedFor = -1

	rf.lastApplied = 0
	rf.commitIndex = 0

	rf.lastSnapShotIndex = 0
	rf.lastSnapShotTerm = 0

	rf.log = []Entry{}
	rf.log = append(rf.log, Entry{}) //追加日志条目
	rf.applyCh = applyCh
	rf.mu.Unlock()

	rf.readPersist(persister.ReadRaftState())
	if rf.lastSnapShotIndex > 0 {
		rf.lastApplied = rf.lastSnapShotIndex
	}

	//依次触发ticker
	go rf.candidateElectionTicker()
	go rf.leaderAppendEntriesTicker()
	go rf.committedToAppliedTicker()
	return rf
}
