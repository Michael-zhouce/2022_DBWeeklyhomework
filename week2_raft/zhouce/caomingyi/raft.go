package demo

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
// ApplicationMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplicationMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplicationMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplicationMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplicationMsg, but set CommandValid to false for these other uses.
type ApplicationMsg struct {
	IsLog bool // true为log，false为snapshot

	// 向application层提交日志
	Operator interface{}
	LogIndex int
	LogItem  int

	// 向application层安装快照
	Snapshot          []byte
	LastSnapshotIndex int
	LastSnapshotTerm  int
}

// 日志项
type LogEntry struct {
	Command interface{}
	Term    int
}

// 当前角色
const ROLE_LEADER = "Leader"
const ROLE_FOLLOWER = "Follower"
const ROLE_CANDIDATES = "Candidates"

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex  // Lock to protect shared access to this peer's state
	peers     []*EndPoint // RPC end points of all nodes
	persister *Persister  // Object to hold this peer's persisted state
	me        int         // this peer's index into peers[]
	dead      int32       // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 所有服务器，持久化状态（lab-2A不要求持久化）
	currentTerm       int        // 见过的最大任期
	votedFor          int        // 记录在currentTerm任期投票给谁了
	log               []LogEntry // 操作日志
	lastSnapshotIndex int        // snapshot最后1个logEntry的index，没有snapshot则为0
	lastSnapshotTerm  int        // snapthost最后1个logEntry的term，没有snaphost则无意义

	// 所有服务器，易失状态
	commitIndex int // 已知的最大已提交索引
	lastApplied int // 当前应用到状态机的索引

	// 仅Leader，易失状态（成为leader时重置）
	//每个节点进行同步，也就是子节点从哪里开始同步
	nextIndex  []int //	每个follower的log同步起点索引（初始为leader log的最后一项）
	matchIndex []int // 每个follower的log同步进度（初始为0），和nextIndex强关联

	// 所有服务器，选举相关状态
	role              string    // 身份
	leaderId          int       // leader的id
	lastActiveTime    time.Time // 上次活跃时间（刷新时机：收到leader心跳、给其他candidates投票、请求其他节点投票）
	lastBroadcastTime time.Time // 作为leader，上次的广播时间

	applyCh chan ApplicationMsg // 应用层的提交队列
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.role == ROLE_LEADER
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// 不用加锁，外层逻辑会锁
	DPrintf("RaftNode[%d] persist starts, currentTerm[%d] voteFor[%d] log[%d]", rf.me, rf.currentTerm, rf.votedFor, len(rf.log))
	data := rf.compressRaftState()
	rf.persister.SetRaftState(data)
}

// 把raft作为字节数组保存，为持久化做准备，存到硬盘里，防止丢失
func (rf *Raft) compressRaftState() []byte {
	w := new(bytes.Buffer)
	e := NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastSnapshotIndex)
	e.Encode(rf.lastSnapshotTerm)
	data := w.Bytes()
	return data
}

// 通过字节数组创建对象，生成raft对象，把字节转换成
func (rf *Raft) uncompressRaftState(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := NewDecoder(r)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	d.Decode(&rf.currentTerm) //周期
	d.Decode(&rf.votedFor)    //投票
	d.Decode(&rf.log)         //命令

	d.Decode(&rf.lastSnapshotIndex)
	d.Decode(&rf.lastSnapshotTerm)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
//	发起请求投票
type RequestVoteParams struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int // 最后一条日志
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteResponse struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// todo 向follow节点发起同步操作日志模块
type AppendEntriesParams struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesResponse struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

// 向follow节点发起同步快照操作
type InstallSnapshotParams struct {
	Term              int
	LeaderId          int
	LastSnapshotIndex int
	LastSnapshotTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotResponse struct {
	Term int
}

// 当前raft节点接收到别的raft节点发起的请求投票的服务处理
func (rf *Raft) RequestVote(args *RequestVoteParams, reply *RequestVoteResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//todo 初始默认为当前raft节点任期，且默认为反对票。
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	//todo  任期不如我大，默认值返回，投反对票
	if args.Term < rf.currentTerm {
		return
	}

	//todo 发现更大的任期，则转为该任期的follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = ROLE_FOLLOWER
		rf.votedFor = -1
		rf.leaderId = -1
		// 继续向下走，进行投票
	}

	//todo 每个任期，只能投票给1人
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// candidate的日志必须比我的新
		// 1, 最后一条log的任期大的更新
		// 2，任期相同, 更长的log则更新
		lastLogTerm := rf.lastTerm()
		// log长度一样也是可以给对方投票的
		// 只有候选人的日志对应的任期大于当前raft节点最后日志的任期
		// 或者日志任期对应相等，则候选人的日志序列必须大于等于当前raft节点的序列。
		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= rf.lastIndex()) {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.lastActiveTime = time.Now() // 为其他人投票，那么重置自己的下次投票时间
		}
	}
	rf.persist()
}

// 持久化
func (rf *Raft) AppendEntries(args *AppendEntriesParams, reply *AppendEntriesResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	if args.Term < rf.currentTerm {
		return
	}

	// 发现更大的任期，则转为该任期的follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = ROLE_FOLLOWER
		rf.votedFor = -1
		rf.persist()
		// 继续向下走
	}

	// 认识新的leader
	rf.leaderId = args.LeaderId
	// 刷新活跃时间
	rf.lastActiveTime = time.Now()

	// 如果prevLogIndex在快照内，且不是快照最后一个log，那么只能从index=1开始同步了
	if args.PrevLogIndex < rf.lastSnapshotIndex {
		reply.ConflictIndex = 1
		return
	} else if args.PrevLogIndex == rf.lastSnapshotIndex { // prevLogIndex正好等于快照的最后一个log
		if args.PrevLogTerm != rf.lastSnapshotTerm { // 冲突了，那么从index=1开始同步吧
			reply.ConflictIndex = 1
			return
		}
		// 否则继续走后续的日志覆盖逻辑
	} else { // prevLogIndex在快照之后，那么进一步判定
		if args.PrevLogIndex > rf.lastIndex() { // prevLogIndex位置没有日志的case
			reply.ConflictIndex = rf.lastIndex() + 1
			return
		}
		// prevLogIndex位置有日志，那么判断term必须相同，否则false
		if rf.log[rf.index2LogPos(args.PrevLogIndex)].Term != args.PrevLogTerm {
			reply.ConflictTerm = rf.log[rf.index2LogPos(args.PrevLogIndex)].Term
			for index := rf.lastSnapshotIndex + 1; index <= args.PrevLogIndex; index++ { // 找到冲突term的首次出现位置，最差就是PrevLogIndex
				if rf.log[rf.index2LogPos(index)].Term == reply.ConflictTerm {
					reply.ConflictIndex = index
					break
				}
			}
			return
		}
		// 否则继续走后续的日志覆盖逻辑
	}

	// 保存日志，提交持久化之后就放到硬盘中去了，不会丢失
	for i, logEntry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		logPos := rf.index2LogPos(index)
		if index > rf.lastIndex() { // 超出现有日志长度，继续追加
			rf.log = append(rf.log, logEntry)
		} else { // 重叠部分
			if rf.log[logPos].Term != logEntry.Term {
				rf.log = rf.log[:logPos]          // 删除当前以及后续所有log
				rf.log = append(rf.log, logEntry) // 把新log加入进来
			} // term一样啥也不用做，继续向后比对Log
		}
	}
	rf.persist()

	// 更新提交下标
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if rf.lastIndex() < rf.commitIndex {
			rf.commitIndex = rf.lastIndex()
		}
	}
	reply.Success = true
}

// 安装快照RPC Handler
func (rf *Raft) InstallSnapshot(args *InstallSnapshotParams, reply *InstallSnapshotResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	// 发现更大的任期，则转为该任期的follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = ROLE_FOLLOWER
		rf.votedFor = -1
		rf.persist()
		// 继续向下走
	}

	// 认识新的leader
	rf.leaderId = args.LeaderId
	// 刷新活跃时间
	rf.lastActiveTime = time.Now()

	// leader快照不如本地长，那么忽略这个快照
	if args.LastSnapshotIndex <= rf.lastSnapshotIndex {
		return
	} else { // leader快照比本地快照长
		if args.LastSnapshotIndex < rf.lastIndex() { // 快照外还有日志，判断是否需要截断
			if rf.log[rf.index2LogPos(args.LastSnapshotIndex)].Term != args.LastSnapshotTerm {
				rf.log = make([]LogEntry, 0) // term冲突，扔掉快照外的所有日志
			} else { // term没冲突，保留后续日志
				leftLog := make([]LogEntry, rf.lastIndex()-args.LastSnapshotIndex)
				copy(leftLog, rf.log[rf.index2LogPos(args.LastSnapshotIndex)+1:])
				rf.log = leftLog
			}
		} else {
			rf.log = make([]LogEntry, 0) // 快照比本地日志长，日志就清空了
		}
	}
	// 更新快照位置
	rf.lastSnapshotIndex = args.LastSnapshotIndex
	rf.lastSnapshotTerm = args.LastSnapshotTerm
	// 持久化raft state和snapshot
	rf.persister.SaveStateAndSnapshot(rf.compressRaftState(), args.Data)

	// snapshot提交给应用层
	rf.saveSnapshotToApplication()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so invokeRemoteServiceer should
// pass &reply.
// the types of the args and reply passed to invokeRemoteService() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// invokeRemoteService() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, invokeRemoteService() returns true; otherwise
// invokeRemoteService() returns false. Thus invokeRemoteService() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// invokeRemoteService() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around invokeRemoteService().
//
// look at the comments in mit-6.824/src/labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the invokeRemoteServiceer passes the address of the reply struct with &, not
// the struct itself.
// 1->2  2 拿到server
func (rf *Raft) sendRequestVote(server int, args *RequestVoteParams, reply *RequestVoteResponse) bool {
	ok := rf.peers[server].invokeRemoteService("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesParams, reply *AppendEntriesResponse) bool {
	ok := rf.peers[server].invokeRemoteService("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotParams, reply *InstallSnapshotResponse) bool {
	ok := rf.peers[server].invokeRemoteService("Raft.InstallSnapshot", args, reply)
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
//
// 已兼容snapshot
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 只有leader才能写入
	if rf.role != ROLE_LEADER {
		return -1, -1, false
	}

	logEntry := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	}
	rf.log = append(rf.log, logEntry)
	index = rf.lastIndex()
	term = rf.currentTerm
	rf.persist()

	DPrintf("RaftNode[%d] Add Command, logIndex[%d] currentTerm[%d]", rf.me, index, term)
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does invokeRemoteService the Kill() method. your code can use killed() to
// check whether Kill() has been invokeRemoteServiceed. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should invokeRemoteService killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 已经兼容snapshot
// 当前raft节点持续刷新时间
// 如果为候选人 则进行发送请求投票的消息
// 遍历当前 raft对象下的peers ,依次向他们发送投票请求
// 不停循环，进行选举
func (rf *Raft) electionLoop() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			now := time.Now()
			//todo 随机200 ~350 毫秒
			timeout := time.Duration(200+rand.Int31n(150)) * time.Millisecond // 超时随机化
			durationTime := now.Sub(rf.lastActiveTime)
			// follower -> candidates
			if rf.role == ROLE_FOLLOWER {
				if durationTime >= timeout {
					rf.role = ROLE_CANDIDATES
					DPrintf("RaftNode[%d] Follower -> Candidate", rf.me)
				}
			}
			// 请求vote
			if rf.role == ROLE_CANDIDATES && durationTime >= timeout {
				rf.lastActiveTime = now // 重置下次选举时间

				rf.currentTerm += 1 // 发起新任期
				rf.votedFor = rf.me // 该任期投了自己
				rf.persist()

				// 请求投票req
				//此节点的term,id,最大日志序列化发送给子节点
				voteReq := RequestVoteParams{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: rf.lastIndex(),
				}
				voteReq.LastLogTerm = rf.lastTerm()

				rf.mu.Unlock()

				// 并发RPC请求vote
				type VoteResult struct {
					peerId int
					resp   *RequestVoteResponse
				}
				voteCount := 1   // 收到投票个数（先给自己投1票）
				finishCount := 1 // 收到应答个数
				voteResultChan := make(chan *VoteResult, len(rf.peers))
				for peerId := 0; peerId < len(rf.peers); peerId++ {
					go func(id int) {
						if id == rf.me {
							return
						}
						resp := RequestVoteResponse{}
						//发送子节点
						if ok := rf.sendRequestVote(id, &voteReq, &resp); ok {
							voteResultChan <- &VoteResult{peerId: id, resp: &resp}
						} else {
							voteResultChan <- &VoteResult{peerId: id, resp: nil}
						}
					}(peerId)
				}
				//处理接收到的请求投票的结果
				//处理收到的结果是赞同票就加一
				maxTerm := 0
				for {
					select {
					case voteResult := <-voteResultChan:
						finishCount += 1
						if voteResult.resp != nil {
							if voteResult.resp.VoteGranted {
								voteCount += 1
							}
							if voteResult.resp.Term > maxTerm {
								maxTerm = voteResult.resp.Term
							}
						}
						// 得到大多数vote后，立即离开，大于百分之50
						if finishCount == len(rf.peers) || voteCount > len(rf.peers)/2 {
							goto VOTE_END
						}
					}
				}
			VOTE_END:
				rf.mu.Lock()
				defer func() {
					DPrintf("RaftNode[%d] RequestVote ends, finishCount[%d] voteCount[%d] Role[%s] maxTerm[%d] currentTerm[%d]", rf.me, finishCount, voteCount,
						rf.role, maxTerm, rf.currentTerm)
				}()
				// 如果角色改变了，则忽略本轮投票结果
				if rf.role != ROLE_CANDIDATES {
					return
				}
				// 发现了更高的任期，切回follower
				if maxTerm > rf.currentTerm {
					rf.role = ROLE_FOLLOWER
					rf.leaderId = -1
					rf.currentTerm = maxTerm
					rf.votedFor = -1
					rf.persist()
					return
				}
				// 赢得大多数选票，则成为leader
				if voteCount > len(rf.peers)/2 {
					rf.role = ROLE_LEADER
					rf.leaderId = rf.me                       //更新id
					rf.nextIndex = make([]int, len(rf.peers)) //每个节点序列化同步
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = rf.lastIndex() + 1
					}
					rf.matchIndex = make([]int, len(rf.peers))
					for i := 0; i < len(rf.peers); i++ {
						rf.matchIndex[i] = 0
					}
					//把此广播时间定为当前时间，选举完成
					rf.lastBroadcastTime = time.Unix(0, 0) // 令appendEntries广播立即执行
					return
				}
			}
		}()
	}
}

func (rf *Raft) updateCommitIndex() {
	// 数字N, 让nextIndex[i]的大多数>=N
	// peer[0]' index=2
	// peer[1]' index=2
	// peer[2]' index=1
	// 1,2,2
	// 更新commitIndex, 就是找中位数
	sortedMatchIndex := make([]int, 0)
	sortedMatchIndex = append(sortedMatchIndex, rf.lastIndex())
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		sortedMatchIndex = append(sortedMatchIndex, rf.matchIndex[i])
	}
	sort.Ints(sortedMatchIndex)
	newCommitIndex := sortedMatchIndex[len(rf.peers)/2]
	// 如果index属于snapshot范围，那么不要检查term了，因为snapshot的一定是集群提交的
	// 否则还是检查log的term是否满足条件
	if newCommitIndex > rf.commitIndex && (newCommitIndex <= rf.lastSnapshotIndex || rf.log[rf.index2LogPos(newCommitIndex)].Term == rf.currentTerm) {
		rf.commitIndex = newCommitIndex
	}

}

// 日志同步，就是从快照开始同步
func (rf *Raft) doAppendEntries(peerId int) {
	args := AppendEntriesParams{} //同步日志模板
	args.Term = rf.currentTerm    //周期发给子节点
	args.LeaderId = rf.me
	args.LeaderCommit = rf.commitIndex
	args.Entries = make([]LogEntry, 0)
	args.PrevLogIndex = rf.nextIndex[peerId] - 1

	// 如果prevLogIndex是leader快照的最后1条log, 那么取快照的最后1个term
	if args.PrevLogIndex == rf.lastSnapshotIndex {
		args.PrevLogTerm = rf.lastSnapshotTerm
	} else { // 否则一定是log部分
		args.PrevLogTerm = rf.log[rf.index2LogPos(args.PrevLogIndex)].Term
	}
	args.Entries = append(args.Entries, rf.log[rf.index2LogPos(args.PrevLogIndex+1):]...)

	go func() {
		// DPrintf("RaftNode[%d] appendEntries starts, myTerm[%d] peerId[%d]", rf.me, args1.Term, id)
		reply := AppendEntriesResponse{}
		if ok := rf.sendAppendEntries(peerId, &args, &reply); ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			defer func() {
				DPrintf("RaftNode[%d] appendEntries ends,  currentTerm[%d]  peer[%d] logIndex=[%d] nextIndex[%d] matchIndex[%d] commitIndex[%d]",
					rf.me, rf.currentTerm, peerId, rf.lastIndex(), rf.nextIndex[peerId], rf.matchIndex[peerId], rf.commitIndex)
			}()

			// 如果不是rpc前的leader状态了，那么啥也别做了
			if rf.currentTerm != args.Term {
				return
			}
			if reply.Term > rf.currentTerm { // 如果收到的Term更大，变成follower
				rf.role = ROLE_FOLLOWER
				rf.leaderId = -1
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.persist()
				return
			}
			// 因为RPC期间无锁, 可能相关状态被其他RPC(向其他节点发送的选举)修改了
			// 因此这里得根据发出RPC请求时的状态做更新，而不要直接对nextIndex和matchIndex做相对加减
			if reply.Success { //eg.如果第一次同步到100，下一次一定从101开始同步
				rf.nextIndex[peerId] = args.PrevLogIndex + len(args.Entries) + 1
				rf.matchIndex[peerId] = rf.nextIndex[peerId] - 1 //节点同步进度
				rf.updateCommitIndex()                           // 更新commitIndex
			} else { //如果有冲突有
				// 回退优化，参考：https://thesquareplanet.com/blog/students-guide-to-raft/#an-aside-on-optimizations
				//nextIndexBefore := rf.nextIndex[peerId] // 仅为打印log
				//持久化，就算发生问题，数据也不会丢失。
				if reply.ConflictTerm != -1 { // follower的prevLogIndex位置term冲突了
					// 我们找leader log中conflictTerm最后出现位置，如果找到了就用它作为nextIndex，否则用follower的conflictIndex
					conflictTermIndex := -1
					for index := args.PrevLogIndex; index > rf.lastSnapshotIndex; index-- {
						if rf.log[rf.index2LogPos(index)].Term == reply.ConflictTerm {
							conflictTermIndex = index
							break
						}
					}
					if conflictTermIndex != -1 { // leader log出现了这个term，那么从这里prevLogIndex之前的最晚出现位置尝试同步
						rf.nextIndex[peerId] = conflictTermIndex
					} else {
						rf.nextIndex[peerId] = reply.ConflictIndex // 用follower首次出现term的index作为同步开始
					}
				} else {
					// follower没有发现prevLogIndex term冲突, 可能是被snapshot了或者日志长度不够
					// 这时候我们将返回的conflictIndex设置为nextIndex即可
					rf.nextIndex[peerId] = reply.ConflictIndex
				}
			}
		}
	}()
}

func (rf *Raft) doSyncSnapshot(peerId int) {

	args := InstallSnapshotParams{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LastSnapshotIndex = rf.lastSnapshotIndex
	args.LastSnapshotTerm = rf.lastSnapshotTerm
	args.Offset = 0
	args.Data = rf.persister.GetSnapshot()
	args.Done = true

	reply := InstallSnapshotResponse{}

	go func() {
		if rf.sendInstallSnapshot(peerId, &args, &reply) {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 如果不是rpc前的leader状态了，那么啥也别做了
			if rf.currentTerm != args.Term {
				return
			}
			if reply.Term > rf.currentTerm { // 变成follower
				rf.role = ROLE_FOLLOWER
				rf.leaderId = -1
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.persist()
				return
			}
			rf.nextIndex[peerId] = rf.lastIndex() + 1      // 重新从末尾同步log（未经优化，但够用）
			rf.matchIndex[peerId] = args.LastSnapshotIndex // 已同步到的位置（未经优化，但够用）
			rf.updateCommitIndex()                         // 更新commitIndex
		}
	}()
}

// 作为leader 需要持续将日志或者快照 同步到follow节点
func (rf *Raft) appendEntriesLoop() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 只有leader才向外广播心跳
			if rf.role != ROLE_LEADER {
				return
			}

			// 100ms广播1次
			now := time.Now()
			if now.Sub(rf.lastBroadcastTime) < 100*time.Millisecond {
				return
			}
			rf.lastBroadcastTime = time.Now()

			// 向所有follower发送心跳
			for peerId := 0; peerId < len(rf.peers); peerId++ {
				if peerId == rf.me {
					continue
				}

				//raft每执行一段时间都会把日志块进行快照的存储，日志太大就压缩从内存放到硬盘中
				//如果某个子节点的开始同步点在快照版本中，则需要直接同步快照版本
				if rf.nextIndex[peerId] <= rf.lastSnapshotIndex {
					rf.doSyncSnapshot(peerId) //如果小，说明快照版的还没同步，就直接同步快照版的
				} else { // 否则同步日志
					rf.doAppendEntries(peerId) //从快照之后开始同步
				}
			}
		}()
	}
}

// 每个raft节点需要持续将操作日志的命令和数据取去，让应用层进行执行
func (rf *Raft) applyLogLoop() {
	noMore := false
	for !rf.killed() {
		if noMore {
			time.Sleep(10 * time.Millisecond)
		}
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			noMore = true
			if rf.commitIndex > rf.lastApplied {
				rf.lastApplied += 1
				appliedIndex := rf.index2LogPos(rf.lastApplied)
				appliedMsg := ApplicationMsg{
					IsLog:    true,
					Operator: rf.log[appliedIndex].Command,
					LogIndex: rf.lastApplied,
					LogItem:  rf.log[appliedIndex].Term,
				}
				rf.applyCh <- appliedMsg // 引入snapshot后，这里必须在锁内投递了，否则会和snapshot的交错产生bug
				noMore = false
			}
		}()
	}
}

// 将快照放入applicationCh 通知当前应用服务执行保持日志、保存快照
func (rf *Raft) saveSnapshotToApplication() {
	var applicationMsg *ApplicationMsg

	// 同步给application层的快照
	applicationMsg = &ApplicationMsg{
		IsLog:             false,
		Snapshot:          rf.persister.GetSnapshot(),
		LastSnapshotIndex: rf.lastSnapshotIndex,
		LastSnapshotTerm:  rf.lastSnapshotTerm,
	}
	// 快照部分就已经提交给application了，所以后续applyLoop提交日志后移
	rf.lastApplied = rf.lastSnapshotIndex
	rf.applyCh <- *applicationMsg
	return
}

// 最后的index
func (rf *Raft) lastIndex() int {
	return rf.lastSnapshotIndex + len(rf.log)
}

// 最后的term
func (rf *Raft) lastTerm() (lastLogTerm int) {
	lastLogTerm = rf.lastSnapshotTerm // for snapshot
	if len(rf.log) != 0 {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	return
}

// 日志index转化成log数组下标
func (rf *Raft) index2LogPos(index int) (pos int) {
	return index - rf.lastSnapshotIndex - 1
}

// 日志是否需要压缩
func (rf *Raft) ExceedLogSize(logSize int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.persister.RaftStateSize() >= logSize {
		return true
	}
	return false
}

// 保存snapshot，截断log
func (rf *Raft) TakeSnapshot(snapshot []byte, lastSnapshotIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 已经有更大index的snapshot了
	if lastSnapshotIndex <= rf.lastSnapshotIndex {
		return
	}

	// 快照的当前元信息
	DPrintf("RafeNode[%d] TakeSnapshot begins, IsLeader[%v] snapshotLastIndex[%d] lastSnapshotIndex[%d] lastSnapshotTerm[%d]",
		rf.me, rf.leaderId == rf.me, lastSnapshotIndex, rf.lastSnapshotIndex, rf.lastSnapshotTerm)

	// 要压缩的日志长度
	compactLogLen := lastSnapshotIndex - rf.lastSnapshotIndex

	// 更新快照元信息
	rf.lastSnapshotTerm = rf.log[rf.index2LogPos(lastSnapshotIndex)].Term
	rf.lastSnapshotIndex = lastSnapshotIndex

	// 压缩日志
	afterLog := make([]LogEntry, len(rf.log)-compactLogLen)
	copy(afterLog, rf.log[compactLogLen:])
	rf.log = afterLog

	// 把snapshot和raftstate持久化
	rf.persister.SaveStateAndSnapshot(rf.compressRaftState(), snapshot)

}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplicationMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func CreateRaft(peers []*EndPoint, me int,
	persister *Persister, applyCh chan ApplicationMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.role = ROLE_FOLLOWER
	rf.leaderId = -1
	rf.votedFor = -1
	rf.lastSnapshotIndex = 0
	rf.lastSnapshotTerm = 0
	rf.lastActiveTime = time.Now()
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.uncompressRaftState(persister.GetRaftState())

	// 向application层安装快照
	rf.saveSnapshotToApplication()

	// election逻辑
	go rf.electionLoop()
	// leader逻辑
	go rf.appendEntriesLoop()
	// apply逻辑
	go rf.applyLogLoop()

	return rf
}
