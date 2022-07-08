package raft

import (
	"time"
)

// Append Entries handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.MoveBackIndex = -1
		return
	}
	// log.Printf("args's Term:%d, rf's Term:%d", args.Term, rf.currentTerm)
	// log.Printf("recv hearbeats from %d, so %d becomes follower from %d\n", args.LeaderID, rf.me, rf.state)

	// args.Term >= rf.currentTerm
	rf.currentTerm = args.Term // Attention!!!Order is important!!!!
	rf.becomeFollower(true)    //

	if args.PrevLogIndex > rf.getLastIndex() {
		reply.Term = args.Term
		reply.Success = false
		reply.MoveBackIndex = rf.getLastIndex()
		return
	}

	tmpTerm := rf.log[args.PrevLogIndex].Term
	if tmpTerm != args.PrevLogTerm {
		i := args.PrevLogIndex - 1
		for ; i >= 0 && rf.log[i].Term == tmpTerm; i-- {

		}
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.MoveBackIndex = i
		return
	}

	rf.log = rf.log[:args.PrevLogIndex+1]
	rf.log = append(rf.log, args.Entries...)
	rf.persist()
	reply.Term = rf.currentTerm
	reply.Success = true
	reply.MoveBackIndex = -1

	if args.LeaderCommit > rf.commitIndex {
		lastIndex := rf.getLastIndex()
		if args.LeaderCommit > lastIndex {
			rf.commitIndex = lastIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		// log.Println(rf.commitIndex)
	}
}

// only leader uses
func (rf *Raft) leaderAppendEntries() {
	for index := range rf.peers {
		if index == rf.me {
			continue
		}

		go func(server int) {
			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}
			prevLogIndex := rf.nextIndex[server] - 1
			prevLogTerm := rf.log[prevLogIndex].Term
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderID:     rf.me,
				Entries:      []LogEntry{},
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				LeaderCommit: rf.commitIndex,
			}
			if rf.getLastIndex() > prevLogIndex {
				args.Entries = append(args.Entries, rf.log[prevLogIndex+1:]...)
			}
			rf.mu.Unlock()
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(server, &args, &reply)
			if ok {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.persist()
					rf.becomeFollower(true)
					rf.mu.Unlock()
					return
				}

				if reply.Success {
					rf.matchIndex[server] = prevLogIndex + len(args.Entries)
					rf.nextIndex[server] = prevLogIndex + len(args.Entries) + 1
					rf.updateCommitIndex()
				} else {
					if reply.MoveBackIndex != -1 {
						rf.nextIndex[server] = reply.MoveBackIndex + 1
					}

				}
				rf.mu.Unlock()
			}
		}(index)
	}
}

// Update Leader's CommitIndex
func (rf *Raft) updateCommitIndex() {
	for N := rf.getLastIndex(); N >= rf.commitIndex; N-- {
		count := 1
		if rf.log[N].Term == rf.currentTerm {
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me && rf.matchIndex[i] >= N {
					count++
				}
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = N
			break
		}
	}
}

// Append entries RPC ticker
func (rf *Raft) LeaderAppendEntriesTicker() {
	for rf.killed() == false {
		time.Sleep(40 * time.Millisecond)
		rf.mu.Lock()
		if rf.state == Leader {
			rf.leaderAppendEntries()
		}
		rf.mu.Unlock()
	}
}

// put the committed entry to apply on the state machine
func (rf *Raft) ApplyToStateMachineTicker() {
	for rf.killed() == false {
		time.Sleep(30 * time.Millisecond)
		rf.mu.Lock()

		// log.Println(rf.lastApplied, rf.commitIndex, rf.getLastIndex())

		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			continue
		}

		Messages := make([]ApplyMsg, 0)
		for rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.getLastIndex() {

			rf.lastApplied += 1
			Messages = append(Messages, ApplyMsg{
				CommandValid:  true,
				SnapshotValid: false,
				CommandIndex:  rf.lastApplied,
				Command:       rf.log[rf.lastApplied].Command,
			})
		}
		rf.mu.Unlock()

		for _, messages := range Messages {
			rf.applyCh <- messages
		}
	}

}
