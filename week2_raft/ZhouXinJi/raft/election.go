package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) becomeFollower(resetTime bool) {
	rf.state = Follower
	rf.voteFor = -1
	rf.recvVotes = 0
	rf.persist()
	if resetTime {
		rf.lastResetTime = time.Now()
	}
}

func (rf *Raft) becomeCandidate() {
	// log.Printf("%d becomes candidate", rf.me)
	rf.state = Candidate
	rf.currentTerm += 1
	rf.recvVotes = 1 // Votes itself
	rf.persist()
	rf.lastResetTime = time.Now()
	rf.broadcastRequestVotes()
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.voteFor = -1
	rf.recvVotes = 0
	rf.persist()

	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.getLastIndex() + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	rf.matchIndex[rf.me] = rf.getLastIndex()
	rf.lastResetTime = time.Now()
}

// The candidate Requests all peers for votes
func (rf *Raft) broadcastRequestVotes() {
	for index := range rf.peers {
		if index == rf.me {
			continue
		}

		go func(serverId int) {
			rf.mu.Lock()
			args := RequestVoteArgs{
				rf.currentTerm,
				rf.me,
				rf.getLastIndex(),
				rf.getLastTerm(),
			}
			rf.mu.Unlock()
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(serverId, &args, &reply)
			if ok {
				rf.mu.Lock()
				// It's no longer a candidate, either a follower or a leader now.
				if rf.state != Candidate || args.Term < rf.currentTerm {
					rf.mu.Unlock()
					return
				}

				if reply.Term > args.Term {
					if reply.Term > rf.currentTerm { // why check? Maybe rf.currenTerm has been updated but not args.Term
						rf.currentTerm = reply.Term
					}
					rf.becomeFollower(false)
					rf.mu.Unlock()
					return
				}

				// Same Term
				if reply.VoteGranted && reply.Term == args.Term {
					rf.recvVotes += 1
					// log.Println(rf.recvVotes, len(rf.peers))
					if rf.recvVotes >= len(rf.peers)/2+1 {
						// log.Printf("%d become leader", rf.me)
						rf.becomeLeader()
					}
					rf.mu.Unlock()
					return
				}

				rf.mu.Unlock()
			}
		}(index)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) CandidateTicker() {
	for rf.killed() == false && rf.state != Leader {
		nowTime := time.Now()
		time.Sleep(time.Duration(150+rand.Intn(150)) * time.Millisecond)
		rf.mu.Lock()
		if rf.state != Leader && rf.lastResetTime.Before(nowTime) {
			rf.becomeCandidate()
		}
		rf.mu.Unlock()
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// log.Printf("serverid:%d, candidate's id:%d, arg's Term:%d, rf's Term:%d", rf.me, args.CandidateId, args.Term, rf.currentTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.persist()
		reply.Term = args.Term
		rf.becomeFollower(true)
		if rf.isUpToDate(args) {
			rf.voteFor = args.CandidateId
			rf.persist()
			// log.Printf("%d votes for %d, arg's Term:%d, rf's Term:%d", rf.me, args.CandidateId, args.Term, rf.currentTerm)
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
		return
	}

	// args.Term == rf.currentTerm
	reply.Term = args.Term
	if rf.voteFor != -1 && rf.voteFor != args.CandidateId { // This Term has granted vote
		reply.VoteGranted = false
	} else {
		// log.Printf("term equals, %d votes for %d, arg's Term:%d, rf's Term:%d", rf.me, args.CandidateId, args.Term, rf.currentTerm)
		rf.voteFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
	}
}
