package raft

import (
	"math/rand"
	"time"
)

const tickerInterval = 50
const electionTimeout = 500
const electionTimeoutRange = 300

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// RequestVote
// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// for all servers
	if args.Term > rf.currentTerm {
		rf.newTermL(args.Term)
	}

	upToDate := args.LastLogTerm > rf.log.lastTerm() ||
		(args.LastLogTerm == rf.log.lastTerm() && args.LastLogIndex >= rf.log.lastIndex())

	DPrintf("%v: RequestVote up-to-date %v args %v\n", rf.me, upToDate, *args)

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if upToDate && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persistL()
		rf.setElectionTimeL()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
}

// 向 peer 发送投票
// true for success, false if failed
func (rf *Raft) sendRequestVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[peer].Call("Raft.RequestVote", args, reply)
	return ok
}

// The ticker go routine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// 这里的 tick 执行频率不得小于 heartbeats 执行频率
		rf.tick()
		time.Sleep(tickerInterval * time.Millisecond)
	}
}

// ticker 每次执行的函数
func (rf *Raft) tick() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//DPrintf("%v: tick state %v\n", rf.me, rf.state)

	// 如果是 leader，重置超时选举时间
	if rf.state == leader {
		rf.setElectionTimeL()
		rf.sendAppendEntriesAllL(true)
	}
	if time.Now().After(rf.electionTime) {
		// 如果超时，则重置超时时间，并开始新的选举
		rf.setElectionTimeL()
		rf.startElectionL()
	}
	//DPrintf("%v: log is %v\n", rf.me, rf.log)
}

// 设置超时选举时间
func (rf *Raft) setElectionTimeL() {
	ran := rand.Int63() % electionTimeoutRange
	ms := electionTimeout + ran
	t := time.Now()
	t = t.Add(time.Duration(ms) * time.Millisecond)
	rf.electionTime = t
	DPrintf("%v: state %v: setElectionTimeL %vms\n", rf.me, rf.state, ms)
}

// 新的选举
func (rf *Raft) startElectionL() {
	rf.currentTerm++
	rf.state = candidate
	rf.votedFor = rf.me
	rf.persistL()

	DPrintf("%v: start election\n", rf.me)

	rf.sendRequestVotesL()
}

// 向每个 peer 投票
func (rf *Raft) sendRequestVotesL() {
	voteNum := 1
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log.lastIndex(),
		LastLogTerm:  rf.log.lastTerm(),
	}
	for peer := range rf.peers {
		if peer != rf.me {
			go rf.requestVote(peer, &args, &voteNum)
		}
	}
}

// 向 peer 投票
func (rf *Raft) requestVote(peer int, args *RequestVoteArgs, voteNum *int) {
	cnt := 0
retry:
	cnt++
	var reply RequestVoteReply
	ok := rf.sendRequestVote(peer, args, &reply)

	DPrintf("%v: RequestVote reply from %v: %v\n", rf.me, peer, reply)

	if !ok {
		if cnt < rpcRetryTimes {
			time.Sleep(rpcRetryInterval)
			goto retry
		}
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// for all servers
	if reply.Term > rf.currentTerm {
		rf.newTermL(reply.Term)
		//return
	}
	// 投票成功
	if reply.VoteGranted {
		*voteNum++
		// 在该 term 内超过半数同意
		// peers 为奇数
		if rf.currentTerm == reply.Term &&
			*voteNum > len(rf.peers)/2 {
			rf.becomeLeaderL()
			rf.sendAppendEntriesAllL(true)
		}
	}
}

// 进入新 term
func (rf *Raft) newTermL(term int) {
	rf.currentTerm = term
	rf.state = follower
	rf.votedFor = -1
	rf.persistL()
	//rf.setElectionTimeL()
	DPrintf("%v: start new term: %v\n", rf.me, rf.currentTerm)
}

// 成为 leader
func (rf *Raft) becomeLeaderL() {
	// 已成为 leader 就不用重复了
	if rf.state == leader {
		return
	}

	rf.state = leader
	rf.persistL()

	//log.Printf("\n\n%v: become leader in term %v\n\n\n", rf.me, rf.currentTerm)
	DPrintf("\n\n%v: become leader in term %v\n\n\n", rf.me, rf.currentTerm)

	// 初始化 nextIndex 和 matchIndex
	for peer := range rf.peers {
		rf.nextIndex[peer] = rf.log.lastIndex() + 1
		rf.matchIndex[peer] = rf.log.startIndex()
		//rf.matchIndex[peer] = maxInt(rf.matchIndex[peer], rf.log.startIndex())
	}
}
