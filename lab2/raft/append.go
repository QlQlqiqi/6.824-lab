package raft

import (
	"fmt"
	"time"
)

// 同步日志时，如果日志之间 index 差距大于 maxLogDivIndex
// 则发送 snapshot
const maxLogDivIndex = 10

// 连续 maxAppendTimes 次同步日志失败，则发送 snapshot
const maxAppendTimes = 5

// AppendEntriesArgs 是 AppendEntries RPC 的参数
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []Entry
}

func (args *AppendEntriesArgs) toString() string {
	return fmt.Sprintf("Term %v LeaderId %v PrevLogIndex %v "+
		"PrevLogTerm %v LeaderCommit %v Entries %v",
		args.Term, args.LeaderId, args.PrevLogIndex,
		args.PrevLogTerm, args.LeaderCommit, args.Entries)
}

// AppendEntriesReply 是 AppendEntries RPC 的返回值
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries
// example AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// for all servers
	if args.Term > rf.currentTerm {
		rf.newTermL(args.Term)
	}

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	if rf.state == candidate {
		DPrintf("%v: convert to follower with leader %v\n", rf.me, args.LeaderId)
		rf.state = follower
		rf.persistL()
	}
	rf.setElectionTimeL()

	DPrintf("%v: (term: %v) AppendEntries %v\n", rf.me, rf.currentTerm, args.toString())

	// 可能是旧的 appendEntries （这个 bug 找了好久）
	// 这里的旧是因网络延迟等问题，导致此次收到的 appendEntries 是曾经发送的
	// 这里是用 command 是否相等来判断的，真实情况下只能另取他法
	if args.PrevLogIndex <= rf.log.lastIndex() &&
		args.PrevLogIndex >= rf.log.startIndex() &&
		args.PrevLogTerm == rf.log.entry(args.PrevLogIndex).Term {
		entries := args.Entries
		i := 0
		for ; i < len(entries) && i+args.PrevLogIndex+1 <= rf.log.lastIndex(); i++ {
			entry := rf.log.entry(i + args.PrevLogIndex + 1)
			if entries[i].Term != entry.Term || entries[i].Command != entry.Command {
				break
			}
		}
		// 曾经已经执行的旧的 appendEntries，即：此时的 log 包含 entries 中所有的日志项
		if i == len(entries) {
		} else {
			rf.log.delAndOverlap(args.PrevLogIndex+i+1, entries[i:])
		}
		//log.Printf("%v: AppendEntries log is: %v\n", rf.me, rf.log.toString())
		DPrintf("%v: AppendEntries log is: %v\n", rf.me, rf.log.toString())
		rf.persistL()
		reply.Success = true
	}

	if rf.commitIndex < args.LeaderCommit {
		// restriction 不能提交非 currentTerm 的日志项
		if args.LeaderCommit >= rf.log.startIndex() &&
			args.LeaderCommit <= rf.log.lastIndex() &&
			rf.log.entry(args.LeaderCommit).Term == rf.currentTerm {
			rf.commitIndex = minInt(rf.log.lastIndex(), args.LeaderCommit)
			//log.Printf("%v: commit change to %v %v(AppendEntries)\n",
			//	rf.me, rf.commitIndex, rf.log.entry(rf.commitIndex))
			//log.Printf("%v: log is %v\n",
			//	rf.me, rf.log.slice(maxInt(rf.log.startIndex(), rf.log.lastIndex()-10), rf.log.lastIndex()-1))
			// 每个 peer 都要发送数据（debug 这个浪费了我很长时间）
			rf.signalApplierL()
		}
	}
}

// 向 peer 发送日志
// true for success, false if failed
func (rf *Raft) sendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[peer].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 向每个 peer 发送日志（heartbeats）
func (rf *Raft) sendAppendEntriesAllL(heartbeats bool) {
	for peer := range rf.peers {
		// 自己或无新日志可发
		if peer == rf.me || (!heartbeats && rf.log.lastIndex() <= rf.nextIndex[peer]) {
			continue
		}
		rf.sendAppendEntriesL(peer)
	}
}

// 向 peer 发送日志（heartbeats）
func (rf *Raft) sendAppendEntriesL(peer int) {
	var args = AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}

	nextIndex := rf.nextIndex[peer]
	nextIndex = maxInt(nextIndex, rf.log.startIndex()+1)
	nextIndex = minInt(nextIndex, rf.log.lastIndex()+1)
	entries := rf.log.slice(nextIndex, rf.log.lastIndex()+1)
	args.PrevLogIndex = nextIndex - 1
	args.PrevLogTerm = rf.log.entry(nextIndex - 1).Term
	args.Entries = make([]Entry, len(entries))
	copy(args.Entries, entries)

	go func() {
		cnt := 0
	retry:
		cnt++
		rf.mu.Lock()
		DPrintf("%v: sendAppendEntries to %v %v\n", rf.me, peer, args.toString())
		rf.mu.Unlock()
		var reply AppendEntriesReply
		ok := rf.sendAppendEntries(peer, &args, &reply)
		if ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.processAppendReplyL(peer, &args, &reply)
		} else if cnt < rpcRetryTimes {
			time.Sleep(rpcRetryInterval)
			// 如果 rpc 失败，需要立刻重新发送
			goto retry
		}
	}()
}

// 处理向 peer 发送 AppendEntries RPC 的 reply
func (rf *Raft) processAppendReplyL(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("%v: processAppendReplyL from %v: %v\n", rf.me, peer, *reply)
	// for all servers
	if reply.Term > rf.currentTerm {
		rf.newTermL(reply.Term)
		return
	}
	// 不是本 term 的 reply
	if rf.currentTerm != args.Term {
		return
	}
	// 递减 nextIndex
	if !reply.Success {
		// 可以递减，也可以多一些
		rf.nextIndex[peer] -= 100
		if rf.nextIndex[peer] < rf.log.startIndex()+1 {
			rf.nextIndex[peer] = rf.log.startIndex() + 1
			// 需要发送 snapshot
			rf.sendSnapshotL(peer)
		}
		//rf.sendAppendEntriesL(peer)
	} else {
		// 更新 nextIndex 和 matchIndex
		nextIndex := args.PrevLogIndex + len(args.Entries) + 1
		rf.nextIndex[peer] = maxInt(rf.nextIndex[peer], nextIndex)
		rf.matchIndex[peer] = maxInt(rf.matchIndex[peer], nextIndex-1)
	}
	// 更新 commitIndex
	rf.advanceCommitL()
}

// 更新 commitIndex
func (rf *Raft) advanceCommitL() {
	if rf.state != leader {
		DPrintf("%v: advanceCommitL state error: %v\n", rf.me, rf.state)
		return
	}
	for index := maxInt(rf.commitIndex+1, rf.log.startIndex()+1); index <= rf.log.lastIndex(); index++ {
		if rf.currentTerm != rf.log.entry(index).Term {
			continue
		}
		cnt := 1
		//matchPeers := make([]int, 0, len(rf.peers))
		for peer := range rf.peers {
			if peer != rf.me && rf.matchIndex[peer] >= index {
				cnt++
				//matchPeers = append(matchPeers, peer)
			}
		}
		if cnt > len(rf.peers)/2 {
			rf.commitIndex = index
			//log.Printf("%v: match from: %v", rf.me, rf.me)
			//for _, peer := range matchPeers {
			//	log.Printf(" %v", peer)
			//}
			//log.Printf("\n")
			//log.Printf("%v: commit change to %v %v(advanceCommitL)\n", rf.me, index, rf.log.entry(index))
			//log.Printf("%v: log is %v\n",
			//	rf.me, rf.log.slice(maxInt(rf.log.startIndex(), rf.log.lastIndex()-10), rf.log.lastIndex()-1))
			DPrintf("%v: commit change to %v\n", rf.me, index)
		}
	}
	rf.signalApplierL()
}
