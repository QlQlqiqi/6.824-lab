package raft

import (
	"time"
)

// InstallSnapshotArgs 是 InstallSnapshot RPC 参数类型
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

// InstallSnapshotReply 是 InstallSnapshot RPC 返回类型
type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = maxInt(rf.currentTerm, args.Term)
	if args.Term < rf.currentTerm || args.LastIncludedIndex <= rf.log.startIndex() || rf.state == leader {
		return
	}

	if args.Term > rf.currentTerm {
		rf.newTermL(args.Term)
	}

	// 如果上一个还没执行的快照更新，则保留
	//if rf.waitingNoticeSnapshot != nil &&
	//	(rf.waitingNoticeSnapshot.SnapshotTerm > args.LastIncludedTerm ||
	//		rf.waitingNoticeSnapshot.SnapshotTerm == args.LastIncludedTerm &&
	//			rf.waitingNoticeSnapshot.SnapshotIndex > args.LastIncludedIndex) {
	//	return
	//}
	defer rf.persistL()
	defer rf.persistSnapshotL()

	DPrintf("%v: InstallSnapshot (new) from %v args %v\n", rf.me, args.LeaderId, *args)

	rf.snapshot = args.Data
	if args.LastIncludedIndex >= rf.log.startIndex() &&
		args.LastIncludedIndex <= rf.log.lastIndex() &&
		rf.log.entry(args.LastIncludedIndex).Term == args.LastIncludedTerm {
		// 保留 args.LastIncludedIndex 之后的日志
		rf.log.cut(args.LastIncludedIndex)
	} else {
		// 删除全部的日志
		rf.log.snapshot(args.LastIncludedIndex, args.LastIncludedTerm)
	}
	rf.lastApplied = maxInt(rf.lastApplied, args.LastIncludedIndex)
	rf.commitIndex = maxInt(rf.commitIndex, args.LastIncludedIndex)
	rf.waitingNoticeSnapshot = &ApplyMsg{
		SnapshotValid: true,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
		Snapshot:      args.Data,
	}
	rf.signalApplierL()
}

// 向 peer 发送快照
func (rf *Raft) sendSnapshotL(peer int) {
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.log.startIndex(),
		LastIncludedTerm:  rf.log.entry(rf.log.startIndex()).Term,
		Data:              rf.snapshot,
	}
	DPrintf("%v: sendSnapshotL to %v args %v\n", rf.me, peer, args)

	go func() {
		cnt := 0
	retry:
		cnt++
		var reply InstallSnapshotReply
		rf.mu.Lock()
		if rf.state != leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		ok := rf.sendInstallSnapshot(peer, &args, &reply)
		if !ok {
			if cnt < rpcRetryTimes {
				time.Sleep(rpcRetryInterval)
				// 如果 rpc 失败，需要立刻重新发送
				goto retry
			}
			return
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		DPrintf("%v: InstallSnapshotReply from %v: %v\n", rf.me, peer, reply)
		if reply.Term > rf.currentTerm {
			rf.newTermL(reply.Term)
		} else if reply.Term == rf.currentTerm && rf.state == leader {
			rf.nextIndex[peer] = args.LastIncludedIndex + 1
			rf.matchIndex[peer] = args.LastIncludedIndex
			rf.sendAppendEntriesL(peer, false)
		}
	}()
}

// 向 peer 发送 snapshot
// true for success, false if failed
func (rf *Raft) sendInstallSnapshot(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[peer].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
