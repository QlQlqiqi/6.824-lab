package raft

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
	if args.Term < rf.currentTerm || args.LastIncludedIndex <= rf.log.startIndex() {
		return
	}

	if args.Term > rf.currentTerm {
		rf.newTermL(args.Term)
	}

	// 如果上一个还没执行的快照更新，则保留
	if rf.waitingNoticeSnapshot != nil &&
		(rf.waitingNoticeSnapshot.SnapshotTerm > args.LastIncludedTerm ||
			rf.waitingNoticeSnapshot.SnapshotTerm == args.LastIncludedTerm &&
				rf.waitingNoticeSnapshot.SnapshotIndex > args.LastIncludedIndex) {
		return
	}

	DPrintf("%v: InstallSnapshot (new) from %v args %v\n", rf.me, args.LeaderId, *args)

	// 告诉 server/tester snapshot 相关信息
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
	retry:
		var reply InstallSnapshotReply
		ok := rf.sendInstallSnapshot(peer, &args, &reply)
		if !ok {
			goto retry
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		DPrintf("%v: InstallSnapshotReply from %v: %v\n", rf.me, peer, reply)
		if reply.Term > rf.currentTerm {
			rf.newTermL(reply.Term)
		} else {
			rf.nextIndex[peer] = args.LastIncludedIndex + 1
			rf.matchIndex[peer] = args.LastIncludedIndex
			rf.sendAppendEntriesL(peer)
		}
	}()
}

// 向 peer 发送 snapshot
// true for success, false if failed
func (rf *Raft) sendInstallSnapshot(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[peer].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
