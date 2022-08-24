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
	"6.824/labgob"
	"bytes"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const leader = 0
const candidate = 1
const follower = 2

const rpcRetryTimes = 5

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

// Raft
// A Go object implementing a single Raft peer.
type Raft struct {
	mu        *sync.Mutex         // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	electionTime          time.Time
	state                 int
	cond                  *sync.Cond
	applyCh               chan ApplyMsg
	waitingNoticeSnapshot *ApplyMsg // 告诉 server/tester snapshot 相关信息

	// persistent state on all servers
	currentTerm int
	votedFor    int
	log         Log
	snapshot    []byte

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

// CondInstallSnapshot
// A service wants to switch to snapshot. Only do so if Raft hasn't
// had more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 不存储旧的 snapshot
	startTerm := rf.log.entry(rf.log.startIndex()).Term
	if lastIncludedTerm < startTerm ||
		(lastIncludedTerm == startTerm && lastIncludedIndex < rf.log.startIndex()) {
		return false
	}

	// 尽可能保留日志
	// 因为有可能已经执行了 i + 1 位置的日志项，
	// 然后才设置 i 位置的快照
	if lastIncludedIndex <= rf.log.lastIndex() &&
		rf.log.entry(lastIncludedIndex).Term == lastIncludedTerm {
		rf.log.cut(lastIncludedIndex)
	} else {
		rf.log = *mkLogEmpty()
		rf.log.Log[0].Term = lastIncludedTerm
		rf.log.Index = lastIncludedIndex
	}

	rf.snapshot = snapshot
	rf.persistL()
	rf.lastApplied = maxInt(rf.lastApplied, lastIncludedIndex)
	DPrintf("%v: CondInstallSnapshot log %v\n", rf.me, rf.log)
	return true
}

// Snapshot
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.log.startIndex() || index > rf.log.lastIndex() {
		return
	}

	DPrintf("%v: log is %v\n", rf.me, rf.log)
	rf.log.cut(index)
	rf.snapshot = snapshot
	rf.persistL()
	DPrintf("%v: Snapshot index %v log %v\n", rf.me, index, rf.log)
}

// Start
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != leader {
		return -1, rf.currentTerm, false
	}

	// 添加日志，并发送到其他 peer
	entry := Entry{rf.currentTerm, command}
	rf.log.append(&entry)
	rf.persistL()

	index := rf.log.lastIndex()
	DPrintf("\n\n%v: state %v: Start with index %v: %v\n\n\n", rf.me, rf.state, index, entry)

	rf.sendAppendEntriesAllL(false)

	return index, rf.currentTerm, true
}

// Kill
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Make
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		mu:           &sync.Mutex{},
		peers:        peers,
		persister:    persister,
		me:           me,
		dead:         0,
		electionTime: time.Now(),
		applyCh:      applyCh,
		currentTerm:  0,
		votedFor:     -1,
		log:          *mkLogEmpty(),
		snapshot:     make([]byte, 0),
		state:        follower,
		nextIndex:    make([]int, len(peers)),
		matchIndex:   make([]int, len(peers)),
	}
	rf.cond = sync.NewCond(rf.mu)
	rf.setElectionTimeL()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.lastApplied = rf.log.startIndex()

	DPrintf("\n\n%v: (re)boot log is: \n\n\n", rf.me, rf.log)

	// 执行 log 中指令，并写 applyCh
	go rf.applier()

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// 当 committed 时，将 log 写入 applyCh
func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() {
		if rf.lastApplied < rf.log.startIndex() {
			rf.lastApplied = rf.log.startIndex()
		}
		// 发送 snapshot
		if rf.waitingNoticeSnapshot != nil {
			applyMsg := *rf.waitingNoticeSnapshot
			rf.waitingNoticeSnapshot = nil
			//rf.log = *mkLogEmpty()
			//rf.log.Log[0].Term = applyMsg.SnapshotTerm
			//rf.log.Index = applyMsg.SnapshotIndex
			//rf.snapshot = applyMsg.Snapshot
			//rf.persistL()
			//rf.lastApplied = applyMsg.SnapshotIndex
			DPrintf("%v: apply snapshot index %v term %v to applyCh\n",
				rf.me, applyMsg.SnapshotIndex, applyMsg.SnapshotTerm)
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		} else if rf.lastApplied < rf.commitIndex &&
			rf.lastApplied < rf.log.lastIndex() &&
			rf.lastApplied >= rf.log.startIndex() {
			// 应用并向 applyCh 写入日志
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log.entry(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied,
			}
			DPrintf("%v: apply command %v with index %v and send it to applyCh\n",
				rf.me, applyMsg.Command, applyMsg.CommandIndex)
			rf.mu.Unlock()
			// 这块不能用 goroutine 去发送数据，否则会导致 goroutine 数量过多而 fail
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		} else {
			rf.cond.Wait()
		}
	}
}

// 通知 applier 执行
func (rf *Raft) signalApplierL() {
	rf.cond.Broadcast()
}

// GetState
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persistL() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.log)
	e.Encode(rf.votedFor)
	e.Encode(rf.snapshot)
	rf.persister.SaveRaftState(w.Bytes())
	DPrintf("%v: persistL\n", rf.me)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var log Log
	var snapshot []byte
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&snapshot) != nil {
		DPrintf("%v: readPersistL failed\n", rf.me)
		//os.Exit(1)
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	rf.snapshot = snapshot
	DPrintf("%v: readPersistL\n", rf.me)
}
