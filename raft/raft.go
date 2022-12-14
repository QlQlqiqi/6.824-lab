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
	"encoding/gob"

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
const rpcRetryInterval = 50 * time.Millisecond

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

	electionTime time.Time
	state        int
	cond         *sync.Cond
	applyCh      chan ApplyMsg

	// ?????? server/tester snapshot ????????????
	waitingNoticeSnapshot *ApplyMsg
	//waitingSnapshot       []byte
	//waitingIndex          int
	//waitingTerm           int

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

	// ??????????????? snapshot
	startTerm := rf.log.entry(rf.log.startIndex()).Term
	if lastIncludedTerm < startTerm ||
		(lastIncludedTerm == startTerm && lastIncludedIndex < rf.log.startIndex()) {
		return false
	}
	defer rf.persistL()
	defer rf.persistSnapshotL()

	// ?????????????????????
	// ?????????????????????????????? i + 1 ?????????????????????
	// ??????????????? i ???????????????
	if lastIncludedIndex <= rf.log.lastIndex() &&
		lastIncludedIndex >= rf.log.startIndex() &&
		rf.log.entry(lastIncludedIndex).Term == lastIncludedTerm {
		rf.log.cut(lastIncludedIndex)
	} else {
		rf.log.snapshot(lastIncludedIndex, lastIncludedTerm)
	}

	rf.snapshot = snapshot
	rf.lastApplied = maxInt(rf.lastApplied, lastIncludedIndex)
	rf.commitIndex = maxInt(rf.commitIndex, lastIncludedIndex)
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
	defer rf.persistL()
	defer rf.persistSnapshotL()

	DPrintf("%v: log is %v\n", rf.me, rf.log)
	rf.log.cut(index)
	rf.snapshot = snapshot
	rf.lastApplied = maxInt(rf.lastApplied, index)
	rf.commitIndex = maxInt(rf.commitIndex, index)
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

	// ????????????????????????????????? peer
	entry := Entry{rf.currentTerm, command}
	rf.log.append(&entry)
	rf.persistL()

	index := rf.log.lastIndex()
	DPrintf("%v: state %v: Start with index %v: %v\n", rf.me, rf.state, index, entry)
	//log.Printf("%v: state %v: Start with index %v: %v\n", rf.me, rf.state, index, entry)

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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	atomic.StoreInt32(&rf.dead, 1)
	rf.signalApplierL()
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
		mu:          &sync.Mutex{},
		peers:       peers,
		persister:   persister,
		me:          me,
		dead:        0,
		applyCh:     applyCh,
		currentTerm: 0,
		votedFor:    -1,
		log:         *mkLogEmpty(),
		snapshot:    make([]byte, 0),
		state:       follower,
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
	}
	rf.cond = sync.NewCond(rf.mu)
	rf.setElectionTimeL()

	// initialize from state persisted before a crash
	rf.readPersistL(persister.ReadRaftState())
	rf.readPersistSnapshotL(persister.ReadSnapshot())

	rf.lastApplied = rf.log.startIndex()
	rf.commitIndex = rf.log.startIndex()

	DPrintf("\n\n%v: (re)boot log is: \n\n\n", rf.me, rf.log)

	// ?????? log ?????????????????? applyCh
	go rf.applier()

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// ??? committed ????????? log ?????? applyCh
func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastApplied = maxInt(rf.lastApplied, rf.log.startIndex())

	for !rf.killed() {
		// ?????? snapshot
		if rf.waitingNoticeSnapshot != nil {
			applyMsg := *rf.waitingNoticeSnapshot
			rf.waitingNoticeSnapshot = nil
			DPrintf("%v: apply snapshot index %v term %v to applyCh\n",
				rf.me, applyMsg.SnapshotIndex, applyMsg.SnapshotTerm)
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		} else if rf.lastApplied < rf.commitIndex &&
			rf.lastApplied < rf.log.lastIndex() &&
			rf.lastApplied >= rf.log.startIndex() {
			// ???????????? applyCh ????????????
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log.entry(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied,
			}
			//log.Printf("%v: apply command %v with index %v and send it to applyCh\n",
			//	rf.me, applyMsg.Command, applyMsg.CommandIndex)
			DPrintf("%v: apply command %v with index %v and send it to applyCh\n",
				rf.me, applyMsg.Command, applyMsg.CommandIndex)
			rf.mu.Unlock()
			// ??????????????? goroutine ????????????????????????????????? goroutine ??????????????? fail
			rf.applyCh <- applyMsg
			//fmt.Printf("apply: %v\n", applyMsg)
			rf.mu.Lock()
		} else {
			rf.cond.Wait()
		}
	}
}

// ?????? applier ??????
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
	//state := w.Bytes()
	//rf.persister.SaveStateAndSnapshot(state, rf.snapshot)
	rf.persister.SaveRaftState(w.Bytes())
	DPrintf("%v: persistL success\n", rf.me)
}

// restore previously persisted state.
func (rf *Raft) readPersistL(state []byte) {
	DPrintf("%v: start to readPersistL\n", rf.me)
	if state == nil || len(state) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(state)
	d := labgob.NewDecoder(r)
	if d.Decode(&rf.currentTerm) != nil ||
		d.Decode(&rf.log) != nil ||
		d.Decode(&rf.votedFor) != nil {
		DPrintf("%v: readPersistL failed\n", rf.me)
	}
	//rf.snapshot = snapshot
	DPrintf("%v: readPersistL success\n", rf.me)
}

func (rf *Raft) persistSnapshotL() {
	DPrintf("%v: start to persistSnapshotL\n", rf.me)
	if rf.snapshot == nil || len(rf.snapshot) == 0 { // bootstrap without any state?
		return
	}
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.snapshot)
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), w.Bytes())
	DPrintf("%v: persistSnapshotL success\n", rf.me)
}

func (rf *Raft) readPersistSnapshotL(data []byte) {
	DPrintf("%v: start to readPersistSnapshotL\n", rf.me)
	if data == nil || len(data) == 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	var snapshot []byte
	if d.Decode(&snapshot) != nil {
		DPrintf("%v: readPersistSnapshotL failed\n", rf.me)
	}
	if len(snapshot) == 0 {
		return
	}
	rf.snapshot = snapshot
	rf.lastApplied = rf.log.startIndex()
	rf.commitIndex = rf.log.startIndex()
	DPrintf("%v: readPersistSnapshotL success\n", rf.me)
	// ?????? server/tester snapshot ????????????
	rf.waitingNoticeSnapshot = &ApplyMsg{
		SnapshotValid: true,
		SnapshotIndex: rf.log.startIndex(),
		SnapshotTerm:  rf.log.entry(rf.log.startIndex()).Term,
		Snapshot:      snapshot,
	}
	rf.signalApplierL()
}
