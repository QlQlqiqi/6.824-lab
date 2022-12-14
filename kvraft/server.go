package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const WAITING_TIMEOUT = 10 * time.Second

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	// @see PutAppendArgs in common.go
	// "Put" or "Append" or "Get"
	Op    string
	Key   string
	Value string
	// 唯一的标识这个操作
	Id int
	// 发送的 client
	ClientId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	//cond    *sync.Cond

	maxraftstate int // snapshot if log grows this big

	// 数据库，key->value 的映射
	db map[string]string
	// 记录客户端已经 committed 过的最大的 id，clientId->Id
	// 因为 id 对每个的 client 来说都是递增的
	lastCommittedId map[int64]int
}

// 检查是否为 leader
func (kv *KVServer) CheckLeader(args *CheckLeaderArgs, reply *CheckLeaderReply) {
	term, isLeader := kv.rf.GetState()
	reply.IsLeader = isLeader
	reply.Term = term
	reply.LeaderIndex = kv.me
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		Op:       "Get",
		Key:      args.Key,
		Id:       args.Id,
		ClientId: args.ClientId,
	}
	reply.ClientId = args.ClientId
	reply.Id = args.Id
	reply.Err = ErrWrongLeader
	reply.Value = ""
	// 如果不是 leader，拒绝
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	DPrintf("server %v: Get func args: %v\n", kv.me, getArgsToString(*args))
	DPrintf("server %v: waiting to Get op: %v\n", kv.me, opToString(op))
	kv.mu.Unlock()
	defer func() {
		kv.mu.Lock()
		DPrintf("server %v: Get func reply: %v\n", kv.me, getReplyToString(*reply))
		DPrintf("server %v: end to Get op: %v\n", kv.me, opToString(op))
		kv.mu.Unlock()
	}()
	// 等待 WAITING_TIMEOUT，超时则认为是错误的 leader（可能因为这个 leader 断开连接了）
	t := time.Now()
	for time.Since(t) < WAITING_TIMEOUT {
		kv.mu.Lock()
		// 检查状态
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			reply.Err = ErrWrongLeader
			kv.mu.Unlock()
			return
		}
		// 检查该 op 是否被执行过
		lastCommittedId, ok := kv.lastCommittedId[op.ClientId]
		if !ok || lastCommittedId < op.Id {
			kv.mu.Unlock()
			continue
		}
		// no key
		//val, ok := kv.db[op.Key]
		//if !ok {
		//	reply.Err = ErrNoKey
		//	kv.mu.Unlock()
		//	return
		//}
		reply.Err = OK
		reply.Value = kv.db[op.Key]
		kv.mu.Unlock()
		return
	}
	reply.Err = ErrWrongLeader
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Op:       args.Op,
		Key:      args.Key,
		Value:    args.Value,
		Id:       args.Id,
		ClientId: args.ClientId,
	}
	reply.ClientId = args.ClientId
	reply.Id = args.Id
	// 如果不是 leader，拒绝
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	DPrintf("server %v: PutAppend func args: %v\n", kv.me, *args)
	DPrintf("server %v: waiting to PutAppend op: %v\n", kv.me, opToString(op))
	kv.mu.Unlock()
	defer func() {
		kv.mu.Lock()
		DPrintf("server %v: PutAppend func reply: %v\n",
			kv.me, *reply)
		DPrintf("server %v: end to PutAppend op: %v and reply is: %v\n",
			kv.me, opToString(op), *reply)
		kv.mu.Unlock()
	}()
	// 等待 WAITING_TIMEOUT，超时则认为是错误的 leader（可能因为这个 leader 断开连接了）
	t := time.Now()
	for time.Since(t) < WAITING_TIMEOUT {
		kv.mu.Lock()
		// 检查状态
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			reply.Err = ErrWrongLeader
			kv.mu.Unlock()
			return
		}
		// 检查该 op 是否被执行过
		lastCommittedId, ok := kv.lastCommittedId[op.ClientId]
		if !ok || lastCommittedId < op.Id {
			kv.mu.Unlock()
			continue
		}
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	reply.Err = ErrWrongLeader
}

// Kill
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.rf.Kill()
	atomic.StoreInt32(&kv.dead, 1)
	//kv.cond.Broadcast()
	//fmt.Printf("server %v: kill\n", kv.me)
	DPrintf("server %v: kill\n", kv.me)
	DPrintf("server %v: db is: %v\n", kv.me, kv.db)
	DPrintf("server %v: lastCommittedId is: %v\n", kv.me, kv.lastCommittedId)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.mu = sync.Mutex{}
	//kv.cond = sync.NewCond(&kv.mu)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.db = make(map[string]string)
	kv.lastCommittedId = make(map[int64]int)
	//kv.readPersistSnapshotL(persister.ReadSnapshot())
	//fmt.Println("StartKVServer start")

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// 接收 commit 的日志
	go kv.receiveRaftCommit(persister)

	return kv
}

// 监听来自 raft applyCh 提交的日志
func (kv *KVServer) receiveRaftCommit(persister *raft.Persister) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	tickTimer := time.NewTicker(10 * time.Millisecond)
	for !kv.killed() {
		kv.mu.Unlock()
		var msg raft.ApplyMsg
		select {
		case msg = <-kv.applyCh:
		case <-tickTimer.C:
		}
		//msg := <-kv.applyCh
		kv.mu.Lock()
		if kv.killed() {
			break
		}
		DPrintf("server %v: receive raft commit msg: %v\n", kv.me, msg)
		// 不接受 CommandValid 为 false 的日志
		//if !msg.CommandValid {
		//	continue
		//}
		// 快照
		if msg.SnapshotValid {
			DPrintf("server %v: receive raft commit snapshot\n", kv.me)
			if msg.Snapshot == nil || len(msg.Snapshot) == 0 {
				//kv.mu.Unlock()
				continue
			}
			kv.readPersistSnapshotL(msg.Snapshot)
			kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot)
			//kv.mu.Unlock()
		} else if msg.CommandValid {
			// 日志
			//DPrintf("receive raft commit: %v\n", msg)
			cmd := (msg.Command).(Op)
			DPrintf("server %v: receive raft commit entry: %v\n", kv.me, applyMsgToString(msg))
			// 如果之前已经执行过了，则不再应用到 db 中
			if kv.lastCommittedId[cmd.ClientId] >= cmd.Id {
				//kv.mu.Unlock()
				continue
			}
			if cmd.Op == "Append" {
				kv.db[cmd.Key] = kv.db[cmd.Key] + cmd.Value
				kv.lastCommittedId[cmd.ClientId] = cmd.Id
			} else if cmd.Op == "Put" {
				kv.db[cmd.Key] = cmd.Value
				kv.lastCommittedId[cmd.ClientId] = cmd.Id
			} else if cmd.Op == "Get" {
				kv.lastCommittedId[cmd.ClientId] = cmd.Id
				kv.db[cmd.Key] = kv.db[cmd.Key]
			}
			// 如果超过日志大小阈值，存储快照，并告诉 raft
			if kv.maxraftstate != -1 && kv.maxraftstate < persister.RaftStateSize() {
				DPrintf("server %v: start to compress log\n", kv.me)
				kv.persistSnapshotL(msg.CommandIndex)
				DPrintf("server %v: end to compress log\n", kv.me)
			}
			//kv.mu.Unlock()
		} else {
			//kv.cond.Wait()
		}
	}
	//fmt.Println("receiveRaftCommit over")
}

// 将 index of log 之前的 kv 状态保存成快照给 raft
func (kv *KVServer) persistSnapshotL(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.db)
	e.Encode(kv.lastCommittedId)
	kv.rf.Snapshot(index, w.Bytes())
	DPrintf("server %v: persistSnapshotL to index: %v\n", kv.me, index)
}

// 根据快照恢复 kv 状态
func (kv *KVServer) readPersistSnapshotL(data []byte) {
	//if data == nil || len(data) < 1 {
	//	kv.db = make(map[string]string)
	//	kv.lastCommittedId = make(map[int64]int)
	//	return
	//}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.db) != nil ||
		d.Decode(&kv.lastCommittedId) != nil {
		DPrintf("server %v: readPersistSnapshotL failed for data: %v\n", kv.me, data)
		//fmt.Printf("server %v: readPersistSnapshotL failed\n", kv.me)
		//fmt.Println(data)
		//fmt.Println(kv.db)
		//fmt.Println(kv.lastCommittedId)
		os.Exit(1)
	}
	DPrintf("server %v: readPersistSnapshotL success and the db is: %v\n", kv.me, kv.db)
}

// raft commit 的 msg toString
func applyMsgToString(msg raft.ApplyMsg) string {
	var str = ""
	cmd := msg.Command
	// 日志
	if msg.CommandValid {
		str += opToString(cmd)
	}
	return str
}

func opToString(cmd interface{}) string {
	return fmt.Sprintf("op: %v key: %v value: %v id: %v clientId: %v", (cmd).(Op).Op,
		(cmd).(Op).Key, (cmd).(Op).Value, (cmd).(Op).Id, (cmd).(Op).ClientId) + " "
}

func getArgsToString(cmd interface{}) string {
	return fmt.Sprintf("id: %v clientId: %v key: %v", (cmd).(GetArgs).Id,
		(cmd).(GetArgs).ClientId, (cmd).(GetArgs).Key) + " "
}
func getReplyToString(cmd interface{}) string {
	return fmt.Sprintf("id: %v clientId: %v value: %v err: %v", (cmd).(GetReply).Id,
		(cmd).(GetReply).ClientId, (cmd).(GetReply).Value, (cmd).(GetReply).Err) + " "
}
