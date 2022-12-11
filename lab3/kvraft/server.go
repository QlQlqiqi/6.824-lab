package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const WAITING_TIMEOUT = 4 * time.Second

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

	maxraftstate int // snapshot if log grows this big

	// 数据库，key->value 的映射
	db map[string]string
	// 记录客户端已经 committed 过的最大的 id，clientId->Id
	// 因为 id 对每个的 client 来说都是递增的
	lastCommittedId map[int64]int
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
	reply.LeaderIdx = -1
	reply.Err = ErrWrongLeader
	reply.Value = ""
	// 如果不是 leader，拒绝
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("server: Get func args: %v\n", getArgsToString(*args))
	DPrintf("server: waiting to Get op: %v\n", opToString(op))
	defer func() {
		DPrintf("server: Get func reply: %v\n", getReplyToString(*reply))
		DPrintf("server: end to Get op: %v\n", opToString(op))
	}()
	// 等待 WAITING_TIMEOUT，超时则认为是错误的 leader（可能因为这个 leader 断开连接了）
	t := time.Now()
	for time.Since(t) < WAITING_TIMEOUT {
		time.Sleep(10 * time.Millisecond)
		// 检查状态
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}
		kv.mu.Lock()
		// 检查该 op 是否被执行过
		lastCommittedId, ok := kv.lastCommittedId[op.ClientId]
		if !ok || lastCommittedId < op.Id {
			DPrintf("==========")
			kv.mu.Unlock()
			continue
		}
		// no key
		val, ok := kv.db[op.Key]
		if !ok {
			reply.Err = ErrNoKey
			kv.mu.Unlock()
			return
		}
		reply.Err = OK
		reply.Value = val
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
	reply.LeaderIdx = -1
	// 如果不是 leader，拒绝
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("server: waiting to PutAppend op: %v\n", opToString(op))
	defer func() {
		DPrintf("server: end to PutAppend op: %v\n", opToString(op))
	}()
	// 等待 WAITING_TIMEOUT，超时则认为是错误的 leader（可能因为这个 leader 断开连接了）
	t := time.Now()
	for time.Since(t) < WAITING_TIMEOUT {
		time.Sleep(10 * time.Millisecond)
		// 检查状态
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}
		kv.mu.Lock()
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
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.

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
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.db = make(map[string]string)
	kv.lastCommittedId = make(map[int64]int)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// 接收 commit 的日志
	go kv.receiveRaftCommit()

	return kv
}

// 监听来自 raft applyCh 提交的日志
func (kv *KVServer) receiveRaftCommit() {
	for msg := range kv.applyCh {
		// 不接受 CommandValid 为 false 的日志
		if !msg.CommandValid {
			continue
		}
		// 只接受日志
		if msg.Type != 1 {
			continue
		}
		//DPrintf("receive raft commit: %v\n", msg)
		cmd := (msg.Command).(Op)
		DPrintf("server: receive raft commit entry: %v\n", applyMsgToString(msg))
		kv.mu.Lock()
		// 如果之前已经执行过了，则不再应用到 db 中
		if kv.lastCommittedId[cmd.ClientId] >= cmd.Id {
			kv.mu.Unlock()
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
		}
		kv.mu.Unlock()
	}
}

// raft commit 的 msg toString
func applyMsgToString(msg raft.ApplyMsg) string {
	var str = ""
	cmd := msg.Command
	// 日志
	if msg.Type == 1 {
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
	return fmt.Sprintf("id: %v clientId: %v value: %v leaderIdx: %v err: %v", (cmd).(GetReply).Id,
		(cmd).(GetReply).ClientId, (cmd).(GetReply).Value, (cmd).(GetReply).LeaderIdx, (cmd).(GetReply).Err) + " "
}
