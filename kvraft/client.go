package kvraft

import (
	"6.824/labrpc"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// 当前 leader 在 servers 中的索引，-1 代表不清楚谁是 leader
	leaderIdx int
	//mu        *sync.Mutex
	//cond      *sync.Cond
	clientId int64
	// 请求 id
	opId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		servers:   servers,
		leaderIdx: -1,
		//mu:        &sync.Mutex{},
		clientId: nrand(),
		opId:     1,
	}
	//ck.cond = sync.NewCond(ck.mu)

	return ck
}

// Get
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// 先 Append 的一个，然后就能 get 了
	//ck.PutAppend(key, "", "Append")
	leaderIdx := ck.leaderIdx
	// 不清楚当前 leader 是谁，则随机挑选一个
	if ck.leaderIdx == -1 {
		leaderIdx = ck.getServerIdx()
	}
	ck.opId++
	args := &GetArgs{
		Id:       ck.opId,
		ClientId: ck.clientId,
		Key:      key,
	}
sendOp:
	reply := &GetReply{}
	DPrintf("client: Get to server %v\n", leaderIdx)
	ok := ck.sendGet(leaderIdx, args, reply)
	// 错误的回复
	if reply.Id != args.Id || reply.ClientId != args.ClientId {
		goto sendOp
	}
	DPrintf("Get:  func reply: %v\n", getReplyToString(*reply))
	if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
		return reply.Value
	}
	// 请求失败，对方不是 leader，重新请求
	if reply.Err == ErrWrongLeader {
		leaderIdx = ck.getServerIdx()
	}
	goto sendOp
}

// 向 servers[idx] 发送 Get 请求
func (ck *Clerk) sendGet(idx int, args *GetArgs, reply *GetReply) bool {
	return ck.servers[idx].Call("KVServer.Get", args, reply)
}

// PutAppend
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	leaderIdx := ck.leaderIdx
	// 不清楚当前 leader 是谁，则随机挑选一个
	if ck.leaderIdx == -1 {
		leaderIdx = ck.getServerIdx()
	}
	ck.opId++
	args := &PutAppendArgs{
		Id:       ck.opId,
		ClientId: ck.clientId,
		Key:      key,
		Value:    value,
		Op:       op,
	}
sendOp:
	reply := &PutAppendReply{}
	DPrintf("client: PutAppend to server %v\n", leaderIdx)
	ok := ck.sendPutAppend(leaderIdx, args, reply)
	// 错误的回复
	if reply.Id != args.Id || reply.ClientId != args.ClientId {
		goto sendOp
	}
	// 请求成功
	if ok && reply.Err == OK {
		ck.leaderIdx = leaderIdx
		return
	}
	// 请求失败，对方不是 leader，重新请求
	if reply.Err == ErrWrongLeader {
		leaderIdx = ck.getServerIdx()
	}
	goto sendOp
	// 不存在该 key，更换 op（因为 append 的 key 不存在）
	// 事实上不存在该情况，server 默认处理了
	//if reply.Err == ErrNoKey {
	//	// 更换 append 为 put
	//	args.Op = "Put"
	//	goto sendOp
	//}
}

// 向 servers[idx] 发送 PutAppend 请求
func (ck *Clerk) sendPutAppend(idx int, args *PutAppendArgs, reply *PutAppendReply) bool {
	return ck.servers[idx].Call("KVServer.PutAppend", args, reply)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

// 获取一个 server index
func (ck *Clerk) getServerIdx() int {
retry:
	DPrintf("Checking .. .. OneLeader\n")
	args := &CheckLeaderArgs{}
	reply := make([]CheckLeaderReply, len(ck.servers))
	//leaders := make([]int, len(ck.servers))
	leaders := make(map[int][]int)
	for si := 0; si < len(ck.servers); si++ {
		ok := ck.servers[si].Call("KVServer.CheckLeader", args, &reply[si])
		if ok && reply[si].IsLeader {
			t := reply[si].Term
			leaders[t] = append(leaders[t], si)
		}
	}
	lastTermWithLeader := -1
	for t, ls := range leaders {
		if len(ls) > 1 {
			DPrintf("Waring: There are two leader in term %d\n", t)
			goto retry
		}

		if t > lastTermWithLeader {
			lastTermWithLeader = t
		}
	}

	if len(leaders) != 0 {
		return leaders[lastTermWithLeader][0]
	}
	goto retry
}
