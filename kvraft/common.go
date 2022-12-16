package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// PutAppendArgs
// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	// 唯一的标识这个操作
	Id int
	// 发送的 client
	ClientId int64
}

type PutAppendReply struct {
	Err Err
	// 唯一的标识这个操作
	// @see PutAppendArgs Id
	Id int
	// 发送的 client
	// @see PutAppendArgs ClientId
	ClientId int64
}

type GetArgs struct {
	Key string
	// 唯一的标识这个操作
	// @see PutAppendArgs Id
	Id int
	// 发送的 client
	// @see PutAppendArgs ClientId
	ClientId int64
}

type GetReply struct {
	Err   Err
	Value string
	// 唯一的标识这个操作
	// @see PutAppendArgs Id
	Id int
	// 发送的 client
	// @see PutAppendArgs ClientId
	ClientId int64
}

type CheckLeaderArgs struct{}

type CheckLeaderReply struct {
	IsLeader    bool
	Term        int
	LeaderIndex int
}
