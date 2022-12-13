package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

//type ExampleArgs struct {
//	X int
//}
//
//type ExampleReply struct {
//	Y int
//}

//// MapArg 是 worker 请求 Map 任务的参数类型
//type MapArg struct {}
//
//// MapReply 是 worker 请求 Map 任务的返回类型
//type MapReply Task
//
//// ReduceArg 是 worker 请求 reduce 任务的参数类型
//type ReduceArg MapArg
//
//// ReduceReply 是 worker 请求 reduce 任务的返回类型
//type ReduceReply MapReply

// GetTaskArg 是调用 CallGetTask 时的参数类型
type GetTaskArg struct{}

// GetTaskReply 是调用 CallGetTask 时的结果类型
type GetTaskReply struct {
	// id
	Id int
	// 任务
	Task *Task
}

// NoticeTaskFinishArg 是 worker 通知 master 任务完成的参数类型
type NoticeTaskFinishArg struct {
	// id 是 getTaskReply 里对应的 id
	Id int
	// 任务
	Task *Task
	// 结果
	Result *Result
}

// NoticeTaskFinishReply 是 worker 通知 master 任务完成的结果类型
type NoticeTaskFinishReply struct{}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
