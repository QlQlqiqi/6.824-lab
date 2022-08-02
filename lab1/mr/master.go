package mr

import (
	"errors"
	"lab/debug"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "net/rpc"
import "net/http"

// Mapping 表示正在进行 map 阶段
const Mapping = 0

// Reducing 表示正在进行 reduce 阶段
const Reducing = 1

// Done 表示已经结束
const Done = 2

// Timeout 是任务执行的超时时间
const Timeout = 30 * time.Second

type KVA []KeyValue

// Task 为任务类型
type Task struct {
	// 任务的唯一 id
	Id int
	// 任务的阶段，Mapping 或者 Reducing
	Phase int
	// 任务内容，这里代表文件名
	Filenames []string
}

// Tasks 为任务集类型
type Tasks map[int]*Task

// Result 为结果类型，这里和任务类型相同
type Result Task

// Results 为结果集类型
type Results map[int]*Result

type Master struct {
	// id
	id int
	// 锁
	mu sync.Mutex
	// 当前阶段
	phase int
	// 当前阶段的待做的任务 id 到任务本身的映射
	waitingTasks Tasks
	// 当前阶段的请求 id 到正在做的任务的映射
	workingTasks Tasks
	// 当前阶段的待做的任务 id 到结果本身的映射
	results Results
	// reduce 的并行度
	nReduce int
}

// assignTask 返回 tasks 中的一个任务指针
// 如果 tasks 为空，则返回 nil
// 注：这里的复制的浅拷贝
func (m *Master) assignTask() *Task {
	var task *Task
	for _, tmpTask := range m.waitingTasks {
		task = tmpTask
		break
	}
	return task
}

// tryToNextPhase 尝试进入下一阶段，并将结果转化到待做中，即：
// reduce 阶段：将结果合并，根据
func (m *Master) tryToNextPhase() bool {
	if len(m.waitingTasks) != 0 || len(m.workingTasks) != 0 || m.phase == Done {
		return false
	}

	transmit := false
	if m.phase == Mapping {
		transmit = true
		m.phase = Reducing
	} else {
		m.phase = Done
	}

	for _, result := range m.results {
		// 结果 -> 待做
		if transmit {
			// 将文件名分别保存在待做任务中
			for _, filename := range result.Filenames {
				key, _ := strconv.Atoi(strings.Split(filename, "-")[2])
				if m.waitingTasks[key] == nil {
					m.waitingTasks[key] = &Task{
						Id:        key,
						Phase:     Reducing,
						Filenames: []string{filename},
					}
				} else {
					m.waitingTasks[key].Filenames = append(m.waitingTasks[key].Filenames, filename)
				}
			}
		}
		// ×结果
		delete(m.results, result.Id)
	}
	return true
}

// CallGetTask 获取任务
// worker 通过 rpc 调用 GetTask，获取当前阶段的任务
// 且该任务只有 Timeout 的处理时间，超时则放弃（正在做 -> 待做）
func (m *Master) CallGetTask(_ GetTaskArg, reply *GetTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 获取任务
	task := m.assignTask()
	if task == nil {
		return errors.New("no task")
	}

	// 待做 -> 正在做
	m.id++
	replyId := m.id
	delete(m.waitingTasks, task.Id)
	m.workingTasks[replyId] = task

	// 一定时间内如果任务没有被完成，则换一个 worker 执行
	time.AfterFunc(Timeout, func() {
		m.mu.Lock()
		defer m.mu.Unlock()

		// 如果 master 阶段没有变化，且任务未被处理，则取消处理该任务
		// 正在做 -> 待做
		if m.phase == task.Phase && m.workingTasks[replyId] != nil {
			delete(m.workingTasks, replyId)
			m.waitingTasks[task.Id] = task
			debug.DBPrintf("task timeout. %v\n", task.Id)
		}
	})

	*reply = GetTaskReply{
		Id:   replyId,
		Task: task,
	}
	return nil
}

// CallNoticeTaskFinished 记录 task 已经完成
// 如果所有任务均完成，则进入下一阶段
func (m *Master) CallNoticeTaskFinished(args NoticeTaskFinishArg, _ *NoticeTaskFinishReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	task, result := args.Task, args.Result

	// master 必须是 Map 或 Reduce 阶段
	// 且任务必须是当前阶段的任务
	// 且任务存在
	if m.phase == Done || m.phase != task.Phase || m.workingTasks[args.Id] == nil {
		return nil
	}

	// 正在做 -> 已经做完
	delete(m.workingTasks, args.Id)
	m.results[task.Id] = result

	// 尝试进入下一阶段
	m.tryToNextPhase()

	return nil
}

// CallGetNReduce 是返回 reduce 阶段并行度
func (m *Master) CallGetNReduce(_ int, reply *int) error {
	*reply = m.nReduce
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.phase == Done {
		return true
	}

	return false
}

// MakeMaster create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	var m = &Master{
		mu:           sync.Mutex{},
		phase:        Mapping,
		waitingTasks: Tasks{},
		workingTasks: Tasks{},
		results:      Results{},
		nReduce:      nReduce,
	}
	tasks := make(Tasks, len(files))
	for idx, file := range files {
		tasks[idx] = &Task{
			Id:        idx,
			Phase:     Mapping,
			Filenames: []string{file},
		}
	}
	m.waitingTasks = tasks

	// 如果输入为空，则直接进入 Done 阶段
	if len(m.waitingTasks) == 0 {
		m.phase = Done
	}

	m.server()
	return m
}
