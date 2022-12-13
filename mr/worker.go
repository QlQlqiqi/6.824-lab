package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"lab/debug"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// reduce 阶段 worker 的数量
var nReduce int

// hashMod 保存键值对中 (key -> hash(key) % nReduce) 的映射
var hashMod map[string]int

func (a KVA) Len() int      { return len(a) }
func (a KVA) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// Less 比较
// hash 是第一优先级，key 是第二优先级
func (a KVA) Less(i, j int) bool {
	h1, h2 := hashMod[a[i].Key], hashMod[a[j].Key]
	if h1 == h2 {
		return a[i].Key < a[j].Key
	}
	return h1 < h2
}

// ByKey for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// lockFile 用于 reduce 阶段对文件进行加锁
const lockFile = "mr-out-lock"

//
// use hash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func hash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// noticeTaskFinished 通过 rpc 告诉 master 任务完成
func noticeTaskFinished(noticeArg *NoticeTaskFinishArg) *NoticeTaskFinishReply {
	var noticeReply *NoticeTaskFinishReply
	if err := call("Master.CallNoticeTaskFinished", noticeArg, &noticeReply); err != nil {
		return nil
	}
	return noticeReply
}

// doMapTask 执行 map 任务，并返回结果
func doMapTask(mapf func(string, string) []KeyValue, task *Task) []KeyValue {
	var kva []KeyValue
	for _, filename := range task.Filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		tmpKva := mapf(filename, string(content))
		kva = append(kva, tmpKva...)
	}
	return kva
}

// doReduceTask 执行 reduce 任务，并返回结果
func doReduceTask(reducef func(string, []string) string, task *Task) string {
	filename := "mr-out-" + string(task.Id+'0')
	fpw, err := os.Create(filename)
	if err != nil {
		log.Printf("create file failed in func doReduceTask.  %v\n", err)
		return ""
	}
	defer fpw.Close()

	// 中间键值对
	var kva []KeyValue
	for _, filename := range task.Filenames {
		fpr, err := os.Open(filename)
		if err != nil {
			log.Printf("open file failed in func doReduceTask. %v\n", err)
			continue
		}
		kva = append(kva, readFromFile(fpr)...)
		fpr.Close()
	}

	sort.Sort(ByKey(kva))

	len := len(kva)
	for i, j := 0, 0; i < len; i = j {
		j = i + 1
		for j < len && kva[j].Key == kva[i].Key {
			j++
		}

		var values = make([]string, 0, j-i)
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}

		output := reducef(kva[i].Key, values)
		fmt.Fprintf(fpw, "%v %v\n", kva[i].Key, output)
		//debug.DBPrintf("key: %v, values: %v, output: %v\n", kva[i].Key, values, output)
	}
	return filename
}

// 通过 rpc 获取执行的任务 id 和任务本身，如果失败则返回 nil
func getTask() (int, *Task) {
	var taskReply *GetTaskReply
	if err := call("Master.CallGetTask", GetTaskArg{}, &taskReply); err != nil {
		//debug.DBPrintf("getTask failed\n")
		return 0, nil
	}
	return taskReply.Id, taskReply.Task
}

// 通过 rpc 获取 reduce 并行度
func getNReduce() int {
	var nReduce int
	if err := call("Master.CallGetNReduce", 0, &nReduce); err != nil {
		debug.DBPrintf("getNReduce failed\n")
		return 0
	}
	return nReduce
}

// readFromFile 从 fpr 读出反序列化后的结果
func readFromFile(fpr *os.File) []KeyValue {
	var kva []KeyValue
	dec := json.NewDecoder(fpr)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	return kva
}

// writeToFile 将 kva 序列化后写入 fpw
func writeToFile(fpw *os.File, kva []KeyValue) {
	enc := json.NewEncoder(fpw)
	for _, kv := range kva {
		enc.Encode(&kv)
	}
}

// Worker
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.

	nReduce = getNReduce()
	// 获取任务，如果阶段为 Done 则退出，否则执行
	for {
	retry:
		time.Sleep(1 * time.Second)

		replyId, task := getTask()
		if task == nil {
			goto retry
		}
		debug.DBPrintf("worker get task: %v\n", *task)

		var allFilenames []string
		switch task.Phase {
		case Mapping:
			kva := doMapTask(mapf, task)
			len := len(kva)
			hashMod = make(map[string]int, len)
			for _, kv := range kva {
				hashMod[kv.Key] = hash(kv.Key) % nReduce
			}
			sort.Sort(KVA(kva))

			// 根据 hash 将 kva 按 key 分区，并保存在 `mr-${task.Id}-${hash(kva[i].Key) % nReduce}`
			for i, j := 0, 0; i < len; i = j {
				j = i + 1
				for j < len && hashMod[kva[i].Key] == hashMod[kva[j].Key] {
					j++
				}
				// 该分区
				partitionKva := kva[i:j]
				// 写入文件
				filename := "mr-" + strconv.Itoa(task.Id) + "-" + strconv.Itoa(hashMod[kva[i].Key])
				//debug.DBPrintf("start to write file.  %v\n", filename)
				allFilenames = append(allFilenames, filename)
				fpw, _ := os.Create(filename)
				writeToFile(fpw, partitionKva)
				fpw.Close()
				//debug.DBPrintf("end to write file.  %v\n", filename)
			}
			debug.DBPrintf("a map of worker has done.    %v\n", task.Id)
		case Reducing:
			filename := doReduceTask(reducef, task)
			allFilenames = append(allFilenames, filename)
			debug.DBPrintf("a reduce of worker has done.    %v\n", allFilenames)
		default:
			break
		}
		// 告诉 master 任务完成
		var noticeArg = &NoticeTaskFinishArg{
			Id:   replyId,
			Task: task,
			Result: &Result{
				Id:        task.Id,
				Phase:     task.Phase,
				Filenames: allFilenames,
			},
		}
		noticeTaskFinished(noticeArg)
	}

}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) error {
	//c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	return err
}
