package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Task types
const (
	MapTask = iota
	ReduceTask
	Waiting
	JobFinished
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		// init reply and args
		args := WorkerArgs{}
		reply := WorkerReply{}
		// request allcateTask
		if ok := call("Coordinator.AllocateTask", &args, &reply); !ok || reply.Tasktype == JobFinished {
			break
		}
		// switch to handle Tasktype
		switch reply.Tasktype {
		case MapTask:
			HandleMapTask(reply, mapf)
		case ReduceTask:
			HandleReduceTask(reply, reducef)
		case Waiting:
			time.Sleep(time.Second)
		}
	}
}

// HandleMapTask 处理一个Map任务，读取输入文件，调用map函数，生成中间键值对，并按Reduce任务数分桶存储
func HandleMapTask(reply WorkerReply, mapf func(string, string) []KeyValue) {
	// 初始化中间键值对切片
	intermediate := []KeyValue{}

	// 1. 打开输入文件
	file, err := os.Open(reply.Filename)
	if err != nil {
		log.Printf("MapTask cannot open %v: %v", reply.Filename, err)
		return // 返回而不是终止进程
	}

	// 2. 读取文件内容
	content, err := io.ReadAll(file)
	if err != nil {
		log.Printf("cannot read %v: %v", reply.Filename, err)
		file.Close()
		return
	}
	file.Close()

	// 3. 调用用户提供的map函数处理文件内容
	kva := mapf(reply.Filename, string(content))
	intermediate = append(intermediate, kva...)

	// 4. 将中间结果分桶
	buckets := make([][]KeyValue, reply.NReduce)
	for i := range buckets {
		buckets[i] = []KeyValue{}
	}

	for _, kva := range intermediate {
		bucketId := ihash(kva.Key) % reply.NReduce
		buckets[bucketId] = append(buckets[bucketId], kva)
	}

	// 5. 将分桶结果写入临时文件
	for i := range buckets {
		oname := "mr-" + strconv.Itoa(reply.MapTaskId) + "-" + strconv.Itoa(i)
		ofile, err := os.CreateTemp("", oname+"*")
		if err != nil {
			log.Printf("cannot create temp file for %v: %v", oname, err)
			continue
		}

		enc := json.NewEncoder(ofile)
		for _, kv := range buckets[i] {
			if err := enc.Encode(&kv); err != nil {
				log.Printf("cannot encode %v: %v", kv, err)
				ofile.Close()
				os.Remove(ofile.Name())
				continue
			}
		}

		ofile.Close()
		if err := os.Rename(ofile.Name(), oname); err != nil {
			log.Printf("cannot rename temp file %v to %v: %v", ofile.Name(), oname, err)
			os.Remove(ofile.Name())
			continue
		}
	}

	// 6. 通知协调者任务完成
	finishedArgs := WorkerArgs{
		MapTaskId:    reply.MapTaskId,
		ReduceTaskId: -1,
	}
	finishedReply := WorkerReply{}
	call("Coordinator.ReceiveFinishedMap", &finishedArgs, &finishedReply)
}

// HandleReduceTask 处理Reduce任务，读取中间文件，按Key分组并调用reduce函数，最终输出结果
func HandleReduceTask(reply WorkerReply, reducef func(string, []string) string) {
	// 存储所有读取的中间键值对
	intermediate := []KeyValue{}

	// 1. 读取所有Map任务生成的中间文件
	for i := 0; i < reply.NMap; i++ {
		iname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.ReduceTaskId)
		file, err := os.Open(iname)
		if err != nil {
			log.Printf("ReduceTask cannot open %v: %v", iname, err)
			continue // 继续处理其他文件
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	// 2. 按键排序中间结果
	sort.Sort(ByKey(intermediate))

	// 3. 创建临时输出文件
	oname := "mr-out" + strconv.Itoa(reply.ReduceTaskId)
	ofile, err := os.CreateTemp("", oname+"*")
	if err != nil {
		log.Printf("cannot create temp file for %v: %v", oname, err)
		return
	}

	// 4. 处理分组后的键值对
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	// 5. 原子性重命名临时文件为最终输出文件
	ofile.Close()
	if err := os.Rename(ofile.Name(), oname); err != nil {
		log.Printf("cannot rename temp file %v to %v: %v", ofile.Name(), oname, err)
		os.Remove(ofile.Name())
		return
	}

	// 6. 清理已处理的中间文件
	for i := 0; i < reply.NMap; i++ {
		iname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.ReduceTaskId)
		if err := os.Remove(iname); err != nil {
			log.Printf("cannot delete file %v: %v", iname, err)
		}
	}

	// 7. 通知协调者任务完成
	finishedArgs := WorkerArgs{
		MapTaskId:    -1,
		ReduceTaskId: reply.ReduceTaskId,
	}
	finishedReply := WorkerReply{}
	if ok := call("Coordinator.ReceiveFinishedReduce", &finishedArgs, &finishedReply); !ok {
		log.Printf("cannot notify coordinator for finished reduce task")
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
