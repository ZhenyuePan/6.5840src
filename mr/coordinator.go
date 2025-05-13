package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// 定义任务状态常量，提高可读性
const (
	TaskStatusUnassigned = iota
	TaskStatusAssigned
	TaskStatusCompleted
)

// 定义任务类型常量
const (
	TaskTypeMap = iota
	TaskTypeReduce
	TaskTypeWait
	TaskTypeDone
)

type Coordinator struct {
	// Your definitions here.
	nReduce        int // number of reduce tasks
	nMap           int // number of map tasks
	files          []string
	mapFinished    int
	mapTaskLog     []int
	reduceFinished int
	reduceTaskLog  []int
	mu             *sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) ReceiveFinishedMap(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// 更新任务状态为已完成
	if args.MapTaskId >= 0 && args.MapTaskId < len(c.mapTaskLog) {
		c.mapTaskLog[args.MapTaskId] = TaskStatusCompleted
		c.mapFinished++
	}
	return nil
}
func (c *Coordinator) ReceiveFinishedReduce(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// 更新任务状态为已完成
	if args.ReduceTaskId >= 0 && args.ReduceTaskId < len(c.reduceTaskLog) {
		c.reduceTaskLog[args.ReduceTaskId] = TaskStatusCompleted
		c.reduceFinished++
	}
	return nil
}

func (c *Coordinator) AllocateTask(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock() // 使用defer确保锁释放

	// Phase 1: 分配Map任务
	if c.mapFinished < c.nMap {
		// 查找第一个未分配(Map)任务
		for i := 0; i < c.nMap; i++ {
			if c.mapTaskLog[i] == TaskStatusUnassigned {
				// 分配任务
				reply.Tasktype = TaskTypeMap
				reply.MapTaskId = i
				reply.Filename = c.files[i]
				reply.NReduce = c.nReduce

				// 标记任务为已分配
				c.mapTaskLog[i] = TaskStatusAssigned

				// 启动超时检测协程（需传递i的副本）
				go func(taskId int) {
					time.Sleep(10 * time.Second)
					c.mu.Lock()
					defer c.mu.Unlock()
					// 仅重置未完成的任务
					if c.mapTaskLog[taskId] == TaskStatusAssigned {
						c.mapTaskLog[taskId] = TaskStatusUnassigned
					}
				}(i) // 传递当前i的副本
				return nil
			}
		}

		// 所有Map任务都已分配但未完成
		reply.Tasktype = TaskTypeWait
		return nil
	}

	// Phase 2: 分配Reduce任务
	if c.reduceFinished < c.nReduce {
		// 查找第一个未分配(Reduce)任务
		for i := 0; i < c.nReduce; i++ {
			if c.reduceTaskLog[i] == TaskStatusUnassigned {
				// 分配任务
				reply.Tasktype = TaskTypeReduce
				reply.ReduceTaskId = i
				reply.NMap = c.nMap

				// 标记任务为已分配
				c.reduceTaskLog[i] = TaskStatusAssigned

				// 启动超时检测协程
				go func(taskId int) {
					time.Sleep(10 * time.Second)
					c.mu.Lock()
					defer c.mu.Unlock()
					if c.reduceTaskLog[taskId] == TaskStatusAssigned {
						c.reduceTaskLog[taskId] = TaskStatusUnassigned
					}
				}(i)
				return nil
			}
		}

		// 所有Reduce任务都已分配但未完成
		reply.Tasktype = TaskTypeWait
		return nil
	}

	// Phase 3: 所有任务已完成
	reply.Tasktype = TaskTypeDone
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	ret := c.reduceFinished == c.nReduce
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.files = files
	c.nMap = len(files)
	c.nReduce = nReduce
	c.mapFinished = 0
	c.reduceFinished = 0
	c.mapTaskLog = make([]int, c.nMap)
	c.reduceTaskLog = make([]int, c.nReduce)
	c.mu = new(sync.Mutex)
	// Your code here.

	c.server()
	return &c
}
