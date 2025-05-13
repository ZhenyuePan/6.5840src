package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
// WorkerArgs is used to send the finished task ID to the coordinator
type WorkerArgs struct {
	MapTaskId    int // Finished Map task ID
	ReduceTaskId int // Finished Reduce task ID
}
type WorkerReply struct {
	Tasktype     byte   // 0: MapTask, 1: ReduceTask, 2: Waiting, 3: JobFinished
	NMap         int    // count of map task
	NReduce      int    // count of reduce task
	Filename     string // maptask only
	MapTaskId    int    // map task only
	ReduceTaskId int    // reduce task only
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
