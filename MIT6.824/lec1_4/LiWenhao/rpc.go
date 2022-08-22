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

type TaskType int

const (
	MapTask    TaskType = 0
	ReduceTask TaskType = 1
	WaitTask   TaskType = 2
	Finished   TaskType = 3
)

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type AcTaskArgs struct {
}

type AcTaskReply struct {
	Task_t  TaskType // task type
	Task_id int      // task number
	// input file names
	Num_reducer   int
	Map_file_name string
	Map_task_num  int
}

type DoneTaskArgs struct {
	Task_t  TaskType
	Task_id int
}

type DoneTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
