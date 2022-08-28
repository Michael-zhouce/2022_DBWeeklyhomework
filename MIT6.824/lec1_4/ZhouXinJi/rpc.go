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

type OpType int
type WorkerState int

const (
	MapOperation OpType = iota
	ReduceOperation
	WaitOperation
	DoneOpetation
)

const (
	Idle WorkerState = iota
	Finished
)

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type RequestArgs struct {
	WorkerState WorkerState
	TaskId      int
}

// The first letter must be capitalized
type ResponseArgs struct {
	Filename     string
	Operation    OpType
	TaskId       int
	MapNumber    int
	ReduceNumber int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
