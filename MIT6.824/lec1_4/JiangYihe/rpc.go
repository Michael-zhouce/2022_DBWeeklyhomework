package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

const (
	WorkerWait  = 0
	MapPhase    = 1
	ReducePhase = 2
	FinishPhase = 3
)

type HeartbeatArgs struct {
}

type HeartbeatReply struct {
	WorkingType   int
	MapContent    MapContent
	ReduceContent ReduceContent
}

type MapContent struct {
	Filename string
	MapId    int
	NReduce  int
}

type MapFinishedArgs struct {
	MapId                 int
	IntermediateFilenames []string
}

type MapFinishedReply struct {
}

type ReduceContent struct {
	ReduceId              int
	IntermediateFilenames []string
}

type ReduceFinishedArgs struct {
	ReduceId       int
	OutputFilename string
}

type ReduceFinishedReply struct {
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
