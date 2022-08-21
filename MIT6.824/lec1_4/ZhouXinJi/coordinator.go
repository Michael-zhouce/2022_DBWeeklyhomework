package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskState int
type CoordinatorState int

const (
	NoStart TaskState = iota
	Running
	Done
)

const (
	MapState CoordinatorState = iota
	ReduceState
	Finish
)

type Coordinator struct {
	mutex_               sync.Mutex
	InputFiles_          []string
	CurState_            CoordinatorState
	MapTaskStatusMap_    map[int]TaskState
	ReduceTaskStatusMap_ map[int]TaskState
	nFinishedMapTask_    int
	nFinishedReduceTask_ int
	nMap_                int
	nReduce_             int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// The RPC calling method must have a return value of error type, otherwise it cannot be recognized
func (c *Coordinator) AssignTask(args *RequestArgs, reply *ResponseArgs) error {
	c.mutex_.Lock()
	coordinatorState := c.CurState_
	c.mutex_.Unlock()
	switch args.WorkerState {
	// Worker asks for a task
	case Idle:
		switch coordinatorState {
		case MapState:
			c.AssignMapTask(reply)
			break
		case ReduceState:
			c.AssignReduceTask(reply)
			break
		case Finish:
			c.NotifyFinish(reply)
			break
		}
		break
	// Worker tells coordinator that it has finished its task
	case Finished:
		c.HandleFinish(args)
		break
	}
	return nil
}

func (c *Coordinator) AssignMapTask(reply *ResponseArgs) {
	reply.Operation = MapOperation
	c.mutex_.Lock()
	defer c.mutex_.Unlock()
	for i := 0; i < len(c.MapTaskStatusMap_); i++ {
		if c.MapTaskStatusMap_[i] == NoStart {
			c.MapTaskStatusMap_[i] = Running
			reply.TaskId = i
			break
		}
	}
	reply.Filename = c.InputFiles_[reply.TaskId]
	reply.MapNumber = c.nMap_
	reply.ReduceNumber = c.nReduce_
	// If the task has not been completed within 10 seconds, the worker is considered dead.
	// The task will be given to other workers.
	go func(taskId int) {
		time.Sleep(time.Duration(10) * time.Second)
		c.mutex_.Lock()
		defer c.mutex_.Unlock()
		if c.MapTaskStatusMap_[taskId] == Running {
			c.MapTaskStatusMap_[taskId] = NoStart
		}
	}(reply.TaskId)
}

func (c *Coordinator) AssignReduceTask(reply *ResponseArgs) {
	reply.Operation = ReduceOperation
	c.mutex_.Lock()
	defer c.mutex_.Unlock()
	for i := 0; i < len(c.ReduceTaskStatusMap_); i++ {
		if c.ReduceTaskStatusMap_[i] == NoStart {
			c.ReduceTaskStatusMap_[i] = Running
			reply.TaskId = i
			break
		}
	}
	reply.MapNumber = c.nMap_
	reply.ReduceNumber = c.nReduce_
	fmt.Printf("assign reduce task, task id is %d\n", reply.TaskId)
	go func(taskId int) {
		time.Sleep(time.Duration(10) * time.Second)
		c.mutex_.Lock()
		defer c.mutex_.Unlock()
		if c.ReduceTaskStatusMap_[taskId] == Running {
			c.ReduceTaskStatusMap_[taskId] = NoStart
		}
	}(reply.TaskId)
}

// Coordinator tells workers that all tasks have been processed,
// so the worker can stop
func (c *Coordinator) NotifyFinish(reply *ResponseArgs) {
	reply.Operation = DoneOpetation
}

func (c *Coordinator) HandleFinish(args *RequestArgs) {
	c.mutex_.Lock()
	defer c.mutex_.Unlock()
	switch c.CurState_ {
	case MapState:
		c.MapTaskStatusMap_[args.TaskId] = Done
		c.nFinishedMapTask_ += 1
		if c.nFinishedMapTask_ == len(c.MapTaskStatusMap_) {
			fmt.Println("coordinator state changed to reduce")
			c.CurState_ = ReduceState
		}
		break
	case ReduceState:
		fmt.Println("reduce finish")
		c.ReduceTaskStatusMap_[args.TaskId] = Done
		c.nFinishedReduceTask_ += 1
		if c.nFinishedReduceTask_ == len(c.ReduceTaskStatusMap_) {
			c.CurState_ = Finish
		}
		break
	}
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mutex_.Lock()
	ret := c.CurState_ == Finish
	c.mutex_.Unlock()

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce_ is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce_ int) *Coordinator {
	c := Coordinator{}
	c.InputFiles_ = files
	c.CurState_ = MapState
	c.nMap_ = len(files)
	c.nReduce_ = nReduce_
	c.nFinishedMapTask_ = 0
	c.nFinishedReduceTask_ = 0
	c.MapTaskStatusMap_ = make(map[int]TaskState, c.nMap_)
	for i := 0; i < c.nMap_; i++ {
		c.MapTaskStatusMap_[i] = NoStart
	}
	c.ReduceTaskStatusMap_ = make(map[int]TaskState, c.nReduce_)
	for i := 0; i < c.nReduce_; i++ {
		c.ReduceTaskStatusMap_[i] = NoStart
	}

	// Your code here.
	c.server()
	return &c
}
