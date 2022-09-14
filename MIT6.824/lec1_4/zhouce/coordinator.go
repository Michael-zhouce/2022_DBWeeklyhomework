package mr

import (
	"log"
	"strconv"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type CoordinatorState int
type TaskState int

//每个任务的状态 枚举值
const (
	Unstart TaskState =itoa
	Running
	Done
}

const (
	MapState CoordinatorState =iota
	ReduceState
	Notask

)
type Coordinator struct {
	// Your definitions here.
	mapfiles []string

	nmaptask int
	nreducetask int

	MapTaskStatus_map   map[int]TaskState
	ReduceTaskStatus_map   map[int]TaskState

	nFinishedMapTask   int
	nFinishedReduceTask int

	//目前的状态
	Curstate CoordinatorState

	mu sync.Mutex

	FinishedMaptask []bool //map任务是否完成



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
func (c *Coordinator) AssignTask(args *askArgs, reply *ResponseReply) error {
	//共享变量需要锁一下，互斥
	c.mu.Lock()
	coordinatorstate:=c.Curstate
	c.mu.Unlock()
	//看一下这个请求任务的worker状态
	switch args.WorkerState{
	//如果是空闲，就要领一下任务
	case Idle:
		//领什么任务需要协调者统筹,看一下协调者的状态
		switch coordinatorstate{
		case MapState:
			c.
		}

	}

}
func (c *Coordinator) ToDoMapTask (reply *ResponseReply){

}
//
// start a thread that listens for RPCs from worker.go
//
//这是rpc的服务端
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
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
//files []string 是传进来的那些文本
func MakeCoordinator(files []string, nReduce int) *Coordinator { //
	// Your code here.
	c := Coordinator{}
	c.mapfiles=files
	c.Curstate=MapState
	c.nmaptask=len(files)
	c.nreducetask=nReduce
	c.nFinishedMapTask=0
	c.nFinishedReduceTask=0
	c.MapTaskStatus_map=make(map[int]TaskState,c.nmaptask)
	//刚开始，map任务还没开始
	for i:=0;i<c.nmaptask;i++{
		c.MapTaskStatus_map[i]=Unstart
	}
	c.ReduceTaskStatus_map=make(map[int]TaskState,c.nreducetask)
	for i:=0;i<c.nreducetask;i++{
		c.ReduceTaskStatus_map[i]=Unstart
	}
   //开始准备分配任务
	c.server()
	return &c
}
