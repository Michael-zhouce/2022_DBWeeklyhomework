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

type Coordinator struct {
	// Your definitions here.
	mapTaskNoAllocated    map[int]bool
	mapTaskNoDone         map[int]bool
	reduceTaskNoAllocated map[int]bool
	reduceTaskNoDone      map[int]bool
	files                 []string
	num_reducer           int

	mutex sync.Mutex
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

// worker call this function to get task to do
func (c *Coordinator) AcquireTask(args *AcTaskArgs, reply *AcTaskReply) error {

	c.mutex.Lock()
	defer c.mutex.Unlock()
    //还有map任务
	if len(c.mapTaskNoAllocated) > 0 {
		for i := range c.mapTaskNoAllocated {
			reply.Task_id = i
			reply.Task_t = MapTask
			reply.Map_file_name = c.files[i]
			reply.Num_reducer = c.num_reducer
			delete(c.mapTaskNoAllocated, i)
			c.mapTaskNoDone[i] = true
			go func(task_id int) {
				time.Sleep(10 * time.Second)
				c.mutex.Lock()
				defer c.mutex.Unlock()
				_, ok := c.mapTaskNoDone[task_id]
				if ok {
					// timeout! should put back to no allocated queue
					fmt.Println("map Task", task_id, "time out!")
					delete(c.mapTaskNoDone, task_id)
					c.mapTaskNoAllocated[task_id] = true
				}
			}(i)
			return nil
		}
	} else if len(c.mapTaskNoDone) > 0 {
		// some map task is not done! work should wait
		reply.Task_t = WaitTask
		return nil
	} else if len(c.reduceTaskNoAllocated) > 0 {
		// map task is all done and reduce task is not all done
		for i := range c.reduceTaskNoAllocated {
			reply.Task_id = i
			reply.Task_t = ReduceTask
			reply.Map_task_num = len(c.files)
			delete(c.reduceTaskNoAllocated, i)
			c.reduceTaskNoDone[i] = true
			go func(task_id int) {
				time.Sleep(10 * time.Second)
				c.mutex.Lock()
				defer c.mutex.Unlock()
				_, ok := c.reduceTaskNoDone[task_id]
				if ok {
					// reduce task timeout!
					fmt.Println("reduce task", task_id, "time out!")
					delete(c.reduceTaskNoDone, task_id)
					c.reduceTaskNoAllocated[task_id] = true
				}
			}(i)
			return nil
		}
	} else if len(c.reduceTaskNoDone) > 0 {
		// reduce is not done! so wait
		reply.Task_t = WaitTask
		return nil
	} else {
		// all queue is empty, so the job is done!
		reply.Task_t = Finished
		fmt.Println("All jobs are done!")
		return nil
	}
	return nil
}

// worker call this function to finish task
func (c *Coordinator) TaskDone(args *DoneTaskArgs, reply *DoneTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	switch args.Task_t {
		case MapTask: {
		_, ok := c.mapTaskNoDone[args.Task_id]
		if ok {
			// the task is in the NoDone queue
			delete(c.mapTaskNoDone, args.Task_id)
		} else {
			_, ok := c.mapTaskNoAllocated[args.Task_id]
			if ok {
				// the taskid is found in NoAll, this can happen when this is timeout and then send reply
				delete(c.mapTaskNoAllocated, args.Task_id)
			} else {
				// replica reply
				return nil
			}
		}
		// check whether all map task is done and then begin reduce task
		if len(c.mapTaskNoAllocated) == 0 && len(c.mapTaskNoDone) == 0 {
			for i := 0; i < c.num_reducer; i++ {
				c.reduceTaskNoAllocated[i] = true
			}
		}
	} 
	case ReduceTask: {
		_, ok := c.reduceTaskNoDone[args.Task_id]
		if ok {
			delete(c.reduceTaskNoDone, args.Task_id)
		} else {
			_, ok := c.reduceTaskNoAllocated[args.Task_id]
			if ok {
				delete(c.reduceTaskNoDone, args.Task_id)
			} else {
				return nil
			}
		}
	}
}
	return nil
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
	ret := false
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// check whether all queue is empty
	if len(c.mapTaskNoAllocated) == 0 && len(c.mapTaskNoDone) == 0 && len(c.reduceTaskNoAllocated) == 0 && len(c.reduceTaskNoDone) == 0 {
		ret = true
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// init the fields
	c.mapTaskNoAllocated = make(map[int]bool)
	c.mapTaskNoDone = make(map[int]bool)
	c.reduceTaskNoAllocated = make(map[int]bool)
	c.reduceTaskNoDone = make(map[int]bool)
	c.files = files
	c.num_reducer = nReduce
	for i := 0; i < len(files); i++ {
		c.mapTaskNoAllocated[i] = true
	}

	// Your code here.

	c.server()
	return &c
}
