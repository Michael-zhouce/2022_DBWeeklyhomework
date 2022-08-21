package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	mu sync.Mutex

	numReduce int
	files     []string

	mapPhases   []bool
	mapFinished []bool

	reducePhases   []bool
	reduceFinished []bool

	curPhase int //当前阶段
}

func (c *Coordinator) HandleHeartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch c.curPhase {
	case MapPhase:
		content := MapContent{}
		for i := 0; i < len(c.mapPhases); i++ {
			if c.mapPhases[i] != true {
				content.Filename = c.files[i]
				content.NReduce = c.numReduce
				content.MapId = i
				c.mapPhases[i] = true
				reply.WorkingType = MapPhase
				reply.MapContent = content
				go c.asyncCheckMapFinished(i)
				return nil
			}
		}
	case ReducePhase:
		content := ReduceContent{}
		for i := 0; i < len(c.reducePhases); i++ {
			if c.reducePhases[i] != true {
				content.IntermediateFilenames = c.getReduceFilenames(i)
				content.ReduceId = i
				c.reducePhases[i] = true
				reply.WorkingType = ReducePhase
				reply.ReduceContent = content
				go c.asyncCheckReduceFinished(i)
				return nil
			}
		}
	case FinishPhase:
		reply.WorkingType = FinishPhase
	default:
	}
	return nil
}

func (c *Coordinator) HandleMapFinished(args *MapFinishedArgs, reply *MapFinishedReply) error {
	tmpFiles := args.IntermediateFilenames
	for i := 0; i < len(tmpFiles); i++ {
		filename := tmpFiles[i]
		err := os.Rename(filename, fmt.Sprintf("data/mr-%d-%d.txt", args.MapId, i))
		if err != nil {
			fmt.Println(err)
		}
	}
	c.mu.Lock()
	c.mapFinished[args.MapId] = true
	c.mu.Unlock()
	if c.checkAllMapFinished() {
		c.curPhase = ReducePhase
	}
	return nil
}

func (c *Coordinator) HandleReduceFinished(args *ReduceFinishedArgs, reply *ReduceFinishedReply) error {
	tmpFilename := args.OutputFilename
	os.Rename(tmpFilename, fmt.Sprintf("mr-out-%d", args.ReduceId))
	c.mu.Lock()
	c.reduceFinished[args.ReduceId] = true
	c.mu.Unlock()
	if c.checkAllReduceFinished() {
		c.curPhase = FinishPhase
	}
	return nil
}

func (c *Coordinator) asyncCheckMapFinished(index int) {
	time.Sleep(10 * time.Second)
	c.mu.Lock()
	if c.mapFinished[index] != true {
		c.mapPhases[index] = false
	}
	c.mu.Unlock()
}

func (c *Coordinator) asyncCheckReduceFinished(index int) {
	time.Sleep(10 * time.Second)
	c.mu.Lock()
	if c.reduceFinished[index] != true {
		c.reducePhases[index] = false
	}
	c.mu.Unlock()
}

func (c *Coordinator) checkAllMapFinished() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; i < len(c.mapFinished); i++ {
		if c.mapFinished[i] == false {
			return false
		}
	}
	return true
}

func (c *Coordinator) checkAllReduceFinished() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; i < len(c.reduceFinished); i++ {
		if c.reduceFinished[i] == false {
			return false
		}
	}
	return true
}

func (c *Coordinator) getReduceFilenames(reduceId int) []string {
	mapLength := len(c.mapPhases)
	var array []string
	for i := 0; i < mapLength; i++ {
		array = append(array, fmt.Sprintf("mr-%d-%d.txt", i, reduceId))
	}
	return array
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.curPhase == FinishPhase
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	os.Mkdir("data", os.ModePerm)
	c.curPhase = MapPhase
	c.files = files
	c.mapPhases = make([]bool, len(files))
	c.mapFinished = make([]bool, len(files))
	c.numReduce = nReduce
	c.reducePhases = make([]bool, nReduce)
	c.reduceFinished = make([]bool, nReduce)
	c.server()
	return &c
}
