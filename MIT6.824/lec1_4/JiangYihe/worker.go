package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// for sorting by key.

type ByKey []KeyValue

// for sorting by key.

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//

type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		reply := callHeartbeat()
		switch reply.WorkingType {
		case MapPhase: //map 阶段
			mapContent := reply.MapContent
			// 开始读文件
			file, err := os.Open(mapContent.Filename)
			if err != nil {
				log.Fatalf("cannot open %v", mapContent.Filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", mapContent.Filename)
			}
			file.Close()
			kva := mapf(mapContent.Filename, string(content))
			sort.Sort(ByKey(kva))
			var tmpFiles []*os.File
			var intermediateFilenames []string
			for i := 0; i < mapContent.NReduce; i++ {
				file, err = ioutil.TempFile("data", fmt.Sprintf("mr-%d-%d-*.txt", mapContent.MapId, i))
				if err != nil {
					log.Fatalf("cannot create tmp file")
				}
				tmpFiles = append(tmpFiles, file)
				intermediateFilenames = append(intermediateFilenames, file.Name())
			}
			for i := 0; i < len(kva); i++ {
				kv := kva[i]
				file := tmpFiles[ihash(kv.Key)%mapContent.NReduce]
				fmt.Fprintf(file, "%v %v\n", kv.Key, kv.Value)
			}

			for i := 0; i < mapContent.NReduce; i++ {
				file := tmpFiles[i]
				file.Close()
			}
			callFinishMap(mapContent.MapId, intermediateFilenames)
		case ReducePhase:
			reduceContent := reply.ReduceContent
			var kva []KeyValue
			for i := 0; i < len(reduceContent.IntermediateFilenames); i++ {
				filename := reduceContent.IntermediateFilenames[i]
				file, err := os.Open("data/" + filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				kva = append(kva, convertContent2KV(string(content))...)
			}

			sort.Sort(ByKey(kva))
			outFile, _ := ioutil.TempFile("data", fmt.Sprintf("mr-out-%d-*", reduceContent.ReduceId))
			for i := 0; i < len(kva); {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				var values []string
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)
				fmt.Fprintf(outFile, "%v %v\n", kva[i].Key, output)
				i = j
			}
			outFile.Close()
			callFinishReduce(reduceContent.ReduceId, outFile.Name())
		case FinishPhase:
			return
		case WorkerWait:
			time.Sleep(1 * time.Second)
		default:
			return
		}

	}

}

func callHeartbeat() *HeartbeatReply {
	args := HeartbeatArgs{}
	reply := HeartbeatReply{}
	call("Coordinator.HandleHeartbeat", &args, &reply)
	return &reply
}

func callFinishMap(mapId int, intermediateFilenames []string) {
	args := MapFinishedArgs{mapId, intermediateFilenames}
	reply := MapFinishedReply{}
	call("Coordinator.HandleMapFinished", &args, &reply)
}

func callFinishReduce(reduceId int, outputFilename string) {
	args := ReduceFinishedArgs{reduceId, outputFilename}
	reply := ReduceFinishedReply{}
	call("Coordinator.HandleReduceFinished", &args, &reply)
}

func convertContent2KV(content string) []KeyValue {
	var kva []KeyValue
	lines := strings.Split(content, "\n")
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		kv := strings.Split(line, " ")
		kva = append(kva, KeyValue{kv[0], kv[1]})
	}
	return kva
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
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
