package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Execute map task
func ExecMapTask(reply *ResponseArgs, mapf func(string, string) []KeyValue) {
	content, err := ioutil.ReadFile(reply.Filename)
	if err != nil {
		log.Fatalf("open %v failed", reply.Filename)
	}
	kv_array := mapf(reply.Filename, string(content))
	for _, kv := range kv_array {
		bucket_id := ihash(kv.Key) % reply.ReduceNumber
		ofilename := fmt.Sprintf("mr-%d-%d", reply.TaskId, bucket_id)
		file, err := os.OpenFile(ofilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("open file %v failed", ofilename)
		}
		encoder := json.NewEncoder(file)
		encoder.Encode(kv)
	}
}

// Execute reduce task
func ExecReduceTask(reply *ResponseArgs, reducef func(string, []string) string) {
	var intermediate []KeyValue
	for i := 0; i < reply.MapNumber; i++ {
		ifilename := fmt.Sprintf("mr-%d-%d", i, reply.TaskId)
		fmt.Println(ifilename)
		ifile, err := os.Open(ifilename)
		if err != nil {
			log.Fatalf("open file %v failed", ifilename)
		}
		decoder := json.NewDecoder(ifile)
		var kv KeyValue
		for {
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))
	ofilename := fmt.Sprintf("mr-out-%d", reply.TaskId)
	ofile, _ := os.Create(ofilename)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()
}

// Notify coordinator that worker's state has changed
func NotifyCoordinator() {
	args := RequestArgs{}
	args.WorkerState = Finished
	reply := ResponseArgs{}
	call("Coordinator.AssignTask", &args, &reply)
}

// Request Tasks from coordinator
func RequestTask(reply *ResponseArgs) bool {
	args := RequestArgs{}
	args.WorkerState = Idle
	return call("Coordinator.AssignTask", &args, reply)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Worker should always be executing until coorinator tells it to stop.
	for {
		reply := ResponseArgs{}
		RequestTask(&reply)
		switch reply.Operation {
		case MapOperation:
			ExecMapTask(&reply, mapf)
			NotifyCoordinator()
			break
		case ReduceOperation:
			ExecReduceTask(&reply, reducef)
			NotifyCoordinator()
			break
		case DoneOpetation:
			return
		}
	}

}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
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

	fmt.Println("error occurs when call rpc")
	fmt.Println(err)
	return false
}
