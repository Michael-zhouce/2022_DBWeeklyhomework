package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
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
	fmt.Println("array len is", len(kv_array))

	encoder_list := make([]*json.Encoder, reply.ReduceNumber)
	for i := 0; i < reply.ReduceNumber; i++ {
		ofilename := fmt.Sprintf("mr-tmp-%d-%d", reply.TaskId, i)
		file, err := os.Create(ofilename)
		if err != nil {
			log.Fatalf("open file %v failed", ofilename)
		}
		encoder := json.NewEncoder(file)
		encoder_list[i] = encoder
	}

	for _, kv := range kv_array {
		bucket_id := ihash(kv.Key) % reply.ReduceNumber
		encoder := encoder_list[bucket_id]
		encoder.Encode(&kv)
	}

	// It's so important!
	fmt.Println("reduce number:", reply.ReduceNumber)
	for i := 0; i < reply.ReduceNumber; i++ {
		origin_name := fmt.Sprintf("mr-tmp-%d-%d", reply.TaskId, i)
		modified_name := fmt.Sprintf("mr-%d-%d", reply.TaskId, i)
		os.Rename(origin_name, modified_name)
	}

}

// Execute reduce task
func ExecReduceTask(reply *ResponseArgs, reducef func(string, []string) string) {
	var intermediate []KeyValue
	for i := 0; i < reply.MapNumber; i++ {
		ifilename := fmt.Sprintf("mr-%d-%d", i, reply.TaskId)
		ifile, err := os.Open(ifilename)
		defer ifile.Close()
		if err != nil {
			log.Fatalf("open file %v failed", ifilename)
		}
		decoder := json.NewDecoder(ifile)
		count := 0
		for {
			var kv KeyValue
			err := decoder.Decode(&kv)
			if err == io.EOF {
				break
			} else if err != nil {
				fmt.Println("other error", err)
				fmt.Println("filename:", ifilename)
				fmt.Println("content:", kv)
				fmt.Println("count is ", count)
				break
			}
			count += 1
			intermediate = append(intermediate, kv)
		}
	}
	fmt.Println("intermediate len ", len(intermediate))
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
func NotifyCoordinator(taskId int) {
	args := RequestArgs{}
	args.TaskId = taskId
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
			NotifyCoordinator(reply.TaskId)
			break
		case ReduceOperation:
			ExecReduceTask(&reply, reducef)
			NotifyCoordinator(reply.TaskId)
			break
		case WaitOperation:
			time.Sleep(time.Second)
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
