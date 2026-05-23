package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator


func doMapTask(taskNumber int, filename string, nReduce int, mapf func(string, string) []KeyValue) {
	// Read the input file
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	
	// Create nReduce intermediate files
	tempFiles := make([]*os.File, nReduce)
	encs := make([]*json.Encoder, nReduce)
	for i := range nReduce {
		tempFile, err := os.CreateTemp("", fmt.Sprintf("mr-%d-%d-*.tmp", taskNumber, i))
		if err != nil {
			log.Fatalf("cannot create temp file for mr-%d-%d: %v", taskNumber, i, err)
		}
		tempFiles[i] = tempFile
		encs[i] = json.NewEncoder(tempFile)
	}
	
	// Write KeyValue pairs to corresponding intermediate files
	for _, kv := range kva {
		reduceTaskNumber := ihash(kv.Key) % nReduce
		err := encs[reduceTaskNumber].Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode kv pair %v: %v", kv, err)
		}
	}
	
	// Close and rename temp files to final intermediate files
	for i, tempFile := range tempFiles {
		tempFile.Close()
		finalName := fmt.Sprintf("mr-%d-%d", taskNumber, i)
		os.Rename(tempFile.Name(), finalName)
	}

	TaskCompletedArgs := TaskCompletedArgs{}
	TaskCompletedArgs.TaskType = MapTask
	TaskCompletedArgs.TaskNumber = taskNumber
	reply := TaskCompletedReply{}
	ok := call("Coordinator.TaskCompleted", TaskCompletedArgs, &reply)
	if !ok {
		fmt.Printf("call failed when reporting map task completion!\n")
	}
}

func doReduceTask(taskNumber int, nMap int, reducef func(string, []string) string) {
	KVs := map[string][]string{}

	// Read all intermediate files for this reduce task
	for i := range nMap {
		intermediateFileName := fmt.Sprintf("mr-%d-%d", i, taskNumber)
		file, err := os.Open(intermediateFileName)
		if err != nil {
			log.Fatalf("cannot open intermediate file %v: %v", intermediateFileName, err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			KVs[kv.Key] = append(KVs[kv.Key], kv.Value)
		}
	}
	// Create output file
	ofile, err := os.Create(fmt.Sprintf("mr-out-%d", taskNumber))
	if err != nil {
		log.Fatalf("cannot create output file for reduce task %d: %v", taskNumber, err)
	}
	defer ofile.Close()
	
	// Apply reduce function and write to output file
	for key, values := range KVs {
		output := reducef(key, values)
		fmt.Fprintf(ofile, "%v %v\n", key, output)
	}

	TaskCompletedArgs := TaskCompletedArgs{}
	TaskCompletedArgs.TaskType = ReduceTask
	TaskCompletedArgs.TaskNumber = taskNumber
	reply := TaskCompletedReply{}
	ok := call("Coordinator.TaskCompleted", TaskCompletedArgs, &reply)
	if !ok {
		fmt.Printf("call failed when reporting reduce task completion!\n")
	}
}

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.
	for {
		args := AssignTaskArgs{}
		args.WorkerID = os.Getpid()
		reply := AssignTaskReply{}
		ok := call("Coordinator.AssignTask", args, &reply)
		if !ok {
			fmt.Printf("call failed!\n")
		} else {
			switch reply.TaskType {
				case MapTask:
					doMapTask(reply.TaskNumber, reply.FileName, reply.NReduce, mapf)
				case ReduceTask:
					doReduceTask(reply.TaskNumber, reply.NMap, reducef)
				case WaitTask:
					// Do nothing
				case ExitTask:
					return
				default:
					fmt.Printf("Unknown task type %v\n", reply.TaskType)
			}
		}
		time.Sleep(time.Second)
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
