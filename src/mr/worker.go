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
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// record finished task to tell coordinator in next communication turn
	var finishedTask TaskRequest = TaskRequest{DoneType: NoneTaskType}
	// new task for this worker to perform
	var currentTask TaskReply

	for {
		currentTask = RequesTask(&finishedTask)
		switch currentTask.Type {
		case MapTaskType:
			filename := currentTask.Files[0]
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}

			defer file.Close()
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}

			// apply map func
			intermediate := mapf(filename, string(content))

			// group intermediate kvs by hash value
			intermediateByReducer := make(map[int][]KeyValue)
			for _, kv := range intermediate {
				reducerId := ihash(kv.Key) % currentTask.NReduce
				intermediateByReducer[reducerId] = append(intermediateByReducer[reducerId], kv)
			}

			// output intermediate kvs to disk files
			reduceTaskFiles := make([]string, currentTask.NReduce)
			for reducerId, kvs := range intermediateByReducer {
				reduceTaskFilename := fmt.Sprintf("mr-%d-%d", currentTask.Id, reducerId)
				reduceTaskFile, _ := os.Create(reduceTaskFilename)
				defer reduceTaskFile.Close()
				enc := json.NewEncoder(reduceTaskFile)
				for _, kv := range kvs {
					err := enc.Encode(kv)
					if err != nil {
						log.Fatal(err)
					}
				}

				reduceTaskFiles[reducerId] = reduceTaskFilename
			}

			finishedTask = TaskRequest{DoneType: MapTaskType, Id: currentTask.Id, Files: reduceTaskFiles}
		case ReduceTaskType:
			intermediate := []KeyValue{}
			// get all intermediate kvs from files for current reduce task
			for _, filename := range currentTask.Files {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}

				defer file.Close()
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
			}

			sort.Sort(ByKey(intermediate))
			outputFilename := fmt.Sprintf("mr-out-%d", currentTask.Id)
			outputFile, _ := os.Create(outputFilename)
			defer outputFile.Close()

			// apply reduce func and output result
			for i := 0; i < len(intermediate); {
				j := i + 1
				for j < len(intermediate) && intermediate[i].Key == intermediate[j].Key {
					j++
				}

				ls := []string{}
				for k := i; k < j; k++ {
					ls = append(ls, intermediate[k].Value)
				}

				output := reducef(intermediate[i].Key, ls)
				fmt.Fprintf(outputFile, "%v %v\n", intermediate[i].Key, output)
				i = j
			}

			finishedTask = TaskRequest{DoneType: ReduceTaskType, Id: currentTask.Id, Files: []string{outputFilename}}
		case SleepTaskType:
			duration := time.Duration(500)
			time.Sleep(duration * time.Millisecond)
			finishedTask = TaskRequest{DoneType: NoneTaskType}
		case ExitTaskType:
			return
		default:
			panic(fmt.Sprintf("unknown type: %v", currentTask.Type))
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

}

func RequesTask(req *TaskRequest) TaskReply {

	// declare an argument structure.

	// declare a reply structure.
	reply := TaskReply{}
	ok := call("Coordinator.AssignTask", &req, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
		os.Exit(0)
	}

	return reply
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
