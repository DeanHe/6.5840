package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type MapTask struct {
	id        int
	filename  string
	startTime time.Time
	done      bool
}

type ReduceTask struct {
	id        int
	files     []string
	startTime time.Time
	done      bool
}

type Coordinator struct {
	// Your definitions here.
	mapTasks         []MapTask
	mapTasksRemain   int
	reduceTasks      []ReduceTask
	ReduceTaskRemain int
	mutex            sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(req *TaskRequest, reply *TaskReply) error {
	switch req.DoneType {
	case MapTaskType:
		if !c.mapTasks[req.Id].done {
			c.mapTasks[req.Id].done = true
			// add intemidate files to reduce tasks
			for reduceId, file := range req.Files {
				if len(file) > 0 {
					c.reduceTasks[reduceId].files = append(c.reduceTasks[reduceId].files, file)
				}
			}
			c.mapTasksRemain--
		}
	case ReduceTaskType:
		if !c.reduceTasks[req.Id].done {
			c.reduceTasks[reply.Id].done = true
			c.ReduceTaskRemain--
		}
	}

	now := time.Now()
	timeoutTick := now.Add(-10 * time.Second)
	if c.mapTasksRemain > 0 { // has remaining map task unfinished
		for i := range c.mapTasks {
			mapTask := &c.mapTasks[i]
			// unfinished and timeout
			if !mapTask.done && mapTask.startTime.Before(timeoutTick) {
				reply.Type = MapTaskType
				reply.Id = mapTask.id
				reply.Files = []string{mapTask.filename}
				reply.NReduce = len(c.reduceTasks)
				mapTask.startTime = now
				return nil
			}
		}

		// not find unfinished map task, ask worker to sleep for a while
		reply.Type = SleepTaskType
	} else if c.ReduceTaskRemain > 0 {
		for i := range c.reduceTasks {
			reduceTask := &c.reduceTasks[i]
			// unfinished and timeout
			if !reduceTask.done && reduceTask.startTime.Before(timeoutTick) {
				reply.Type = ReduceTaskType
				reply.Id = reduceTask.id
				reply.Files = reduceTask.files
				reduceTask.startTime = now
				return nil
			}
		}

		// not find unfinished reduce task, ask worker to sleep for a while
		reply.Type = SleepTaskType
	} else {
		// both map phase and reduce phase finished, ask worker to terminate itself
		reply.Type = ExitTaskType
	}

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	// Your code here.
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.mapTasksRemain == 0 && c.ReduceTaskRemain == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTasks:         make([]MapTask, len(files)),
		reduceTasks:      make([]ReduceTask, nReduce),
		mapTasksRemain:   len(files),
		ReduceTaskRemain: nReduce,
	}

	// Your code here.
	for i, f := range files {
		c.mapTasks[i] = MapTask{id: i, filename: f, done: false}
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = ReduceTask{id: i, done: false}
	}

	c.server()
	return &c
}
