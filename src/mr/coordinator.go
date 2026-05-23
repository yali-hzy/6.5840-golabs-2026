package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"


type Coordinator struct {
	// Your definitions here.
	files 			     []string
	MapTasks             []Task
	ReduceTasks          []Task
	MapCompletedCount    int
	ReduceCompletedCount int
	NReduce              int
	NMap				 int
	TaskLock  	         sync.Mutex
}

const (
	Idle       = 1
	InProgress = 2
	Completed  = 3
)

type Task struct {
	TaskType   int
	TaskNumber int
	WorkerID   int
	Status     int
	StartTime  int64
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	// Check for timed out tasks
	currentTime := time.Now().Unix()
	c.TaskLock.Lock()
	defer c.TaskLock.Unlock()
	for i := range c.NMap {
		if c.MapTasks[i].Status == InProgress && currentTime - c.MapTasks[i].StartTime > 10 {
			c.MapTasks[i].Status = Idle
		}
	}
	for i := range c.NReduce {
		if c.ReduceTasks[i].Status == InProgress && currentTime - c.ReduceTasks[i].StartTime > 10 {
			c.ReduceTasks[i].Status = Idle
		}
	}
	if c.MapCompletedCount < c.NMap {
		// Assign map tasks
		for i := range c.NMap {
			if c.MapTasks[i].Status == Idle {
				c.MapTasks[i].Status = InProgress
				c.MapTasks[i].WorkerID = args.WorkerID
				c.MapTasks[i].StartTime = time.Now().Unix()
				reply.TaskType = MapTask
				reply.FileName = c.files[i]
				reply.TaskNumber = c.MapTasks[i].TaskNumber
				reply.NReduce = c.NReduce
				return nil
			}
		}
		// No map tasks available, ask worker to wait
		reply.TaskType = WaitTask
		return nil
	} else if c.ReduceCompletedCount < c.NReduce {
		// Assign reduce tasks
		for i := range c.NReduce {
			if c.ReduceTasks[i].Status == Idle {
				c.ReduceTasks[i].Status = InProgress
				c.ReduceTasks[i].WorkerID = args.WorkerID
				c.ReduceTasks[i].StartTime = time.Now().Unix()
				reply.TaskType = ReduceTask
				reply.TaskNumber = c.ReduceTasks[i].TaskNumber
				reply.NMap = c.NMap
				return nil
			}
		}
		// No reduce tasks available, ask worker to wait
		reply.TaskType = WaitTask
		return nil
	} else {
		// All tasks are completed, ask worker to exit
		reply.TaskType = ExitTask
		return nil
	}
}

func (c *Coordinator) TaskCompleted(args *TaskCompletedArgs, reply *TaskCompletedReply) error {
	c.TaskLock.Lock()
	defer c.TaskLock.Unlock()
	switch args.TaskType {
	case MapTask:
		if c.MapTasks[args.TaskNumber].Status == InProgress {
			c.MapTasks[args.TaskNumber].Status = Completed
			c.MapCompletedCount++
		}
	case ReduceTask:
		if c.ReduceTasks[args.TaskNumber].Status == InProgress {
			c.ReduceTasks[args.TaskNumber].Status = Completed
			c.ReduceCompletedCount++
		}
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
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.TaskLock.Lock()
	defer c.TaskLock.Unlock()
	ret = (c.ReduceCompletedCount == c.NReduce)

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.NReduce = nReduce
	c.NMap = len(files)
	for i := range c.NMap {
		task := Task{}
		task.TaskType = MapTask
		task.TaskNumber = i
		task.Status = Idle
		c.MapTasks = append(c.MapTasks, task)
	}

	for i := range nReduce {
		task := Task{}
		task.TaskType = ReduceTask
		task.TaskNumber = i
		task.Status = Idle
		c.ReduceTasks = append(c.ReduceTasks, task)
	}

	c.server(sockname)
	return &c
}
