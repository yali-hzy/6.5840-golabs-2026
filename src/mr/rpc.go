package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Task types
const (
	MapTask    = 1
	ReduceTask = 2
	WaitTask   = 3
	ExitTask   = 4
)

type AssignTaskReply struct {
	// Your definitions here.
	TaskType   int
	FileName   string
	TaskNumber int
	NReduce    int
	NMap	   int
}

type AssignTaskArgs struct {
	WorkerID int
}

type TaskCompletedArgs struct {
	TaskType   int
	TaskNumber int
}

type TaskCompletedReply struct {
}
