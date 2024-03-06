package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//
//this file is useful for Remote Procedure Calls (RPCs) to facilitate communication between different components, such as between the coordinator and the worker nodes.

import (
	"os"
	"strconv"
)

// example to show how to declare the arguments
// and reply for an RPC.
type ExampleArgs struct {
	X int
}
type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskArgs struct { //contains the arguments that the worker node requires to perform its task, like the location of data to process or specific task parameters
	//WorkerID      int    //unique identifier for the worker
	//TaskType      int    // 0 for map, 1 for reduce, -1 for no task ; tells the worker what type of task it should do
	//TaskID        int    //not sure if i need this or not; only useful if each task is unique, will come back to later
	//MapSucessFile string //may need to put this in its own struct
	//Filename      string //the name of the file that the worker should process. coordinator will use this to assign tasks to workers
}
type TaskReply struct { //used to store the results or status information that the worker node returns to the coordinator after completing its task.
	TaskStatus int    // 0 for unassigned, 1 for in-progress, 2 for completed
	TaskType   int    // 0 for map, 1 for reduce, -1 for no task
	TaskID     int    //not sure if i need this or not; only useful if each task is unique, will come back to later
	Filename   string //the name of the file that the worker should process. coordinator will use this to assign tasks to workers
	NReduce    int    //the number of tasks that the worker should perform. coordinator will use this to assign tasks to workers
	//Index             int    //the index of the file that the worker should process. coordinator will use this to assign tasks to workers
	Files             []string
	IntermediateFiles [][]string
	mapTaskID         int
	reduceTaskID      int
}

type IntermediateArgs struct { //NotifyIntermediateArgs
	ReduceIndex int
	File        string
	//TaskID      int
	mapTaskID    int
	reduceTaskID int
}

type ReplyStorage struct { //empty i thkn
	//TaskID int
	//File   string
} //i can store the reply here //NotifyReply{}

type CompleteTaskArgs struct { //for intermediate files holding
	File         string
	TaskID       int
	TaskType     int
	mapTaskID    int
	reduceTaskID int
}

type NotifyReduceSucessArgs struct { //NotifyReduceSuccessArgs
	File         string
	TaskID       int
	ReduceIndex  int
	TaskType     int
	mapTaskID    int
	reduceTaskID int
}

type NotifyMapSuccessArgs struct { //NotifyMapSuccessArgs
	File             string
	TaskID           int
	IntermediateArgs [][]string
	mapTaskID        int
	reduceTaskID     int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
