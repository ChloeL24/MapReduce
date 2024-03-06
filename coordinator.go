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

type Coordinator struct {
	// Your definitions here.

	InputFiles        []string //files that we're working on
	intermediateFiles map[int][]string
	//intermediateFiles [][]string     //intermediate files that we're working on //maybe delete
	NReduce           int            //# of reduce tasks
	MapTasksStatus    map[string]int // State of each map task (if unassigned, inprogress, or completed)//
	ReduceTasksStatus map[int]int    // State of each reduce task (if unassigned, in-progress, or completed)
	ReduceTasksDone   bool           // Channel for completed reduce tasks, so that we can tell the coordinator when a reduce task is done;;; dont think i need a channel
	MapTasksDone      bool
	mapTasks          []Task // List of map tasks
	reduceTasks       []Task // List of reduce tasks
	mutex             sync.Mutex
}

const (
	Unassigned = iota
	InProgress
	Completed
)

var maptask chan Task   // declare maptask variable
var reducetask chan int // declare reducetask variable

type Task struct { // represents the state of a task (map or reduce). helps me keep track of the state of each task for later
	Assigned  bool   // Indicates whether the task has been assigned to a worker
	Completed bool   // Indicates whether the task has been completed by the worker
	ID        int    // Unique identifier for the task
	fileName  string // Name of the file that the task is working on
	TaskType  int    // 0 for map, 1 for reduce

}

func (c *Coordinator) mapperTimer(task Task) { //i dont think there is a problem here

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.mutex.Lock()
			c.MapTasksStatus[task.fileName] = Unassigned
			c.mutex.Unlock()
			maptask <- task //put back in channel

		default:
			c.mutex.Lock()
			if c.MapTasksStatus[task.fileName] == Completed {
				c.mutex.Unlock()
				return
			}
			c.mutex.Unlock()

		}
	}
}

/*
func (c *Coordinator) getintermediateFiles(args *IntermediateArgs, reply *TaskReply) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	//return c.intermediateFiles[args.ReduceIndex]

	reply.IntermediateFiles = c.intermediateFiles[]

	//return c.intermediateFiles

}
*/

func (c *Coordinator) checkReduceTaskStatus(reduceTask int) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.ReduceTasksStatus[reduceTask] == Completed {
		return true
	}
	c.ReduceTasksStatus[reduceTask] = Unassigned
	return false
}

func (c *Coordinator) reducerTimer(reduceTask int) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.mutex.Lock()
			c.ReduceTasksStatus[reduceTask] = Unassigned
			c.mutex.Unlock()
			reducetask <- reduceTask // Requeue the reduce task if not completed
		default:
			c.mutex.Lock()
			if c.ReduceTasksStatus[reduceTask] == Completed {
				c.mutex.Unlock()
				return
			}
			c.mutex.Lock()
		}
	}

}

func (c *Coordinator) AddIntermediateFile(args *IntermediateArgs, reply *ReplyStorage) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.intermediateFiles[args.ReduceIndex] = append(c.intermediateFiles[args.ReduceIndex], args.File)
	return nil
}

// Your code here -- RPC handlers for the worker to call.
// should use the inputed informaiton to assign a task to a worker or check the status of a task
// should also be able to handle the case where a worker fails to complete a task?
// The coordinator should notice if a worker hasn't completed its task in a reasonable amount of time (for this lab, use ten seconds), and give the same task to a different worker.
func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	// Your code here.

	select {
	case assignTask := <-maptask:
		//fmt.Println(assignTask.fileName, "this is the file name") //   ../data/pg-huckleberry_finn.txt

		c.mutex.Lock()
		c.intermediateFiles = make(map[int][]string, c.NReduce)
		c.mutex.Unlock()

		reply.NReduce = c.NReduce
		reply.TaskType = 0 // 0 for map, 1 for reduce, -1 for no task
		reply.Filename = assignTask.fileName
		//reply.Index = assignTask.ID
		reply.Files = c.InputFiles
		reply.TaskID = assignTask.ID
		reply.TaskStatus = 1 // 0 for unassigned, 1 for in-progress, 2 for completed
		assignTask.Assigned = true
		assignTask.Completed = false
		assignTask.TaskType = 0

		reply.IntermediateFiles = make([][]string, c.NReduce)

		c.mutex.Lock()
		c.MapTasksStatus[assignTask.fileName] = InProgress
		c.mutex.Unlock()
		go c.mapperTimer(assignTask)
		return nil

	case reduceTask := <-reducetask:
		//fmt.Println("this is a reduce task")

		reply.TaskType = 1
		reply.Filename = c.InputFiles[reduceTask]
		//reply.Index = reduceTask
		reply.TaskID = reduceTask
		reply.NReduce = c.NReduce

		reply.Files = c.intermediateFiles[reduceTask]

		reply.TaskStatus = 0
		c.mutex.Lock()
		c.ReduceTasksStatus[reduceTask] = InProgress
		c.mutex.Unlock()

		go c.reducerTimer(reduceTask)

		reply.IntermediateFiles = make([][]string, c.NReduce)

		return nil

	}
	//return nil

}

func (c *Coordinator) MapSucess(args *NotifyMapSuccessArgs, reply *ReplyStorage) error { //maybe i should not be feednig in a differnet struct
	///args contains info, reply is empty
	c.mutex.Lock()
	defer c.mutex.Unlock()

	//fmt.Printf("MapSuccess called with File: %s, TaskID: %d\n", args.File, args.TaskID)

	//c.MapTasksStatus[reply.File] = Completed
	c.MapTasksStatus[args.File] = Completed
	//fmt.Println(c.MapTasksStatus[args.File], "this is the status of the map task: should be completed")

	allMapTasksCompleted := true

	for _, status := range c.MapTasksStatus {
		if status != Completed {
			allMapTasksCompleted = false
			//fmt.Println("allMapTasksCompleted is false")
			break
		}
	}
	//fmt.Println("allMapTasksCompleted: ", allMapTasksCompleted)
	c.MapTasksDone = allMapTasksCompleted

	if c.MapTasksDone {
		//fmt.Print("starting reduce tasks")
		// Transition to reduce phase by marking all reduce tasks as unassigned
		for i := 0; i < c.NReduce; i++ {
			c.ReduceTasksStatus[i] = Unassigned
			reducetask <- i
		}
	}
	return nil
}

func (c *Coordinator) ReduceSucess(args *NotifyReduceSucessArgs, reply *ReplyStorage) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.ReduceTasksStatus[args.TaskID] = Completed
	allReduceTasksCompleted := true

	for _, v := range c.ReduceTasksStatus {
		if v != Completed {
			allReduceTasksCompleted = false
			break
		}
	}
	c.ReduceTasksDone = allReduceTasksCompleted
	return nil
}

// an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// mr-main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false //change back to false

	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.MapTasksDone && c.ReduceTasksDone {
		ret = true
	}
	return ret
}

// mr-main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator { //the number of reduce jobs will be given here // connection error here
	c := Coordinator{
		InputFiles:        files,
		intermediateFiles: make(map[int][]string, nReduce),
		NReduce:           nReduce,
		MapTasksStatus:    make(map[string]int, len(files)),
		ReduceTasksStatus: make(map[int]int, nReduce),
		mapTasks:          make([]Task, len(files)),
		reduceTasks:       make([]Task, nReduce),

		MapTasksDone:    false,
		ReduceTasksDone: false,
		mutex:           sync.Mutex{},
	}
	//fmt.Print("Coordinator created")

	maptask = make(chan Task, len(files)) //make sure we have enough space for all the tasks
	reducetask = make(chan int, nReduce)

	for index, file := range files {
		c.MapTasksStatus[file] = Unassigned
		c.mapTasks[index] = Task{

			Assigned:  false,
			Completed: false,
			ID:        index,
			fileName:  file,
			TaskType:  0}

		task := Task{
			Assigned:  false,
			Completed: false,
			ID:        index,
			fileName:  file,
			TaskType:  0,
		}
		maptask <- task
		//fmt.Println("task added to maptask channel")

	}

	for i := 0; i < nReduce; i++ {
		c.mutex.Lock()
		c.ReduceTasksStatus[i] = Unassigned
		c.reduceTasks[i] = Task{
			Assigned:  false,
			Completed: false,
			ID:        i,
			TaskType:  1,
		}

		c.mutex.Unlock()
	}

	c.InputFiles = files
	c.NReduce = nReduce
	c.intermediateFiles = make(map[int][]string, nReduce)

	//fmt.Println("Coordinator created")

	// Your code here.

	c.server()
	return &c
}

// start a thread that listens for RPCs from worker.go
// DO NOT MODIFY
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
