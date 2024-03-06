package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
)

type ByKey []KeyValue              //sorting
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

// mr-main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, //send an RPC to the coordinator asking for a task. Then modify the worker to read that file and call the application Map function, as in `mrsequential.go`.
	reducef func(string, []string) string) {
	// Your worker implementation here.

	// id, _ := assign random number
	// only one worker is probably getting all of the tasks. figure out a way to give a task to more than one worker
	for {
		//fmt.Println("Worker started") //good

		args := TaskArgs{}
		reply := TaskReply{}

		ok := call("Coordinator.AssignTask", &args, &reply)
		if !ok || reply.TaskStatus == 2 { // Assuming 2 indicates all tasks are completed
			break
		}

		//fmt.Println("worker", "got task type", reply.TaskType, "this is the task type") //good

		//reply is now filled w info

		//Process the task
		if reply.TaskType == 0 { // Map task
			reply.TaskStatus = 1 //in progress

			fileContents, err := os.ReadFile(reply.Filename)
			if err != nil {
				log.Fatalf("cannot read %v", reply.Filename)
			}

			//fmt.Println("worker found map task")
			mapped := mapf(reply.Filename, string(fileContents)) //return type key value pairs

			intermediate := makeIntermediate(mapped, reply.NReduce)

			for r, kva := range intermediate {
				oname := fmt.Sprintf("mr-%d-%d", reply.TaskID, r)
				
				ofile, _ := os.CreateTemp("", oname)
				enc := json.NewEncoder(ofile)
				for _, kv := range kva {
					enc.Encode(&kv)
				}
				ofile.Close()
				os.Rename(ofile.Name(), oname)
				//fmt.Println(oname, "this is the oname")
				reply.IntermediateFiles[r] = append(reply.IntermediateFiles[r], oname)
				//fmt.Println(reply.IntermediateFiles, "these is the intermediate files") //GOOOOOOOD
				taskDone(r, oname)

			}
			reply.TaskStatus = 2

			//fmt.Println("notifying mapsucess")
			NotifyMapSuccess(reply.Filename, reply.TaskID, reply.Files)

			//fmt.Println("sent to map success") //good

		} else if reply.TaskType == 1 { // Reduce task
			//fmt.Println("worker found reduce task") //GOT HERE

			intermediate := []KeyValue{}
			//fmt.Println("intermediate made")

			//fmt.Println(reply.Files, "this is the files") //stooped HERE 2.29

			//fmt.Println(len(reply.IntermediateFiles), "this is the length of the intermediate files") //stooped HERE2.29
			for r := 0; r < len(reply.IntermediateFiles); r++ {
				oname := fmt.Sprintf("mr-%d-%d", reply.TaskID, r)
				//fmt.Println(oname, "this is the oname")

				file, err := os.Open(oname)
				if err != nil {
					log.Fatalf("cannot open %v", oname)
				}

				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
					//fmt.Println(intermediate, "this is the intermediate") Very long
				}
				file.Close()
			}

			sort.Sort(ByKey(intermediate))
			//finalFile := fmt.Sprintf("mr-out-%d", reply.TaskID)
			//fmt.Println("created out file") //ONLY CREATING ONE FILE
			//ofile, _ := os.Create(finalFile)

			//x := len(intermediate) =24130

			//fmt.Println(reply.TaskID, "this is the task id")

			finalFile := fmt.Sprintf("mr-out-%d", reply.TaskID)

			//fmt.Println(reply.NReduce, "this is the nreduce")
			for i := 0; i < reply.NReduce; i++ {

				finalFile := fmt.Sprintf("mr-out-%d", reply.TaskID) //this is the final file name

				//fmt.Println("created out file") //ONLY CREATING ONE FILE
				os.Create(finalFile)

			}

			//finalFile := fmt.Sprintf("mr-out-%d", reply.TaskID) //this is the final file name

			//fmt.Println("created out file") //ONLY CREATING ONE FILE
			ofile, _ := os.Create(finalFile)

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
				//fmt.Printf("reducef output: %v\n", output) very long

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			ofile.Close()
			NotifyReduceSuccess(reply.TaskID, reply.Files)
			//fmt.Print("notified reduce sucess")

			reply.TaskStatus = 2
		} else {
			break
		}
	}
}

func makeIntermediate(splice []KeyValue, nReduce int) [][]KeyValue { //helper function to partition KeyValue pairs by reduce task
	temp := make([][]KeyValue, nReduce)
	for _, x := range splice {
		v := ihash(x.Key) % nReduce
		temp[v] = append(temp[v], x)
	}
	return temp
}

func taskDone(reduceIndex int, file string) {
	args := IntermediateArgs{}
	args.File = file
	args.ReduceIndex = reduceIndex
	reply := ReplyStorage{}                                //not sure
	call("Coordinator.AddIntermediateFile", &args, &reply) //Unable to Call Coordinator.AddIntermediateFile - Got error: unexpected EOF
	//fmt.Print("task done")
}

func NotifyMapSuccess(filename string, TaskID int, files []string) {
	args := NotifyMapSuccessArgs{} //reserve
	args.File = filename
	args.TaskID = TaskID
	reply := ReplyStorage{}                      //reserve for here only
	call("Coordinator.MapSucess", &args, &reply) //Unable to Call Coordinator.MapSucess - Got error: unexpected EOF
}

func NotifyReduceSuccess(TaskID int, files []string) {
	args := NotifyReduceSucessArgs{}
	args.TaskID = TaskID
	args.File = files[0]
	reply := ReplyStorage{}
	//fmt.Println("nofitying reduce success")
	call("Coordinator.ReduceSucess", &args, &reply) //Unable to Call Coordinator.ReduceSucess - Got error: unexpected EOF
}

// example function to show how to make an RPC call to the coordinator.
// the RPC argument and reply types are defined in rpc.go.

func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the coordinator, wait for the response.
// DO NOT MODIFY
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

	fmt.Println("Unable to Call", rpcname, "- Got error:", err)
	return false
}
