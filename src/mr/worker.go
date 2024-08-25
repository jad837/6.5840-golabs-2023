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

// Map functions return a slice of KeyValue.
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

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func DoMapping(job *MapJob, mapf func(string, string) []KeyValue) {
	// open and get content from file and use mapf as shown in mrsequential.go
	filename := job.InputFile
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("error opening file %s", filename)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("error reading file %s", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	sort.Sort(ByKey(kva))

	// slice the kva, and use ihash to partition the output.

	partitionedKva := make([][]KeyValue, job.ReducerCount)

	for _, v := range kva {
		partitionKey := ihash(v.Key) % job.ReducerCount
		partitionedKva[partitionKey] = append(partitionedKva[partitionKey], v)
	}

	intermediateFiles := make([]string, job.ReducerCount)
	for i := 0; i < job.ReducerCount; i++ {
		// 	oname := "mr-out-0"
		intermediateFile := fmt.Sprintf("mr-%v-%v", job.MapJobNumber, i)
		intermediateFiles[i] = intermediateFile
		oFile, _ := os.Create(intermediateFile)

		b, err := json.Marshal(partitionedKva[i])
		if err != nil {
			fmt.Printf("JSON error", err)
		}

		oFile.Write(b)
		oFile.Close()
	}

	//TODO report that this is done

}

func DoReducing(job *ReduceJob, reducef func(string, []string) string) {
	intermediate := []KeyValue{}

	for _, file := range job.IntermediateFiles {
		dat, err := ioutil.ReadFile(file)
		if err != nil {
			fmt.Println("Read Error: ", err.Error())
		}
		var kva []KeyValue
		err = json.Unmarshal(dat, &kva)
		if err != nil {
			fmt.Println("Unmarshalling error: ", err.Error())
		}

		intermediate = append(intermediate, kva...)
	}

	sort.Sort(ByKey(intermediate))

	ofname := fmt.Sprintf("mr-out-%v", job.ReducerCount)
	tempFile, err := ioutil.TempFile(".", ofname)
	if err != nil {
		fmt.Println("error creating temp file")
	}
	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	// using tempfile instead of direct output file as in mrsequential.go
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
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	os.Rename(tempFile.Name(), ofname)

	// TODO report this shit is done
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.

func GetJob() GetJobResponse {
	req := GetJobRequest{}
	req.Pid = os.Getpid()

	resp := GetJobResponse{}

	call("Coordinator.RequestJob", &req, &resp)
	return resp
}

func ReportMapResult(req MapResult) EmptyResponse {
	resp := EmptyResponse{}
	call("Coordinator.ReportMapResult", &req, &resp)
	return resp
}

func ReportReduceResult(req ReduceResult) EmptyResponse {
	resp := EmptyResponse{}
	call("Coordinator.ReportReduceResult", &req, &resp)
	return resp
}

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
