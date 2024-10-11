package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strings"
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
	isDone := false
	for !isDone {
		resp := GetJob()
		if resp.IsFinished {
			isDone = true
			log.Printf("Done with this, bye bye")
			continue
		}

		if resp.MapJob != nil {
			DoMapping(resp.MapJob, mapf)
		}

		if resp.ReduceJob != nil {
			DoReducing(resp.ReduceJob, reducef)
		}
	}
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
	// open each partitioned file and then write to the file... but wont this make the file appending risky in case of appending to same files?
	// to avoid the writing to same file at the same time by multiple different threads
	// 1. map job -> R files -> add shuffle/collect and create reduce-tmp file stage in reduction jobs.
	// 2. write through a common method with locks.
	// i think we should go with method 1? if method 1 is done following will be true. also this is true in sense of acting as node instead of thread.
	// 1. you can run a collect/shuffle job and collect all the data by "completed" map job. & then this map file should never be processed. (call this stage as finished.)
	// 2. you will need to give each worker its own directory so it will write to this directory & then be done with everything? But then before starting reduction the
	// state of all the mapjobs should be finished/shuffled.
	// with method 2 upside is that at least for this scope it will be easier to process & write to the files as I am working in multithread and not multi node system.
	partitionedKva := make([][]KeyValue, job.ReducerCount)

	for _, v := range kva {
		partitionKey := ihash(v.Key) % job.ReducerCount
		partitionedKva[partitionKey] = append(partitionedKva[partitionKey], v)
	}

	intermediateFiles := make([]string, job.ReducerCount)
	inputFilename := filepath.Base(job.InputFile)
	inputFilename = strings.Split(inputFilename, ".")[0]

	for i := 0; i < job.ReducerCount; i++ {
		// 	oname := "mr-{mapjobfilename}-0"
		// always check if similar file is already there, it is truncated and then rewritten so no worries about using the filename.
		intermediateFile := fmt.Sprintf("mr-%v-%v.txt", inputFilename, i)
		intermediateFiles[i] = intermediateFile
		oFile, _ := os.Create(intermediateFile)

		b, err := json.Marshal(partitionedKva[i])
		if err != nil {
			log.Printf("JSON error", err)
		}

		oFile.Write(b)
		oFile.Close()
	}

	//TODO report that this is done
	ReportMapResult(MapResult{InputFile: filename, IntermediateFile: intermediateFiles, WorkerId: os.Getpid()})
}

func DoReducing(job *ReduceJob, reducef func(string, []string) string) {
	intermediate := []KeyValue{}

	for _, file := range job.IntermediateFiles {
		dat, err := ioutil.ReadFile(file)
		if err != nil {
			log.Println("Read Error: ", err.Error())
		}
		var kva []KeyValue
		err = json.Unmarshal(dat, &kva)
		if err != nil {
			log.Println("Unmarshalling error: ", err.Error())
		}

		intermediate = append(intermediate, kva...)
	}

	sort.Sort(ByKey(intermediate))
	ofname := fmt.Sprintf("mr-out-%v", job.ReducerNumber)
	tempFile, err := ioutil.TempFile(".", ofname)
	if err != nil {
		log.Println("error creating temp file")
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
	ReportReduceResult(ReduceResult{WorkerId: os.Getpid(), ReducerCount: job.ReducerNumber})
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.

func GetJob() GetJobResponse {
	req := GetJobRequest{}
	req.WorkerId = os.Getpid()

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
