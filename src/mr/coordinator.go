package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	mapJobStatus      map[string]string
	reduceJobStatus   map[int]string // map of number to some string
	intermediateFiles map[int][]string
	mapTaskNumber     int
	nReducer          int
	mu                sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestJob(request *GetJobRequest, response *GetJobResponse) error {
	c.mu.Lock()

	mapJob := c.GetMapJob()

	if mapJob != nil {
		response.MapJob = mapJob
		response.IsFinished = false
		c.mu.Unlock()
		return nil
	}

	reduceJob := c.GetReduceJob()

	if reduceJob != nil {
		response.ReduceJob = reduceJob
		response.IsFinished = false
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()
	response.IsFinished = true
	return nil
}

func (c *Coordinator) GetReduceJob() *ReduceJob {
	reducer := -1
	for i, v := range c.reduceJobStatus {
		if v == "pending" {
			reducer = i
			break
		}
	}

	if reducer < 0 {
		return nil
	}

	job := &ReduceJob{}
	job.ReducerCount = reducer
	job.IntermediateFiles = c.intermediateFiles[reducer]
	c.reduceJobStatus[reducer] = "running"
	return job
}

func (c *Coordinator) GetMapJob() *MapJob {
	for filename, status := range c.mapJobStatus {
		if status == "pending" {
			job := &MapJob{}
			job.InputFile = filename
			job.ReducerCount = c.nReducer
			job.MapJobNumber = c.mapTaskNumber
			c.mapJobStatus[filename] = "running"
			c.mapTaskNumber++
			return job
		}
	}
	return nil
}

func (c *Coordinator) ReportMapResult(req *ReportMapJobRequest, resp *EmptyResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mapJobStatus[req.InputFile] = "completed"
	for r := 0; r < c.nReducer; r++ {
		c.intermediateFiles[r] = append(c.intermediateFiles[r], req.IntermediateFile[r])
	}
	return nil
}

func (c *Coordinator) ReportReduceResult(req *ReportReduceJobResult, resp *EmptyResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reduceJobStatus[req.ReduceNumber] = "completed"
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
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, v := range c.reduceJobStatus {
		if v != "completed" {
			return false
		}
	}
	// Your code here.

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.mapTaskNumber = 0
	c.mapJobStatus = make(map[string]string)
	c.nReducer = nReduce
	for _, v := range files {
		c.mapJobStatus[v] = "pending"
	}
	// Your code here.

	c.reduceJobStatus = make(map[int]string)

	for i := 0; i < nReduce; i++ {
		c.reduceJobStatus[i] = "pending"
	}

	c.intermediateFiles = make(map[int][]string)

	c.server()
	return &c
}
