package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Status struct {
	StartTime int64
	Status    string
}

type Coordinator struct {
	// Your definitions here.
	mapJobStatus      map[string]*Status
	reduceJobStatus   map[int]*Status // map of reducerNumber to Status of job
	intermediateFiles map[string][]string
	nReducer          int
	mu                sync.Mutex
	workerStatus      map[int]string
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestJob(request *GetJobRequest, response *GetJobResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// check if worker is in map
	dontPrint := true
	val, ok := c.workerStatus[request.WorkerId]
	if ok {
		// already exists do nothing
		if !dontPrint {
			fmt.Println("has val as " + val)
		}
	} else {
		c.workerStatus[request.WorkerId] = "idle"
	}

	mapJob, allMapDone := c.GetMapJob()

	if mapJob != nil {
		response.MapJob = mapJob
		response.IsFinished = false
		c.workerStatus[request.WorkerId] = "busy"
		return nil
	}

	if !allMapDone {
		// this means that worker will wait for all maps to be done with (this might need a healthcheck logic somewhere along the lines of time)
		return nil
	}
	reduceJob := c.GetReduceJob()

	if reduceJob != nil {
		response.ReduceJob = reduceJob
		response.IsFinished = false
		c.workerStatus[request.WorkerId] = "busy"
		return nil
	}
	response.IsFinished = true
	return nil
}

func (c *Coordinator) GetReduceJob() *ReduceJob {
	reducer := -1

	for i, v := range c.reduceJobStatus {
		if v.Status == "idle" {
			reducer = i
			break
		}
	}

	if reducer < 0 {
		return nil
	}
	job := &ReduceJob{}
	job.ReducerNumber = reducer
	job.IntermediateFiles = make([]string, len(c.mapJobStatus))
	ind := 0
	for f := range c.mapJobStatus {
		job.IntermediateFiles[ind] = c.intermediateFiles[f][reducer]
		// log.Printf("location :%v", c.intermediateFiles[f][re])
		ind++
	}
	c.reduceJobStatus[reducer] = &Status{StartTime: time.Now().Unix(), Status: "inprogress"}
	return job
}

func (c *Coordinator) GetMapJob() (*MapJob, bool) {
	fullMapDone := true
	for filename, job := range c.mapJobStatus {
		if job.Status != "completed" {
			fullMapDone = false
		}
		if job.Status == "idle" {
			job := &MapJob{}
			job.InputFile = filename
			job.ReducerCount = c.nReducer
			c.mapJobStatus[filename] = &Status{StartTime: time.Now().Unix(), Status: "inprogress"}
			return job, fullMapDone
		}

	}
	return nil, fullMapDone
}

func (c *Coordinator) ReportMapResult(req *MapResult, resp *EmptyResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.intermediateFiles[req.InputFile] = req.IntermediateFile
	c.mapJobStatus[req.InputFile].Status = "completed"
	c.workerStatus[req.WorkerId] = "idle"
	return nil
}

func (c *Coordinator) ReportReduceResult(req *ReduceResult, resp *EmptyResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reduceJobStatus[req.ReducerCount].Status = "completed"
	c.workerStatus[req.WorkerId] = "idle"
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) resetDeadJobs() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, job := range c.mapJobStatus {
		if job.Status == "inprogress" {
			now := time.Now().Unix()
			if job.StartTime > 0 && now > (job.StartTime+10) {
				c.mapJobStatus[key] = &Status{StartTime: -1, Status: "idle"}
			}
		}
	}

	for key, job := range c.reduceJobStatus {
		if job.Status == "inprogress" {
			now := time.Now().Unix()
			if job.StartTime > 0 && now > (job.StartTime+10) {
				c.reduceJobStatus[key] = &Status{StartTime: -1, Status: "idle"}
			}
		}
	}

}

// idea from earlier solution
func (c *Coordinator) StartTicker() {
	ticker := time.NewTicker(10 * time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				if c.Done() {
					return
				}
				c.resetDeadJobs()
			}
		}
	}()
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

	doneCount := c.nReducer

	for _, v := range c.reduceJobStatus {
		if v.Status != "completed" {
			doneCount--
		}
	}
	// Your code here.
	return doneCount == c.nReducer
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	//set stage for maps
	c.mapJobStatus = make(map[string]*Status)
	c.intermediateFiles = make(map[string][]string)
	c.workerStatus = make(map[int]string)
	for _, v := range files {
		c.mapJobStatus[v] = &Status{StartTime: -1, Status: "idle"}
		c.intermediateFiles[v] = make([]string, nReduce)
	}

	// set stage for reductions
	c.nReducer = nReduce
	c.reduceJobStatus = make(map[int]*Status)

	for i := 0; i < nReduce; i++ {
		c.reduceJobStatus[i] = &Status{StartTime: -1, Status: "idle"}
	}
	c.StartTicker()
	c.server()
	return &c
}
