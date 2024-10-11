package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// example to show how to declare the arguments
// and reply for an RPC.
type MapJob struct {
	InputFile         string
	ReducerCount      int
	IntermediateFiles []string
}

type ReduceJob struct {
	ReducerNumber     int
	IntermediateFiles []string // locations of the intermediate files
}

type GetJobRequest struct {
	WorkerId int
}

type GetJobResponse struct {
	MapJob     *MapJob
	ReduceJob  *ReduceJob
	IsFinished bool
}

type MapResult struct {
	InputFile        string
	IntermediateFile []string
	WorkerId         int
}

type ReduceResult struct {
	WorkerId     int
	ReducerCount int
}

type EmptyResponse struct {
}

type RegisterWorker struct {
	WorkerId int
}

type RegisterWorkerResponse struct {
	WorkerId   int
	AssignedId int
}

type WorkerHealth struct {
	WorkerId string
}

type WorkerHealthResponse struct {
	Status string
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
