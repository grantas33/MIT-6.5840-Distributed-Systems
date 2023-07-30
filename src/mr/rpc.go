package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// Add your RPC definitions here.

type TaskArgs struct {
	Pid int
}

type Task interface {
	TaskType() string
}

type MapTask struct {
	Filename string
	NReduce  int
}

func (m MapTask) TaskType() string {
	return "map"
}

type ReduceTask struct {
	ReduceIdx int
}

func (r ReduceTask) TaskType() string {
	return "reduce"
}

type IdleTask struct{}

func (i IdleTask) TaskType() string {
	return "idle"
}

type StopTask struct{}

func (i StopTask) TaskType() string {
	return "stop"
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
