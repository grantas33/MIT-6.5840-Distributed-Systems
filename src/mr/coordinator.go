package mr

import (
	"encoding/gob"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	nReduce       int
	dll           *DoublyLinkedList
	mapFiles      []string
	mapTaskIdx    int
	reduceTaskIdx int
	mutex         sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) isUnresponsiveTaskExists(currentTs int64) bool {
	return c.dll.GetLastNodeValue() != nil && currentTs-c.dll.GetLastNodeValue().startTs >= 10
}

// Helper function to handle mapping or reducing phase
func (c *Coordinator) handlePhase(pid int, reply *Task, now int64, phaseIdx *int, taskType string) error {
	taskId := *phaseIdx
	if c.isUnresponsiveTaskExists(now) {
		taskId = c.dll.GetLastNodeValue().taskId
		c.dll.RemoveNode(c.dll.GetLastNodeValue().pid)
	} else {
		*phaseIdx++
	}

	var newTask Task
	if taskType == "mapping" {
		newTask = MapTask{Filename: c.mapFiles[taskId], NReduce: c.nReduce}
	} else if taskType == "reducing" {
		newTask = ReduceTask{ReduceIdx: taskId}
	}

	c.dll.AddNode(TaskData{pid: pid, startTs: now, taskId: taskId})
	*reply = newTask

	return nil
}

// Mapping phase handlers
func (c *Coordinator) handleMappingPhase(pid int, reply *Task, now int64) error {
	return c.handlePhase(pid, reply, now, &c.mapTaskIdx, "mapping")
}

func (c *Coordinator) waitUntilMappersFinish(pid int, reply *Task, now int64) error {
	if c.isUnresponsiveTaskExists(now) {
		return c.handlePhase(pid, reply, now, &c.mapTaskIdx, "mapping")
	} else {
		*reply = IdleTask{}
	}
	return nil
}

// Reducing phase handlers
func (c *Coordinator) handleReducerPhase(pid int, reply *Task, now int64) error {
	return c.handlePhase(pid, reply, now, &c.reduceTaskIdx, "reducing")
}

func (c *Coordinator) waitUntilReducersFinish(pid int, reply *Task, now int64) error {
	if c.isUnresponsiveTaskExists(now) {
		return c.handlePhase(pid, reply, now, &c.reduceTaskIdx, "reducing")
	} else {
		*reply = IdleTask{}
	}
	return nil
}

func (c *Coordinator) GetTask(args *TaskArgs, reply *Task) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	pid := args.Pid

	now := time.Now().Unix()
	c.dll.RemoveNode(pid)

	if c.mapTaskIdx < len(c.mapFiles) {
		c.handleMappingPhase(pid, reply, now)
	} else if c.dll.GetSize() > 0 && c.reduceTaskIdx == 0 {
		c.waitUntilMappersFinish(pid, reply, now)
	} else if c.reduceTaskIdx < c.nReduce {
		c.handleReducerPhase(pid, reply, now)
	} else if c.dll.GetSize() > 0 && c.reduceTaskIdx == c.nReduce {
		c.waitUntilReducersFinish(pid, reply, now)
	} else if c.reduceTaskIdx == c.nReduce {
		*reply = StopTask{}
	}

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
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.dll.GetSize() == 0 && c.reduceTaskIdx == c.nReduce
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	c := Coordinator{
		nReduce:       nReduce,
		dll:           NewDoublyLinkedList(),
		mapFiles:      files,
		mapTaskIdx:    0,
		reduceTaskIdx: 0,
	}

	gob.Register(ReduceTask{})
	gob.Register(IdleTask{})
	gob.Register(StopTask{})
	gob.Register(MapTask{})

	// Your code here.

	c.server()
	return &c
}
