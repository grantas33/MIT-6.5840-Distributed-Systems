package mr

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func Mapper(filename string, nReduce int, mapf func(string, string) []KeyValue) {

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	chunks := make(map[int][]KeyValue)
	kva := mapf(filename, string(content))
	for _, kv := range kva {
		idx := ihash(kv.Key) % nReduce
		chunks[idx] = append(chunks[idx], kv)
	}

	for idx, chunk := range chunks {
		filename := fmt.Sprintf("mr-%d-%s.json", idx, filepath.Base(filename))

		if _, err := os.Stat(filename); os.IsNotExist(err) {
			// File does not exist, create it
			file, err := os.Create(filename)
			if err != nil {
				fmt.Println("Error creating file:", err)
				return
			}

			enc := json.NewEncoder(file)

			for _, kv := range chunk {
				err := enc.Encode(&kv)
				if err != nil {
					fmt.Println("Error writing to file:", err)
					return
				}
			}
			file.Close()

		} else {
			fmt.Printf("File %s already exists. Skipping creation.", filename)
		}
	}

}

func Reducer(reduceIdx int, reducef func(string, []string) string) {

	globPattern := fmt.Sprintf("mr-%v-*", reduceIdx)

	files, err := filepath.Glob(globPattern)
	if err != nil {
		fmt.Println("Error finding files:", err)
		return
	}

	var intermediate []KeyValue

	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			fmt.Println("Error opening file:", err)
			continue
		}

		// Create a JSON decoder for the file
		dec := json.NewDecoder(f)

		var tempArray []KeyValue
		for dec.More() {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				fmt.Println("Error decoding file content:", err)
				break
			}
			tempArray = append(tempArray, kv)
		}
		f.Close()

		intermediate = append(intermediate, tempArray...)
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%v", reduceIdx)
	ofile, _ := os.Create(oname)

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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	gob.Register(ReduceTask{})
	gob.Register(IdleTask{})
	gob.Register(StopTask{})
	gob.Register(MapTask{})

	var reply Task

	for true {
		ok := call("Coordinator.GetTask", TaskArgs{Pid: os.Getpid()}, &reply)
		if ok {
			switch t := reply.(type) {
			case MapTask:
				Mapper(t.Filename, t.NReduce, mapf)
			case ReduceTask:
				Reducer(t.ReduceIdx, reducef)
			case IdleTask:
				time.Sleep(1 * time.Second)
			default:
				os.Exit(0)
			}
		} else {
			fmt.Printf("GetTask failed, exiting!\n")
			os.Exit(0)
		}
	}

	// call coordinator - i'm available for a task
	// coordinator responds with a task type, task related args

	// mapper:
	// produce Nreduce intermediate files

	// reducer:
	// merge and sort intermediate files belonging to this reducer
	// while iterating through key-value pairs, group by keys and reduce
	// put output to mr-out-x

	// worker count can be independent of desired mapper or reducer count

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
