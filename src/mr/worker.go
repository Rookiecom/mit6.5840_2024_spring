package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

var (
	nreduce int
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

func mapWork(mapf func(string, string) []KeyValue, filename string, number int) {
	// fmt.Printf("number %v start to map %v\n", number, filename)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal("open ", filename, " failure")
		return
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	mapResult := mapf(filename, string(content))

	tmpFileMap := make(map[int]*os.File)
	tmpEncMap := make(map[int]*json.Encoder)

	for _, val := range mapResult {
		h := ihash(val.Key) % nreduce
		_, ok := tmpFileMap[h]
		if !ok {
			oname := fmt.Sprintf("mr-%v-%v", number, h)
			ofile, _ := os.OpenFile(oname, os.O_CREATE|os.O_WRONLY, 0644)
			tmpFileMap[h] = ofile
			tmpEncMap[h] = json.NewEncoder(ofile)
		}
		tmpEncMap[h].Encode(val)
	}

	mapDoneReq := MapDoneReq{
		Number: number,
	}
	mapDoneResp := MapDoneResp{}

	call("Coordinator.MapDone", &mapDoneReq, &mapDoneResp)

	return
}

func reduceWork(reducef func(string, []string) string, number int) {
	// fmt.Printf("number %v start to reduce \n", number)

	matchString := fmt.Sprintf("./mr-*-%v", number)
	matchFiles, _ := filepath.Glob(matchString)

	intermediate := []KeyValue{}

	for _, tmpFile := range matchFiles {
		// fmt.Printf("%v get %v\n", number, tmpFile)
		ofile, err := os.Open(tmpFile)
		if err != nil {
			log.Fatal(err)
		}

		dec := json.NewDecoder(ofile)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		ofile.Close()
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%v", number)
	ofile, _ := os.OpenFile(oname, os.O_CREATE|os.O_WRONLY, 0644)

	defer ofile.Close()
	for i := 0; i < len(intermediate); {
		var j int
		for j = i; j < len(intermediate) && intermediate[j].Key == intermediate[i].Key; j++ {
		}

		values := []string{}

		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)
		// fmt.Printf("finish one key %v\n", intermediate[i].Key)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	// fmt.Println("finish reduce work, going to report to coordinator")
	reduceDoneReq := ReduceDoneReq{
		Number: number,
	}

	reduceDoneResp := ReduceDoneResp{}

	call("Coordinator.ReduceDone", &reduceDoneReq, &reduceDoneResp)

	return
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		workReq := GetWorkReq{}
		workResp := GetWorkResp{}
		// fmt.Println("prepare to get work")
		flag := call("Coordinator.GetWork", &workReq, &workResp)

		if !flag {
			log.Fatal("get work rpc request failure")
			break
		}

		if workResp.Sleep {
			time.Sleep(5 * time.Second)
			continue
		}
		nreduce = workResp.Nreduce
		switch workResp.TASK {
		case TASK_MAP:
			mapWork(mapf, workResp.File, workResp.Number)
		case TASK_REDUCE:
			reduceWork(reducef, workResp.Number)
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

	fmt.Printf("rpc %v %v\n", rpcname, err)
	return false
}
