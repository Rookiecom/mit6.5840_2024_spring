package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	ACCOMPLISH = iota
	NOTSTART
	INPROCESS
)

type mapTask struct {
	filename string
	state    int
	c        chan (int)
}

type reduceTask struct {
	number int
	state  int
	c      chan (int)
}

type Coordinator struct {
	// Your definitions here.
	nreduce     int
	mapRate     int
	reduceRate  int
	mapWorks    []mapTask
	mapLock     sync.Mutex
	reduceWorks []reduceTask
	reduceLock  sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetWork(req GetWorkReq, resp *GetWorkResp) error {
	if c.mapRate != len(c.mapWorks) {
		c.mapLock.Lock()
		defer c.mapLock.Unlock()
		for idx, val := range c.mapWorks {
			if val.state == NOTSTART {
				resp.Nreduce = c.nreduce
				resp.Number = idx
				resp.File = val.filename
				resp.Sleep = false
				resp.TASK = TASK_MAP

				c.mapWorks[idx].state = INPROCESS

				go func(number int) {
					ticker := time.NewTicker(10 * time.Second)
					defer ticker.Stop()
					select {
					case <-ticker.C:
						c.mapWorks[number].state = NOTSTART
					case <-c.mapWorks[number].c:
						break
					}
				}(idx)

				return nil
			}
		}
		resp.Sleep = true
		return nil
	}
	c.reduceLock.Lock()
	defer c.reduceLock.Unlock()
	for idx, val := range c.reduceWorks {
		if val.state == NOTSTART {
			resp.Nreduce = nreduce
			resp.Number = idx
			resp.Sleep = false
			resp.TASK = TASK_REDUCE

			c.reduceWorks[idx].state = INPROCESS

			go func(number int) {
				ticker := time.NewTicker(10 * time.Second)
				defer ticker.Stop()
				select {
				case <-ticker.C:
					c.reduceWorks[number].state = NOTSTART
				case <-c.reduceWorks[number].c:
					break
				}
			}(idx)

			return nil
		}
		resp.Sleep = true
	}

	return nil
}

func (c *Coordinator) MapDone(req MapDoneReq, resp *MapDoneResp) error {
	// fmt.Printf("file %v map finish\n", c.mapWorks[req.Number].filename)
	c.mapLock.Lock()
	defer c.mapLock.Unlock()
	c.mapWorks[req.Number].state = ACCOMPLISH
	c.mapRate++

	select {
	case c.mapWorks[req.Number].c <- 1:
	default:
		break
	}
	return nil
}

func (c *Coordinator) ReduceDone(req ReduceDoneReq, resp *ReduceDoneResp) error {
	// fmt.Printf("number %v reduce finish\n", req.Number)
	c.reduceLock.Lock()
	defer c.reduceLock.Unlock()
	c.reduceWorks[req.Number].state = ACCOMPLISH
	c.reduceRate++
	select {
	case c.reduceWorks[req.Number].c <- 1:
	default:
		break
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
	ret := false

	if c.reduceRate == len(c.reduceWorks) {
		ret = true
	}

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nreduce:    nReduce,
		mapRate:    0,
		reduceRate: 0,
	}
	for _, file := range files {
		c.mapWorks = append(c.mapWorks, mapTask{
			state:    NOTSTART,
			filename: file,
			c:        make(chan int),
		})
	}

	for i := 0; i < nReduce; i++ {
		c.reduceWorks = append(c.reduceWorks, reduceTask{
			state:  NOTSTART,
			number: i,
			c:      make(chan int),
		})
	}
	// Your code here.

	c.server()
	return &c
}
