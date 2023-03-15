package mr

import (
	"container/list"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Task struct {
	Id        int
	Files     []string
	StartTime time.Time
	IsMapTask bool
	NumReduce int //only effective when IsMapTask is true
	ReduceNum int //only effective when IsMapTask is false
}

type TaskPool struct {
	readyList   list.List
	runningList list.List
	mutex       sync.Mutex
}

func (p *TaskPool) addTask(taskAdded *Task) {
	p.readyList.PushBack(taskAdded)
}

func (p *TaskPool) getTask() *Task {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.readyList.Len() > 0 {
		taskElement := p.readyList.Front()
		p.readyList.Remove(taskElement)
		taskElement.Value.(*Task).StartTime = time.Now()
		return p.runningList.PushBack(taskElement.Value.(*Task)).Value.(*Task)
	}
	return nil
}

func (p *TaskPool) completeTask(taskId int) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	for e := p.runningList.Front(); e != nil; e = e.Next() {
		if e.Value.(*Task).Id == taskId {
			p.runningList.Remove(e)
			break
		}
	}
}

func (p *TaskPool) isAllDone() bool {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return (p.readyList.Len() + p.runningList.Len()) == 0
}

func (p *TaskPool) failureCheck() {
	for !p.isAllDone() {
		p.mutex.Lock()
		tmpList := list.List{}
		for e := p.runningList.Front(); e != nil; e = e.Next() {
			if time.Now().Sub(e.Value.(*Task).StartTime).Seconds() > 10 {
				tmpList.PushFront(e)
			}
		}
		for e := tmpList.Front(); e != nil; e = e.Next() {
			p.readyList.PushBack(p.runningList.Remove(e.Value.(*list.Element)).(*Task))
		}
		p.mutex.Unlock()
		time.Sleep(time.Duration(5) * time.Second)
	}
}

type Coordinator struct {
	numReduce         int
	taskCounter       int
	intermediateFiles [][]string
	mapTaskPool       TaskPool
	reduceTaskPool    TaskPool
}

func (c *Coordinator) initMapTask(files []string, nReduce int) {
	c.intermediateFiles = make([][]string, nReduce)
	for _, file := range files {
		task := Task{Id: c.taskCounter, Files: []string{file}, IsMapTask: true, NumReduce: nReduce}
		c.taskCounter += 1
		c.mapTaskPool.addTask(&task)
	}
	go c.mapTaskPool.failureCheck()

}

func (c *Coordinator) initReduceTask() {
	for index, files := range c.intermediateFiles {
		task := Task{Id: c.taskCounter, Files: files, StartTime: time.Now(), IsMapTask: false, ReduceNum: index}
		c.taskCounter += 1
		c.reduceTaskPool.addTask(&task)
	}
	go c.reduceTaskPool.failureCheck()
}

func (c *Coordinator) startJob(files []string, nReduce int) {
	c.initMapTask(files, nReduce)
	c.server()
	for !c.mapTaskPool.isAllDone() {
		time.Sleep(time.Duration(1) * time.Second)
	}
	c.initReduceTask()
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	_ = rpc.Register(c)
	rpc.HandleHTTP()
	sockName := coordinatorSock()
	_ = os.Remove(sockName)
	l, e := net.Listen("unix", sockName)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go func() { _ = http.Serve(l, nil) }()
}

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *Empty) error {
	if args.IsMapTask {
		c.mapTaskPool.completeTask(args.TaskID)
		for index, file := range args.IntermediateFiles {
			c.intermediateFiles[index] = append(c.intermediateFiles[index], file)
		}
	} else {
		c.reduceTaskPool.completeTask(args.TaskID)
	}
	return nil
}

func (c *Coordinator) RequestTask(args *Empty, reply *RequestTaskReply) error {
	if task := c.mapTaskPool.getTask(); task != nil {
		reply.Task = task
	} else {
		reply.Task = c.reduceTaskPool.getTask()
	}
	return nil
}

// Done called by main/mr coordinator.go periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.mapTaskPool.isAllDone() && c.reduceTaskPool.isAllDone()
}

// MakeCoordinator create a Coordinator.
// main/mr coordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.startJob(files, nReduce)
	return &c
}
