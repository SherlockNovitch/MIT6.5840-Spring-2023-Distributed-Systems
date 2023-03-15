package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

type KeyValue struct {
	Key   string
	Value string
}

type KeyWithValueList struct {
	Key   string
	Value []string
}

// KeyValueList for sorting
type KeyValueList []KeyValue

func (a KeyValueList) Len() int           { return len(a) }
func (a KeyValueList) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a KeyValueList) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use fnvHash(key) % NReduce to choose reduce Task number for each KeyValue emitted by Map.
func fnvHash(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func readFile(filename string) string {
	file, err := os.Open(filename)
	defer func(file *os.File) { _ = file.Close() }(file)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	return string(content)
}

func partitioning(intermediateKeyValues []KeyValue, numReduce int, taskId int) []string {
	var fps []*os.File
	for i := 0; i < numReduce; i++ {
		temp, _ := os.CreateTemp("", "mr-tmp-")
		fps = append(fps, temp)
	}
	for _, keyValue := range intermediateKeyValues {
		if _, err := fmt.Fprintf(fps[fnvHash(keyValue.Key)%numReduce], "%v %v\n", keyValue.Key, keyValue.Value); err != nil {
			log.Fatalf("cannot write tmp Files")
		}
	}
	var intermediateFileNames []string
	for index, fp := range fps {
		_ = fp.Close()
		newFileName := fmt.Sprintf("mr-itermediate-%v-%v.txt", index, taskId)
		if err := os.Rename(fp.Name(), fmt.Sprintf("mr-itermediate-%v-%v.txt", index, taskId)); err != nil {
			log.Fatalf("cannot  rename %s to %s", fp.Name(), newFileName)
		}
		intermediateFileNames = append(intermediateFileNames, newFileName)
	}
	return intermediateFileNames
}

func handleMapTask(task *Task, mapFunction func(string, string) []KeyValue) {
	var intermediateKeyValues []KeyValue
	for _, filename := range task.Files {
		intermediateKeyValues = append(intermediateKeyValues, mapFunction(filename, readFile(filename))...)
	}
	callCompleteTask(task, partitioning(intermediateKeyValues, task.NumReduce, task.Id))
}

func readIntermediateFiles(task *Task) KeyValueList {
	var intermediateKeyValues KeyValueList
	for _, filename := range task.Files {
		for _, line := range strings.Split(readFile(filename), "\n") {
			var key, value string
			if _, err := fmt.Sscanf(line, "%v %v", &key, &value); err == nil {
				intermediateKeyValues = append(intermediateKeyValues, KeyValue{key, value})
			}
		}
	}
	return intermediateKeyValues
}

func aggregation(intermediateKeyValues KeyValueList) []KeyWithValueList {
	sort.Sort(intermediateKeyValues)
	var keyWithValueListList []KeyWithValueList
	for i := 0; i < len(intermediateKeyValues); {
		var values []string
		j := i
		for ; j < len(intermediateKeyValues) && intermediateKeyValues[j].Key == intermediateKeyValues[i].Key; j++ {
			values = append(values, intermediateKeyValues[j].Value)
		}
		keyWithValueListList = append(keyWithValueListList, KeyWithValueList{Key: intermediateKeyValues[i].Key, Value: values})
		i = j
	}
	return keyWithValueListList
}

func handleReduceTask(task *Task, reduceFunction func(string, []string) string) {
	keyWithValueListList := aggregation(readIntermediateFiles(task))
	outputFile, _ := os.Create(fmt.Sprintf("mr-out-%v", task.ReduceNum))
	for _, klv := range keyWithValueListList {
		_, _ = fmt.Fprintf(outputFile, "%v %v\n", klv.Key, reduceFunction(klv.Key, klv.Value))
	}
	callCompleteTask(task, nil)
}

func Worker(mapFunction func(string, string) []KeyValue, reduceFunction func(string, []string) string) {
	for {
		if task := callRequestTask(); task != nil {
			if task.IsMapTask {
				handleMapTask(task, mapFunction)
			} else {
				handleReduceTask(task, reduceFunction)
			}
		} else {
			time.Sleep(time.Duration(3) * time.Second)
		}
	}
}

func callCompleteTask(task *Task, intermediateFileNames []string) {
	args := CompleteTaskArgs{TaskID: task.Id, IsMapTask: task.IsMapTask, IntermediateFiles: intermediateFileNames}
	if ok := call("Coordinator.CompleteTask", &args, &Empty{}); !ok {
		fmt.Printf("call CompleteTask failed!\n")
	}
}

func callRequestTask() *Task {
	reply := new(RequestTaskReply)
	if ok := call("Coordinator.RequestTask", &Empty{}, reply); !ok {
		fmt.Printf("call RequestTask failed!\n")
	}
	return reply.Task
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcName string, args interface{}, reply interface{}) bool {
	sockName := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockName)
	if err != nil {
		os.Exit(0)
	}
	defer func(c *rpc.Client) { _ = c.Close() }(c)
	err = c.Call(rpcName, args, reply)
	if err == nil {
		return true
	}
	fmt.Println(err)
	return false
}
