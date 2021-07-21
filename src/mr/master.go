package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var taskStates [3]string = [3]string{"added", "pending", "finished"}

type Master struct {
	nReduce       int
	mapTask       map[string]string
	mapTaskStates map[string]string
	mapTaskTime   map[string]time.Time

	reduceTask       map[string][]string
	reduceTaskStates map[string]string
	reduceTaskTime   map[string]time.Time

	taskCounter    int
	mapFinished    bool
	reduceFinished bool
	mu             sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Request(args *WorkerArgs, reply *MasterReply) error {
	if args.Request == WorkerToMasterMsg[2] {
		reply.Content = []string{strconv.Itoa(m.nReduce)}
		return nil
	}

	if args.Request == WorkerToMasterMsg[0] && !m.mapFinished {
		taskId, found := m.getMapTask()
		if found {
			reply.TaskId = taskId
			reply.Task = MasterToWorkerMsg[0]
			reply.Content = []string{m.mapTask[taskId]}
		} else {
			reply.Task = MasterToWorkerMsg[4]
		}

		return nil
	}

	if args.Request == WorkerToMasterMsg[0] && !m.reduceFinished {
		taskId, found := m.getReduceTask()
		if found {
			reply.TaskId = taskId
			reply.Task = MasterToWorkerMsg[1]
			reply.Content = m.reduceTask[taskId]
		} else {
			reply.Task = MasterToWorkerMsg[4]
		}

		return nil
	}

	if args.Request == WorkerToMasterMsg[1] {
		taskId := args.Content[0]
		taskType := args.Content[1]
		m.mu.Lock()
		m.updateTaskStatus(taskId, taskType, taskStates[2])
		m.mu.Unlock()
		if taskType == MasterToWorkerMsg[0] {
			m.addReduceTask(args.Content[2:])
		}
		reply.Task = MasterToWorkerMsg[3]
		return nil
	}

	if m.mapFinished && m.reduceFinished {
		reply.Task = MasterToWorkerMsg[2]
		return nil
	}

	for _, s := range WorkerToMasterMsg {
		if s == args.Request {
			return nil
		}
	}

	return errors.New("request not found")
}

func (m *Master) createMapTask(filenames []string) error {

	for _, filename := range filenames {
		taskId := strconv.Itoa(m.taskCounter)
		m.mapTask[taskId] = filename
		m.mapTaskStates[taskId] = taskStates[0]
		m.taskCounter += 1
	}
	return nil
}

func (m *Master) addReduceTask(filenames []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, filename := range filenames {
		reduceTaskStr := strings.FieldsFunc(filename, func(r rune) bool { return r == '-' })
		reduceTaskId := reduceTaskStr[len(reduceTaskStr)-1]
		tasks, ok := m.reduceTask[reduceTaskId]
		if ok {
			m.reduceTask[reduceTaskId] = append(tasks, filename)
		} else {
			m.reduceTask[reduceTaskId] = []string{filename}
		}
		m.reduceTaskStates[reduceTaskId] = taskStates[0]
	}
	return nil
}

func (m *Master) getMapTask() (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for k, v := range m.mapTaskStates {
		if v == taskStates[0] {
			m.updateTaskStatus(k, "map", taskStates[1])
			return k, true
		}
	}
	return "", false
}

func (m *Master) getReduceTask() (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for k, v := range m.reduceTaskStates {
		if v == taskStates[0] {
			m.updateTaskStatus(k, "reduce", taskStates[1])
			return k, true
		}
	}
	return "", false
}

func (m *Master) updateTaskStatus(taskId string, taskType string, status string) error {

	if taskType == "map" {
		m.mapTaskStates[taskId] = status
		m.mapTaskTime[taskId] = time.Now()
	}
	if taskType == "reduce" {
		m.reduceTaskStates[taskId] = status
		m.reduceTaskTime[taskId] = time.Now()
	}
	return nil
}

func (m *Master) checkStatus() error {

	var durThreshold float64 = 10

	ticker := time.NewTicker(time.Millisecond * 500)
	defer ticker.Stop()

	for range ticker.C {
		var jobsLeft int = 0
		if !m.mapFinished {
			var mapFinished bool = true
			for k, v := range m.mapTaskStates {
				if v == taskStates[1] && time.Since(m.mapTaskTime[k]).Seconds() > durThreshold {
					m.mapTaskStates[k] = taskStates[0]
					m.mapTaskTime[k] = time.Now()
				}
				if v != taskStates[2] {
					mapFinished = false
				}
				if v == taskStates[0] {
					jobsLeft += 1
				}
			}
			m.mapFinished = mapFinished
		} else if !m.reduceFinished {
			var reduceFinished bool = true
			for k, v := range m.reduceTaskStates {
				if v == taskStates[1] && time.Since(m.reduceTaskTime[k]).Seconds() > durThreshold {
					m.reduceTaskStates[k] = taskStates[0]
					m.reduceTaskTime[k] = time.Now()
				}
				if v != taskStates[2] {
					reduceFinished = false
				}
				if v == taskStates[0] {
					jobsLeft += 1
				}
			}
			m.reduceFinished = reduceFinished
		} else {
			break
		}
		fmt.Printf("master status. map finished: %v; reduce finished: %v\n", m.mapFinished, m.reduceFinished)
		if !m.mapFinished {
			fmt.Printf("remaining map jobs: %v\n", jobsLeft)
		} else if !m.reduceFinished {
			fmt.Printf("remaining reduce jobs: %v\n", jobsLeft)
		}
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	if m.mapFinished && m.reduceFinished {
		ret = true
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		mapTask:       make(map[string]string),
		mapTaskStates: make(map[string]string),
		mapTaskTime:   make(map[string]time.Time),

		reduceTask:       make(map[string][]string),
		reduceTaskStates: make(map[string]string),
		reduceTaskTime:   make(map[string]time.Time),
	}

	// Your code here.
	m.nReduce = nReduce
	m.createMapTask(files)
	m.server()
	go m.checkStatus()

	return &m
}
