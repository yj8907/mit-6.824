package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
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

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	args := WorkerArgs{
		Request: WorkerToMasterMsg[2],
	}
	reply := MasterReply{}
	if !call("Master.Request", &args, &reply) {
		log.Fatal("worker failed")
	}

	nReduce, _ := strconv.Atoi(reply.Content[0])

	count := 1
	ch := make(chan int)
	for i := 0; i < count; i++ {
		go SpawnWorker(strconv.Itoa(i), nReduce, mapf, reducef, ch)
	}

	for range ch {
		count -= 1
		if count < 1 {
			break
		}
	}

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func SpawnWorker(workerId string, nReduce int,
	mapf func(string, string) []KeyValue, reducef func(string, []string) string,
	ch chan int) {

	for {

		args := WorkerArgs{
			WorkerId: workerId,
			Request:  WorkerToMasterMsg[0],
		}

		reply := MasterReply{}

		// send the RPC request, wait for the reply.
		if !call("Master.Request", &args, &reply) {
			log.Fatalf("call failed for worker: %v\n", workerId)
		}

		if reply.Task == MasterToWorkerMsg[0] {
			intermediate := []KeyValue{}
			filename := reply.Content[0]
			file, err := os.Open(filename)

			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()

			kva := mapf(filename, string(content))
			intermediate = append(intermediate, kva...)

			// save map intermediate value
			encoders := make(map[int]*json.Encoder)
			tempFiles := make(map[int]*os.File)
			filenames := make(map[int]string)
			mapTempFilenames := []string{reply.TaskId, reply.Task}
			var interFn string
			var keyBucket int
			interFnPrefix := "mr-" + reply.TaskId
			for _, kv := range intermediate {
				keyBucket = ihash(kv.Key) % nReduce
				interFn = interFnPrefix + "-" + strconv.Itoa(keyBucket)
				enc, enc_ok := encoders[keyBucket]
				if !enc_ok {
					// file, err := os.Create(interFn)
					file, err := ioutil.TempFile("", interFn)
					if err != nil {
						log.Fatalf("cannot create %v\n", interFn)
					}
					mapTempFilenames = append(mapTempFilenames, interFn)
					enc = json.NewEncoder(file)
					encoders[keyBucket] = enc
					tempFiles[keyBucket] = file
					filenames[keyBucket] = interFn
				}
				err := enc.Encode(kv)
				if err != nil {
					os.Remove(interFn)
					log.Fatalf("cannot encode %v\n", kv)
				}
			}
			for k, file := range tempFiles {
				file.Close()
				os.Rename(file.Name(), filenames[k])
			}

			// signal master task is finished
			args = WorkerArgs{
				WorkerId: workerId,
				Request:  WorkerToMasterMsg[1],
				Content:  mapTempFilenames,
			}
			reply = MasterReply{}
			if !call("Master.Request", &args, &reply) {
				log.Fatalf("call failed for worker: %v\n", workerId)
			}

			if reply.Task != MasterToWorkerMsg[3] {
				log.Fatal("not acked")
			}

		} else if reply.Task == MasterToWorkerMsg[1] {
			kva := []KeyValue{}
			filenames := reply.Content
			for _, filename := range filenames {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v due to error: %v\n", filename, err)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
				file.Close()
			}

			sort.Sort(ByKey(kva))
			tempFn, finalFn := workerReduce(reply.TaskId, reducef, kva)
			os.Rename(tempFn, finalFn)

			args = WorkerArgs{
				WorkerId: workerId,
				Request:  WorkerToMasterMsg[1],
				Content:  []string{reply.TaskId, reply.Task},
			}
			reply = MasterReply{}
			if !call("Master.Request", &args, &reply) {
				log.Fatalf("call failed for worker: %v\n", workerId)
			}

			// fmt.Printf("worker %v call on reduce job\n", workerId)

			if reply.Task != MasterToWorkerMsg[3] {
				log.Fatal("not acked")
			}

		} else if reply.Task == MasterToWorkerMsg[2] {
			break
		} else if reply.Task == MasterToWorkerMsg[4] {
			d, _ := time.ParseDuration("500ms")
			time.Sleep(d)
		}
	}
	ch <- 1

}

func workerReduce(workerId string, reducef func(string, []string) string,
	kva []KeyValue) (string, string) {
	oname := "mr-out-" + workerId
	ofile, _ := ioutil.TempFile("", oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()
	return ofile.Name(), oname
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
