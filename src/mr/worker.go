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
	"strings"
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

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func SpawnWorker(workerId int, nReduce int,
	mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	for {

		args := WorkerArgs{
			workerId: workerId,
			request:  "jobRequest",
		}

		reply := MasterReply{}

		// send the RPC request, wait for the reply.
		call("Master.Request", &args, &reply)
		if reply.task == "map" {
			intermediate := []KeyValue{}
			filename := reply.content
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
			var interFn string
			var keyBucket int
			interFnPrefix := "mr-" + strconv.Itoa(reply.taskId)
			for _, kv := range intermediate {
				keyBucket = ihash(kv.Key) % nReduce
				interFn = interFnPrefix + "-" + strconv.Itoa(keyBucket)
				enc, enc_ok := encoders[keyBucket]
				if !enc_ok {
					file, err := os.Create(interFn)
					if err != nil {
						log.Fatalf("cannot create %v\n", interFn)
					}
					enc = json.NewEncoder(file)
					encoders[keyBucket] = enc
				}
				err := enc.Encode(kv)
				if err != nil {
					os.Remove(interFn)
					log.Fatalf("cannot encode %v\n", kv)
				}
			}

			// signal master task is finished
			args = WorkerArgs{
				workerId: workerId,
				request:  "mapFinished",
				content:  interFnPrefix,
			}
			reply = MasterReply{}
			call("Master.Request", &args, &reply)

		} else if reply.task == "reduce" {
			kva := []KeyValue{}
			filenames := strings.Fields(reply.content)
			for _, filename := range filenames {
				file, err := os.Open(filename)

				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
			}
			sort.Sort(ByKey(kva))
			workerReduce(workerId, reducef, kva)
		} else if reply.task == "terminate" {
			break
		}
	}

}

func workerReduce(workerId int, reducef func(string, []string) string,
	kva []KeyValue) {
	oname := "mr-out-" + strconv.Itoa(workerId)
	ofile, _ := os.Create(oname)

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
