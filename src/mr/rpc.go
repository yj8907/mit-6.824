package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type WorkerArgs struct {
	WorkerId string
	Request  string
	Content  []string
}

type MasterReply struct {
	TaskId  string
	Task    string
	Content []string
}

var MasterToWorkerMsg [5]string = [5]string{"map", "reduce", "terminate", "ack", "wait"}
var WorkerToMasterMsg [3]string = [3]string{"jobRequest", "finished", "init"}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
