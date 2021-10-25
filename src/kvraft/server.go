package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key    string
	Value  string
	OpType string
	Version  int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	pendingRequests map[int][]*sync.Cond
	pendingRaftCmds map[int][]*sync.Cond
	state           map[string]string
	keyVersionSuccess  map[string]map[int]bool
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	reply.Err = ErrWrongLeader
	command := Op{
		Key:    args.Key,
		Value:  args.Value,
		OpType: args.Op,
		Version:  args.Version,
	}
	index, _, isLeader := kv.rf.Start(command)

	if !isLeader {
		return
	}

	reply.Err = ErrFailed
	requestCond := sync.NewCond(&kv.mu)
	kv.mu.Lock()

	success := kv.keyVersionSuccess[key][command.Version]
	if success {
		reply.Err = OK
		kv.mu.Unlock()
	} else {
		kv.pendingRequests[args.Version] = append(kv.pendingRequests[args.Version], requestCond)
		kv.pendingRaftCmds[index] = append(kv.pendingRaftCmds[index], requestCond)
		requestCond.Wait()

		success = kv.keyVersionSuccess[key][Version]
		if success {
			reply.Err = OK
		}
		requestCond.L.Unlock()
	}

}

func (kv *KVServer) updateState() {

	var key, value, opType string
	var version, commandIndex int

	for m := range kv.applyCh {
		if m.CommandValid {
			command, ok := m.Command.(Op)
			if !ok {
				log.Fatal("applyCh command is not type Op")
			}
			version = command.Version
			key = command.Key
			value = command.Value
			opType = command.OpType
			commandIndex = command.CommandIndex
			kv.mu.Lock()
			success := kv.keyVersionSuccess[key][version]
			if !success {
				kv.keyVersionSuccess[key][version] = true
				if opType == Append {
					kv.state[key] += value
				} else if opType == Put {
					kv.state[key] = value
				}
				for _, cond := range kv.pendingRequests[version] {
					cond.Broadcast()
				}
				for _, cond := range kv.pendingRaftCmds[commandIndex] {
					cond.Broadcast()
				}
			} else {
				log.Fatal("command of the same Version commited twice")
			}
			kv.mu.Unlock()
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.updateState()

	return kv
}
