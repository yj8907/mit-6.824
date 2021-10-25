package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//

type State string

const (
	Follower  = "Follower"
	Candidate = "Candidate"
	Leader    = "Leader"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	state         State
	electionTimer time.Time

	applyCh chan ApplyMsg
}

var electionTimeOutBase, electionTimeOutInterval int = 300, 150
var electionTimeOut *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))
var heartBeatInterval string = "100ms"
var applyMsgInterval string = "40ms"
var commitInterval string = "40ms"

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.state == Leader

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logEntry []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logEntry) != nil {
		log.Fatal("error from reading persistor")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logEntry
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int

	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) getLastLogInfo() (int, int) {
	var lastLogIndex, lastLogTerm int

	if len(rf.log) > 0 {
		lastLogIndex = len(rf.log)
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	return lastLogIndex, lastLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	reply.Term = rf.currentTerm
	reply.Success = false

	// if len(args.Entries) > 0 {
	// 	log.Printf("[%v] recieve AppendEntries from %v, args: %v, currTerm %v, currLog:%v\n",
	// 		rf.me, args.LeaderId, args, rf.currentTerm, rf.log)
	// }
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm <= args.Term {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.electionTimer = time.Now()
		// if len(args.Entries) == 0 {
		// 	log.Printf("[%v] recieve heartbeat from %v, state: %v\n", rf.me, args.LeaderId, rf.state)
		// }
		if len(args.Entries) > 0 {
			if len(rf.log) < args.PrevLogIndex ||
				(args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm) {
				return
			}

			startIndex := args.PrevLogIndex
			for ; startIndex < len(rf.log) && startIndex-args.PrevLogIndex < len(args.Entries); startIndex++ {
				if rf.log[startIndex].Term != args.Entries[startIndex-args.PrevLogIndex].Term {
					break
				}
			}
			if startIndex-args.PrevLogIndex < len(args.Entries) {
				rf.log = append(rf.log[:startIndex], args.Entries[(startIndex-args.PrevLogIndex):]...)
			}
		}

		if args.LeaderCommit > rf.commitIndex && len(rf.log) > rf.commitIndex {
			nextCommitIndex := args.LeaderCommit
			if len(rf.log) < nextCommitIndex {
				nextCommitIndex = len(rf.log)
			}
			if rf.log[nextCommitIndex-1].Term == args.PrevLogTerm {
				rf.commitIndex = nextCommitIndex
			}
		}
		reply.Success = true

		rf.persist()
	}

}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	// log.Printf("RequestVote [%v] receive vote request from [%v]. own term: %v, request term: %v",
	// 	rf.me, args.CandidateId, rf.currentTerm, args.Term)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if rf.currentTerm < args.Term || (rf.currentTerm == args.Term && rf.votedFor == args.CandidateId) {
		rf.currentTerm = args.Term
		lastLogIndex, lastLogTerm := rf.getLastLogInfo()
		if lastLogTerm < args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.state = Follower
			rf.electionTimer = time.Now()
		}
		rf.persist()
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	log.Printf("sendRequestVote [%v] receive vote request from [%v]. own term: %v, request term: %v",
		server, args.CandidateId, rf.currentTerm, args.Term)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) callAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) electionHeartBeat() {

	var sleepDuration int64
	rf.electionTimer = time.Now()

	for {
		rf.mu.Lock()
		if time.Since(rf.electionTimer).Milliseconds() >= sleepDuration && rf.state != Leader {
			go rf.attemptElection()
		}
		// if time.Since(rf.electionTimer) >= sleepDuration {
		// 	go rf.attemptElection()
		// }
		rf.electionTimer = time.Now()

		sleepDuration = int64(electionTimeOut.Intn(electionTimeOutInterval) + electionTimeOutBase)
		duration, _ := time.ParseDuration(strconv.Itoa(int(sleepDuration)) + "ms")
		rf.mu.Unlock()

		time.Sleep(duration)
	}

}

func (rf *Raft) attemptElection() {

	var numVotes int = 0
	numServers := len(rf.peers)

	rf.mu.Lock()
	rf.state = Candidate
	rf.votedFor = rf.me
	lastLogIndex, lastLogTerm := rf.getLastLogInfo()
	rf.currentTerm += 1
	currTerm := rf.currentTerm
	rf.persist()
	rf.mu.Unlock()

	for server := 0; server < numServers; server++ {
		if server != rf.me {
			go func(server int) {
				args := RequestVoteArgs{
					Term:         currTerm,
					CandidateId:  rf.me,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm,
				}
				reply := RequestVoteReply{}

				for {
					ok := rf.sendRequestVote(server, &args, &reply)
					if ok || rf.killed() || currTerm < rf.currentTerm {
						break
					}
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.state != Candidate {
					return
				}
				if reply.VoteGranted {
					numVotes++
				}
				if rf.currentTerm < reply.Term {
					rf.currentTerm = reply.Term
					rf.state = Follower
					return
				}
				// log.Printf("[%v] numVotes: %v at term: %v with current term: %v with %v peers\n",
				// 	rf.me, numVotes, currTerm, rf.currentTerm, numServers)
				if numVotes >= numServers/2 {
					log.Printf("[%v] becomes leader\n", rf.me)
					rf.state = Leader
					go rf.HeartBeat()
					go rf.updateCommitIndex()
					rf.initPeerIndex()

					// commit no-op for read only operations
					var no_op interface{}
					rf.Start(no_op)
					return
				}
				rf.persist()
			}(server)
		}
	}

}

func (rf *Raft) sendEmptyAppendEntries() {

	var prevLogTerm int
	numServers := len(rf.peers)
	rf.mu.Lock()
	state := rf.state
	currTerm := rf.currentTerm
	if rf.commitIndex > 0 {
		prevLogTerm = rf.log[rf.commitIndex-1].Term
	}
	rf.mu.Unlock()

	done := false
	if state == Leader {
		for server := 0; server < numServers; server++ {
			if server == rf.me {
				continue
			}
			go func(server int) {
				args := &AppendEntriesArgs{
					Term:         currTerm,
					LeaderId:     rf.me,
					LeaderCommit: rf.commitIndex,
					PrevLogTerm:  prevLogTerm,
				}
				reply := &AppendEntriesReply{}

				for {
					ok := rf.callAppendEntries(server, args, reply)
					if done {
						return
					}
					if ok || rf.killed() || rf.state != Leader {
						break
					}
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if !reply.Success && reply.Term > rf.currentTerm {
					done = true
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.persist()
				}
			}(server)
		}

	}
}

func (rf *Raft) sendAppendEntries() {

	numServers := len(rf.peers)
	rf.mu.Lock()
	state := rf.state
	currTerm := rf.currentTerm
	rf.mu.Unlock()

	done := false
	if state == Leader {
		for server := 0; server < numServers; server++ {
			if server == rf.me {
				continue
			}
			go func(server int) {

				reply := &AppendEntriesReply{}
				var nextIndex, lastLogIndex, prevLogIndex, prevLogTerm, leaderCommit int
				var ok bool = true
				for {
					rf.mu.Lock()
					lastLogIndex = len(rf.log)
					if rf.nextIndex[server] > lastLogIndex {
						rf.mu.Unlock()
						break
					}

					if nextIndex > 0 && ok {
						rf.nextIndex[server]--
					}
					nextIndex = rf.nextIndex[server]
					prevLogIndex = nextIndex - 1
					prevLogTerm = 0
					if prevLogIndex > 0 {
						prevLogTerm = rf.log[prevLogIndex-1].Term
					}
					entries := rf.log[prevLogIndex:lastLogIndex]
					leaderCommit = rf.commitIndex
					rf.mu.Unlock()

					if lastLogIndex < nextIndex {
						break
					}
					args := &AppendEntriesArgs{
						Term:         currTerm,
						LeaderId:     rf.me,
						PrevLogIndex: prevLogIndex,
						PrevLogTerm:  prevLogTerm,
						Entries:      entries,
						LeaderCommit: leaderCommit,
					}

					ok = rf.callAppendEntries(server, args, reply)
					if done || reply.Term > currTerm || reply.Success || rf.killed() {
						break
					}
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()
				if !reply.Success && reply.Term > rf.currentTerm {
					done = true
					rf.currentTerm = reply.Term
					rf.state = Follower
				} else if reply.Success {
					rf.nextIndex[server] = lastLogIndex + 1
					rf.matchIndex[server] = lastLogIndex
				}
				rf.persist()
			}(server)
		}
	}
}

func (rf *Raft) HeartBeat() {

	hbInterval, _ := time.ParseDuration(heartBeatInterval)

	for {
		rf.mu.Lock()
		if rf.state != Leader || rf.killed() {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
		go rf.sendEmptyAppendEntries()

		time.Sleep(hbInterval)
	}
}

func (rf *Raft) GetOpHearBeat() bool {

	var prevLogTerm int
	numServers := len(rf.peers)
	rf.mu.Lock()
	state := rf.state
	currTerm := rf.currentTerm
	if rf.commitIndex > 0 {
		prevLogTerm = rf.log[rf.commitIndex-1].Term
	}
	rf.mu.Unlock()

	voteChan := make(chan int, numServers-1)
	abort := false
	if rf.state == Leader {
		for server := 0; server < numServers; server++ {
			if server == rf.me {
				continue
			}
			go func(server int) {
				args := &AppendEntriesArgs{
					Term:         currTerm,
					LeaderId:     rf.me,
					LeaderCommit: rf.commitIndex,
					PrevLogTerm:  prevLogTerm,
				}
				reply := &AppendEntriesReply{}

				ok := rf.callAppendEntries(server, args, reply)

				if !ok || abort {
					voteChan <- 0
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if !reply.Success && reply.Term > rf.currentTerm {
					abort = true
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.persist()
					voteChan <- 0
					return
				}
				voteChan <- 1
			}(server)
		}

		numVotes := 0
		for vote := range voteChan {
			numVotes += vote
		}

		if !abort && numVotes > (numServers-1)/2 {
			return true
		}
	}

	return false
}


func (rf *Raft) initPeerIndex() {

	numServers := len(rf.peers)
	currentLogIndex := len(rf.log)
	rf.nextIndex = []int{}
	rf.matchIndex = []int{}
	for s := 0; s < numServers; s++ {
		rf.nextIndex = append(rf.nextIndex, currentLogIndex+1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}
}


func (rf *Raft) updateCommitIndex() {

	numServers := len(rf.peers)
	interval, _ := time.ParseDuration(commitInterval)

	for {
		rf.mu.Lock()
		if rf.state != Leader || rf.killed() {
			rf.mu.Unlock()
			break
		}
		prevCommitIndex := rf.commitIndex
		i := rf.commitIndex + 1
		for ; i < len(rf.log)+1; i++ {
			if rf.log[i-1].Term != rf.currentTerm {
				continue
			}
			numCommits := 0
			for s := 0; s < len(rf.peers); s++ {
				if rf.matchIndex[s] >= i {
					numCommits++
				}
			}
			if numCommits < numServers/2 {
				break
			}
			rf.commitIndex = i
		}
		rf.mu.Unlock()

		if rf.commitIndex > prevCommitIndex {
			rf.sendEmptyAppendEntries()
		}

		time.Sleep(interval)
	}
}

func (rf *Raft) sendApplyMsg() {

	var msg ApplyMsg
	var lastCommitIndex int = 0

	interval, _ := time.ParseDuration(applyMsgInterval)
	for {
		if rf.killed() {
			break
		}

		rf.mu.Lock()
		nextCommitIndex := rf.commitIndex
		rf.mu.Unlock()

		for i := lastCommitIndex; i < nextCommitIndex; i++ {
			msg.CommandValid = true
			msg.Command = rf.log[i].Command
			msg.CommandIndex = i + 1
			rf.applyCh <- msg
		}
		lastCommitIndex = nextCommitIndex
		time.Sleep(interval)
	}

}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		isLeader = false
	} else {
		index = len(rf.log) + 1
		term = rf.currentTerm
		logEntry := LogEntry{Term: term, Command: command}
		rf.log = append(rf.log, logEntry)
		rf.persist()
		go rf.sendAppendEntries()
	}

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	go rf.electionHeartBeat()
	go rf.sendApplyMsg()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
