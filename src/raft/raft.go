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
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

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
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type AppendEntriesArgs struct{
	LeaderTerm int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries  []Entries
	LeaderCommit int

}

type AppendEntriesReply struct{
	MyCommitIndex int
	Term int
	Success bool

}
type Entries struct {
	Term int
	Command interface{}
}
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	currentTerm      int
	log      []Entries
	nextIndex []int
	matchIndex []int
	commitIndex int
	lastApplied int
	voteFor		int
	Cond		*sync.Cond
	hasReceiveHeartBeat bool
	State State  // 接口本身就是引用类型，所以不用加*
	Alive		[]bool
	peerNum		int
	tasksQueue []Entries
	taskQueueMu sync.Mutex

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	term = rf.currentTerm
	isleader = rf.State.IsLeader()
	// Your code here (2A).
	return term, isleader
}

func (rf *Raft) isLogLatestThanMe(args *RequestVoteArgs) bool{
	LastLogIndex := rf.returnLastLogIndex()
	if args.LastLogTerm != (rf.log[LastLogIndex].Term){
		return rf.log[LastLogIndex].Term <= args.LastLogTerm
	}else{
		return args.LastLogIndex >= LastLogIndex
	}
}
//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {

	// Your data here (2A, 2B).
	CandidateTerm int
	CandidateId	  int
	LastLogIndex  int
	LastLogTerm   int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term 		 int
	VoteGranted	 bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) returnLastLogTerm() int{
	index := rf.returnLastLogIndex()
	return rf.log[index].Term

}

func (rf *Raft) returnLastLogIndex() int{
	return len(rf.log)-1

}
func (rf *Raft) isTermNotLatestThanMe(args *RequestVoteArgs) bool{
	return args.CandidateTerm < rf.currentTerm
}
func(rf* Raft) isVoteForOtherPeer(args *RequestVoteArgs) bool{
	return args.CandidateTerm == rf.currentTerm && args.CandidateId != rf.voteFor
}
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.PrintInfo("ID: ",rf.me," wants to vote to ",args.CandidateId)
	rf.mu.Lock()
	defer 	rf.mu.Unlock()
	if rf.isTermNotLatestThanMe(args) ||rf.isVoteForOtherPeer(args) || !rf.isLogLatestThanMe(args) {  // "=" 表示我自己是candidate,我投给了我自己    或者我已经投票给了和你平级的竞争者
		reply.VoteGranted = false
		if rf.currentTerm < args.CandidateTerm{
			rf.State = NewFollower(rf,HEARTBEATMIN,HEARTBEATMAX).SetVoteFor(-1).SetCurrentTerm(args.CandidateTerm)
		}
		reply.Term = rf.currentTerm
	} else{
		rf.State = NewFollower(rf,HEARTBEATMIN,HEARTBEATMAX).ChangeIsReceiveHeartBeat(true).SetCurrentTerm(args.CandidateTerm).SetVoteFor(args.CandidateId)
		reply.VoteGranted = true
		reply.Term = args.CandidateTerm
		rf.PrintInfo("ID :",rf.me," become a follower.","term of ID:",rf.me,":",rf.currentTerm)
	}
	rf.PrintInfo("ID: ",rf.me,"  voting  to ", args.CandidateId, " finished.")


}

func(rf *Raft) AdmitLeader(args *AppendEntriesArgs) bool{ // todo

	if args.LeaderTerm < rf.currentTerm{
		return false
	}
	return true
}

func(rf *Raft) IsConsistency(PrevLogIndex int,PrevLogTerm int)bool{
	lastIndex := rf.returnLastLogIndex()
	if lastIndex < PrevLogIndex{
		return false
	}else{
		myTerm := rf.log[PrevLogIndex].Term
		if myTerm != PrevLogTerm{
			return false
		}else{
			return true
		}
	}
}
func(rf *Raft) UpdateLog(PrevLogIndex int,log []Entries){
	rf.log = rf.log[0:(PrevLogIndex+1)]
	rf.log = append(rf.log, log...)
}
func(rf *Raft) CommitEntries(LeaderCommit int){
	now := LeaderCommit
	if now > rf.returnLastLogIndex(){
		now = rf.returnLastLogIndex()
	}
	rf.commitIndex = now
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).

	rf.PrintInfo("ID: ",rf.me," wants to response appendentries to ",args.LeaderId)
	rf.mu.Lock()

	if rf.AdmitLeader(args){
		rf.State = NewFollower(rf,HEARTBEATMIN,HEARTBEATMAX).ChangeIsReceiveHeartBeat(true).SetVoteFor(args.LeaderId).SetCurrentTerm(args.LeaderTerm)
		rf.PrintInfo("ID :",rf.me," become a follower.","term of ID:",rf.me,":",rf.currentTerm)
		reply.Term = rf.currentTerm
		if rf.IsConsistency(args.PrevLogIndex,args.PrevLogTerm){
			rf.UpdateLog(args.PrevLogIndex,args.Entries)
			rf.CommitEntries(args.LeaderCommit)
			reply.Success = true
			reply.MyCommitIndex = rf.returnLastLogIndex()
		}else{
			reply.Success = false
		}

	}else{
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.PrintInfo("ID :",rf.me ,"(term :",rf.currentTerm,") do not admit this leader ( term :",args.LeaderTerm," ID :",args.LeaderId,")")

	}

	rf.mu.Unlock()
	rf.PrintInfo("ID: ",rf.me,"  response appendentries", args.LeaderId, " finished.")


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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func(rf *Raft) taskNumsNotDone()int{
	return len(rf.tasksQueue)
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

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !rf.State.IsLeader() || !rf.IsAlive(){
		return -1,-1,false
	}else{
		rf.PrintInfo("******************")
		rf.PrintInfo("want to append command ", command," to ID :",rf.me)
		rf.PrintInfo("******************")
		entry := Entries{
			Term: rf.currentTerm,
			Command: command,
		}
		rf.tasksQueue = append(rf.tasksQueue,entry)
		commandIndex := rf.returnLastLogIndex()+rf.taskNumsNotDone()
		return commandIndex,rf.currentTerm,true
	}


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



func TryBeingLeader(rf *Raft,peers []*labrpc.ClientEnd, me int){


}

func (rf *Raft) IsAlive()bool{
	return !rf.killed()
}



func Run( rf *Raft){
	for rf.IsAlive(){
		rf.PrintInfo("My type: ",rf.State.MyType(),"do this job circularly. ID: ", rf.me," term:",rf.currentTerm,"Log: ",rf.log," queue ",rf.tasksQueue,"commitIndex: ",rf.commitIndex," applyindex: ",rf.lastApplied)
		rf.mu.Lock()
		rf.State.Job()
		rf.mu.Unlock()

	}
}



func (rf *Raft) IsReceiveHeartBeat() bool{
	return rf.hasReceiveHeartBeat

}

func InitAServer(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft{
	rf := &Raft{
		peers: peers,
		persister: persister,
		me: me,
		dead: 0,
		hasReceiveHeartBeat: false,
		Alive : make([]bool,len(peers)),
		peerNum: len(peers),
		currentTerm:0,
		voteFor: -1,
		commitIndex:0,
		lastApplied: 0,
		Cond:sync.NewCond(&sync.Mutex{}),
		log : append(make([]Entries,0), Entries{0, nil}),
		tasksQueue: make([]Entries,0),

	}

	rf.State = NewFollower(rf,HEARTBEATMIN,HEARTBEATMAX).SetVoteFor(-1)
	rf.PrintInfo("ID :",rf.me," become a follower.","term of ID:",rf.me,":",rf.currentTerm)

	return rf
}
//this go routine runs continuously in background to execute command
func TryApply(applyCh chan ApplyMsg,rf *Raft){
	for {
		if !rf.IsAlive(){
			break
		}
		time.Sleep(10*time.Millisecond)
		rf.mu.Lock()

		if rf.lastApplied < rf.commitIndex{
			rf.lastApplied++
			ApplyMsg := ApplyMsg{
				Command:      rf.log[rf.lastApplied].Command,
				CommandValid: true,
				CommandIndex: rf.lastApplied,
			}
			applyCh<-ApplyMsg
			rf.PrintInfo("ID: ",rf.me, " Is it a leader ",rf.State.IsLeader()," applych $$$$$$$$$$  ",ApplyMsg)
		}
		rf.mu.Unlock()

	}
}
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := InitAServer(peers,me,persister,applyCh)
	go Run(rf)
	go TryApply(applyCh,rf)
	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}
