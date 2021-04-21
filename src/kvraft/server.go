package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	Key string
	Value string
	Type string
	TaskId   int
	ClientId int64
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}
type CommandOp struct{
	Data Op
	Chan chan Op

}
type ServerToClient struct {
	err Err
	value string
}
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	kvMap     map[string]string
	maxraftstate int // snapshot if log grows this big
	clientIdToChan	map[int64] chan ServerToClient
	maxOpId map[int64]int
	// Your definitions here.
}
func(kv *KVServer) ClientIdToChan(clientIdandTaskId int64)chan ServerToClient{
	_,hasKey := kv.clientIdToChan[clientIdandTaskId]
	if !hasKey{
		kv.clientIdToChan[clientIdandTaskId] = make(chan ServerToClient)
	}
	return kv.clientIdToChan[clientIdandTaskId]
}


func (kv *KVServer) PullRequest(args *RequestArgs, reply *RequestReply) {
	kv.mu.Lock()
	op := Op{
		Type: args.Type,
		Key: args.Key,
		Value: args.Value,
		TaskId:args.TasksId,
		ClientId: args.ClientId,
	}
	commandOp := CommandOp{
		Data: op,
	}
	maxOpId, hasClientRecord := kv.maxOpId[op.ClientId]
	var ok bool
	if hasClientRecord && maxOpId >= op.TaskId{
		ok = kv.rf.State.IsLeader()
	}else{
		_,_,ok = kv.rf.Start(commandOp)

	}
	kv.mu.Unlock()
	go kv.ApplyToServer()
	if !ok{
		reply.Err = ErrWrongLeader
	}else{
		kv.mu.Lock()
		MyCh := kv.ClientIdToChan(op.ClientId+int64(op.TaskId) )
		kv.mu.Unlock()
		select {
		case p := <-MyCh:
			reply.Err = p.err
			reply.Value = p.value
		case <-time.After(1200 * time.Millisecond):
			reply.Err = TimeOut
			return
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


func (kv *KVServer) ApplyToServer(){

	select {
		case data := <-kv.applyCh:
			p, ok := data.Command.(CommandOp)
			if ok{
				kv.mu.Lock()
				Chan := kv.ClientIdToChan(p.Data.ClientId + int64(p.Data.TaskId) )// responding client chan
				reply := ServerToClient{}
				reply.err = OK
				maxOpId, hasClientRecord := kv.maxOpId[p.Data.ClientId]
				if hasClientRecord && maxOpId >= p.Data.TaskId{

					fmt.Println("fffff!!!!!!!!!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++",p.Data.ClientId,maxOpId,p.Data.TaskId)
					if p.Data.Type == GetValue{
						reply.value =  kv.kvMap[p.Data.Key]
					}else{
						reply.value = ""
					}
					kv.mu.Unlock()
					Chan <- reply
					//return

				}else{
					kv.maxOpId[p.Data.ClientId] = p.Data.TaskId
					if p.Data.Type == PutValue{
						kv.kvMap[p.Data.Key] = p.Data.Value
					}else if p.Data.Type ==AppendValue {
						last := kv.kvMap[p.Data.Key]
						last += p.Data.Value
						kv.kvMap[p.Data.Key] = last
					}else{
						value := kv.kvMap[p.Data.Key]
						reply.value = value
					}
					kv.mu.Unlock()
					Chan <- reply
				}
				fmt.Println("great !!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

			}else{
				fmt.Println("type error !!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
				syscall.Exit(2)
			}
			fmt.Println("log len", len(kv.rf.GetLog()),"commit index ",kv.rf.GetCommitIndex(),"apply index ",kv.rf.GetApplyIndex())

		//case <-time.After(3000 * time.Millisecond):
		//	fmt.Println("server to raft timeout !!!!!!!!!!!!!!!!!!!!!!!!!000!!!!")
		//	return
	}
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
	labgob.Register(CommandOp{})
	labgob.Register(ServerToClient{})
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.kvMap = make(map[string]string)
	kv.clientIdToChan = make(map[int64]chan ServerToClient)
	kv.maxOpId = make(map[int64]int)
	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	return kv
}
