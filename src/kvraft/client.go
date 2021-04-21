package kvraft

import (
	"../labrpc"
	"fmt"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	lastLeaderId int
	globalTasksId int
	servers []*labrpc.ClientEnd
	myId     int64
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.globalTasksId = 0
	ck.lastLeaderId = 0
	ck.myId = nrand()
	fmt.Println("++++++++ create a new clerk +++++++++++",ck.myId)
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) SendRequestRPC(key string, value string,op string) string{
	serverPeer:=ck.lastLeaderId
	args := &RequestArgs{
		TasksId: ck.globalTasksId,
		Key:     key,
		Value:   value,
		Type:    op,
		ClientId : ck.myId,
	}
	reply := &RequestReply{}
	for {
		serverPeer = serverPeer % len(ck.servers)
		ok := ck.servers[serverPeer].Call("KVServer.PullRequest", args, reply)
		if ok && reply.Err == OK {
			ck.lastLeaderId = serverPeer
			fmt.Println("clerk ID ", ck.myId, " Server ID : " ,serverPeer,"task ID",ck.globalTasksId, " right Leader")
			return reply.Value
		}
		if  ok && reply.Err == ErrWrongLeader{
			fmt.Println("clerk ID ", ck.myId, " Server ID : " ,serverPeer, "task ID",ck.globalTasksId, " ErrWrongLeader")
		}
		if  ok && reply.Err == TimeOut{
			fmt.Println("timeout","clerk ID ", args.ClientId,"task ID",args.TasksId," Leader Id :",serverPeer )
			//time.Sleep(time.Millisecond * 1000)
		}
		if !ok{
			fmt.Println("rpc error------","clerk ID ", args.ClientId,"task ID",args.TasksId,"rpc server id ",serverPeer )
		}
		serverPeer++
	}
}

func (ck *Clerk) Put(key string, value string) {

	ck.globalTasksId++
	fmt.Println("now command , clerk ID : ", ck.myId , " task ID :" ,ck.globalTasksId ," cmd kind : put______________","wantputkey ",key,"wantputvalue",value)

	ck.SendRequestRPC(key, value, "Put")
	fmt.Println("yes  now command , clerk ID : ", ck.myId , " task ID :" ,ck.globalTasksId ," cmd kind : put________________","key ",key,"putvalue",value)
	//ck.showMessage()

}
func (ck *Clerk) Append(key string, value string) {
	ck.globalTasksId++
	fmt.Println("now command , clerk ID : ", ck.myId , " task ID :" ,ck.globalTasksId ," cmd kind : append_______________","wantappendkey ",key,"wantappendvalue",value)
	ck.SendRequestRPC(key, value, "Append")
	fmt.Println("yes  now command , clerk ID : ", ck.myId , " task ID :" ,ck.globalTasksId ," cmd kind : append________________","key ",key,"appendvalue",value)
	//ck.showMessage()


}
func (ck *Clerk) Get(key string) string {
	ck.globalTasksId++
	fmt.Println("now command , clerk ID : ", ck.myId , " task ID :" ,ck.globalTasksId ," cmd kind : get________________","wantgetkey ",key)

	value := ck.SendRequestRPC(key,"",GetValue)

	fmt.Println("yes  now command , clerk ID : ", ck.myId , " task ID :" ,ck.globalTasksId ," cmd kind : get________________","key",key,"getvalue",value)
	//ck.showMessage()
	return value
	// You will have to modify this function.
}
func (ck *Clerk) showMessage(){
	args := &GetArgs{

	}
	reply := &GetReply{}
	for i:= 0 ;i < len(ck.servers) ; i++{
		ok := ck.servers[i].Call("KVServer.Isleader",args,reply)
		if ok{
			fmt.Println("server:" ,i, "is leader ?????????????????????????????",reply.Err)
		}else{
			fmt.Println("server:" ,i, "is leader ?????????????????????????????  timeout ")

		}
	}
}
