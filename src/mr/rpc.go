package mr


import "os"
import "strconv"



type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type MapFinishedReply struct{
	finished bool
}
type AllFinishedReply struct{
	finished bool
}
type StatusReply struct{
	MapFinished bool
	AllFinished bool
	mapFile []string
}
type Tasks struct{
	ReduceTask []string
	MapTask string
	AllFinished bool
	REDUCENUMBER int
	MapTaskNumber int
	ReduceTaskNumber string
}
type FinishArgs struct{
	Type   int // 0 map 1 reduce
	FinishedFile string
}
type FinishReply struct{
	MapFinished int
	Message string
}
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
