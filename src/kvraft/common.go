package kvraft

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
	PutValue 		= "Put"
	GetValue		= "Get"
	AppendValue 	= "Append"
	TransFromErr    = "TransFromErr"
	TypeErr			= "TypeErr"
	TimeOut			= "TimeOut"
)

type Err string

type PutAppendArgs struct {
	Key   string
	Value string
	Type   string // "Put" or "Append"
	TasksId int
	ClientId int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}
type RequestArgs struct {
	Key   string
	Value string
	Type   string // "Put" or "Append"
	TasksId int
	ClientId int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}
type RequestReply struct {
	Err   Err
	Value string
}
type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Type   string
	Key string
	TasksId int
	ClientId int64

	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
