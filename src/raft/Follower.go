package raft

import (
	"time"
)
const(
	HEARTBEATMAX=400
	HEARTBEATMIN=250
)

type Follower struct { // extends State
	raft *Raft
	WaitHeartBeatMax int
	WaitHeartBeatMin int
}

func(m *Follower) IsLeader() bool{
	return false
}


func(m *Follower) IsCandidate() bool{
	return false
}


func(m *Follower) MyType() string{
	return "Follower"
}

func (m *Follower) WaitHeartBeat() {

	TimeRaft := Timer{}
	TimeRaft.WaitHeartBeatMax = m.WaitHeartBeatMax
	TimeRaft.WaitHeartBeatMin = m.WaitHeartBeatMin
	CheckDur := 5
	HeartBeatDur := RandInt(TimeRaft.WaitHeartBeatMin,TimeRaft.WaitHeartBeatMax)
	m.raft.PrintInfo("ID: ",m.raft.me," follower is waiting HeartBeatDur: ",HeartBeatDur," term :",m.raft.currentTerm)
	CheckCount := HeartBeatDur/CheckDur
	for i := 0; i < CheckCount ; i++{
		time.Sleep(time.Millisecond*time.Duration(CheckDur))
		if m.raft.IsReceiveHeartBeat() == true{
			return
		}
	}

}

func (m *Follower)  SetWaitHeartBeatTime(min int,max int){
	m.WaitHeartBeatMax = max
	m.WaitHeartBeatMin = min
}


func (m *Follower) ChangeIsReceiveHeartBeat(hasReceiveHeartBeat bool) *Follower{
	m.raft.hasReceiveHeartBeat = hasReceiveHeartBeat
	return m
}
func (m *Follower) SetCurrentTerm(term int) *Follower{
	m.raft.currentTerm = term
	return m
}
func (m *Follower) SetVoteFor(voteId int) *Follower{
	m.raft.voteFor = voteId
	return m
}
func NewFollower(raft *Raft,WaitHeartBeatMin int, WaitHeartBeatMax int) *Follower{

	Follower:= &Follower{
		raft: raft,
		WaitHeartBeatMax:WaitHeartBeatMax,
		WaitHeartBeatMin:WaitHeartBeatMin,
	}
	Follower.raft.tasksQueue = make([]Entries,0)

	return Follower
}



func (m *Follower) Job(){
	//m.raft.Wait()
	m.raft.mu.Unlock()
	//we choose mutex and share data to synchronization
	m.WaitHeartBeat()
	m.raft.mu.Lock()  //这样可以防止在判断完wait后 follower接到了通知，
	if 	!m.raft.IsReceiveHeartBeat(){  // it could be a candidate,做这个判断是怕在wait到这里的这个时间段 follower收到了heartbeat
		m = m.SetVoteFor( m.raft.me)
		m.raft.State = NewCandidate(m.raft)
		m.raft.persist()
	} else{

		m.raft.PrintInfo("ID: " ,m.raft.me ," receive a heartBeat.")
		m.ChangeIsReceiveHeartBeat(false)
	}

}
