package raft

import (
	"sync"
	"time"
)

type Leader struct{
	raft  *Raft
	termNewerThanMe bool
	SafeReplicationCount int
	CallCount int
	nextIndex []int
	matchIndex[]int
	muIndex []sync.Mutex

}




func (m *Leader)IsLeader() bool{
	return true
}


func(m *Leader) IsCandidate() bool{
	return false
}


func(m *Leader) MyType() string{
	return "Leader"
}

func (m *Leader) WaitToSendHeartBeat() {

	SendDur := 180
	time.Sleep(time.Millisecond*time.Duration(SendDur))

}
func(m* Leader) HasNewerTermServer() bool{
	return m.termNewerThanMe

}

func(m* Leader) ComputingTermNewerThanMe(AppendEntriesReply *AppendEntriesReply){
	if AppendEntriesReply.Term > m.raft.currentTerm{
		m.raft.currentTerm = AppendEntriesReply.Term
		m.termNewerThanMe = true

	}
}

func(m *Leader)ComputingCallCount(){
	m.CallCount++
}

func(m *Leader)GenerateAppendEntriesArg(currentTerm int, CandidateId int,peer int) AppendEntriesArgs{

	AppendEntriesArgs := AppendEntriesArgs{
		LeaderTerm: currentTerm,
		LeaderId:   CandidateId,
		Entries:  make([]Entries,0),
	}
	AppendEntriesArgs.PrevLogIndex = m.nextIndex[peer]-1
	if AppendEntriesArgs.PrevLogIndex < 0{ //todo
		AppendEntriesArgs.PrevLogIndex = 0
	}
	AppendEntriesArgs.PrevLogTerm = m.raft.log[AppendEntriesArgs.PrevLogIndex].Term
	AppendEntriesArgs.Entries = m.raft.log[m.nextIndex[peer]:]
	AppendEntriesArgs.LeaderCommit = m.raft.commitIndex
	return AppendEntriesArgs
}
func(m* Leader)GenerateAppendEntriesReply() AppendEntriesReply{
	return  AppendEntriesReply{}
}
func(m *Leader) ReceiveAllReplies() bool{
	return !(m.CallCount < (m.raft.peerNum-1))
}

func(m* Leader) Wait(){
	m.raft.PrintInfo("leader ID :",m.raft.me," term :",m.raft.currentTerm," is waiting...")
	m.raft.Cond.L.Lock()
	for  !m.ReceiveAllReplies() && !m.HasNewerTermServer() {
		m.raft.Cond.Wait()
		m.raft.PrintInfo("continue wait... ID :",m.raft.me," term :",m.raft.currentTerm)
	}
	m.raft.Cond.L.Unlock()
	m.raft.mu.Lock()
	if m.raft.State.IsLeader(){
		if m.HasNewerTermServer(){
			m.raft.State = NewFollower(m.raft,HEARTBEATMIN,HEARTBEATMAX).SetVoteFor(-1)
			m.raft.PrintInfo("Leader ID :",m.raft.me,"  changed to a follower. ")
		}
	}
}
func(m*Leader)ChangeIsSafeReplication(AppendEntriesArgs AppendEntriesArgs,AppendEntriesReply AppendEntriesReply){

	if AppendEntriesReply.MyCommitIndex == m.raft.returnLastLogIndex(){
		m.SafeReplicationCount++
	}
}
func(m* Leader)UpdateCommitIndex() {
	if m.SafeReplicationCount > len(m.raft.peers)/2 {
		m.raft.PrintInfo("ID: ",m.raft.me,"has been commited !!!",m.MyType())
		m.raft.commitIndex = m.raft.returnLastLogIndex()
		m.SafeReplicationCount = 1
	} else {
		m.raft.PrintInfo("ID: ",m.raft.me," do not be commited !!!",m.MyType())
		m.SafeReplicationCount = 1

	}
}
func(m* Leader)UpdateLog() {
	if m.raft.commitIndex == m.raft.returnLastLogIndex(){
		if len(m.raft.tasksQueue)!=0{
			entry := m.raft.tasksQueue[0]
			m.raft.log = append(m.raft.log, entry)
			if len(m.raft.tasksQueue) == 1{
				m.raft.tasksQueue =make([]Entries,0)
			}else{
				m.raft.tasksQueue =m.raft.tasksQueue[1:]
			}
		}
	}
}
func (m *Leader) Job(){
	if !m.IsLeader(){
		return
	}


	m.raft.PrintInfo("SafeReplicationCount ",m.SafeReplicationCount," m.raft.commitIndex ",m.raft.commitIndex)
	m.UpdateCommitIndex()
	m.raft.PrintInfo("SafeReplicationCount ",m.SafeReplicationCount," m.raft.commitIndex ",m.raft.commitIndex)
	m.UpdateLog()
	m.raft.persist()
	currentTerm:=m.raft.currentTerm
	CandidateId := m.raft.me
	m.raft.mu.Unlock()
	for peer := 0; peer < len(m.raft.peers); peer++ {
		if peer != m.raft.me {
			go func(peer int) {
				m.raft.PrintInfo("ID :",m.raft.me," request appendEntry to ID :",peer)
				m.raft.mu.Lock()
				AppendEntriesArgs  := m.GenerateAppendEntriesArg(currentTerm,CandidateId ,peer)
				m.raft.mu.Unlock()
				AppendEntriesReply := m.GenerateAppendEntriesReply()
				ok := m.raft.sendAppendEntries(peer, &AppendEntriesArgs, &AppendEntriesReply)
				if ok{
					m.raft.mu.Lock()
					defer m.raft.mu.Unlock()
					if !m.IsLeader(){
						return
					}
					m.ComputingTermNewerThanMe(&AppendEntriesReply)
					if m.HasNewerTermServer(){
						m.raft.State = NewFollower(m.raft,HEARTBEATMIN,HEARTBEATMAX).SetVoteFor(-1).SetCurrentTerm(AppendEntriesReply.Term)
						m.raft.persist()
						m.raft.PrintInfo("Leader ID :",m.raft.me,"  changed to a follower. ")
						return
					}else{
						if AppendEntriesReply.Success == false{
							m.nextIndex[peer] /= 2
						}else{
							m.ChangeIsSafeReplication(AppendEntriesArgs,AppendEntriesReply)
							m.nextIndex[peer] = AppendEntriesReply.MyCommitIndex+1
						}
					}
				}
			}(peer)
		}
	}

	m.WaitToSendHeartBeat()
	m.raft.mu.Lock()

}


func NewLeader(raft *Raft) *Leader{
	Leader:= &Leader{
		raft: raft,
		CallCount:0,
		termNewerThanMe:false,
		SafeReplicationCount:0,

		nextIndex : make([]int,len(raft.peers)),
		matchIndex: make([]int,len(raft.peers)),
		muIndex  :  make([]sync.Mutex,len(raft.peers)),

	}
	Leader.raft.tasksQueue = make([]Entries,0)
	for i:=0;i<len(raft.peers);i++{
		Leader.nextIndex[i] = raft.returnLastLogIndex()+1
		Leader.matchIndex[i] = 0
	}

	raft.StartForEmpty("EmptyOp")
	raft.PrintInfo("ID :",raft.me," become a leader.","term of ID:",raft.me,":",raft.currentTerm)
	return Leader
}
