package raft

import "time"

/*
type RequestVote struct{
	CallCount int
	RepliesList []*RequestVoteReply
}

 */

type Candidate struct{
	raft *Raft
	PosVotes int
	NegVotes int
	termNewerThanMe bool
	NewestTerm	int
}


func(m *Candidate) MyType() string{
	return "Candidate"
}
func(m *Candidate) IsLeader() bool{
	return false
}
func (m *Candidate) IsCandidate() bool{
	return true
}

func(m *Candidate)MustLost()bool{
	if m.NegVotes > m.raft.peerNum/2{
		return true
	} else{
		return false
	}
}

func(m *Candidate) MustWin()bool{

	if m.PosVotes > m.raft.peerNum/2{
		return true
	} else{
		return false
	}
}



func(m* Candidate) ComputingVotes(RequestVotesReply *RequestVoteReply, ok bool){
	//if RequestVotesReply.Term != m.raft.currentTerm{
	//	return
	//}
	if ok == true && RequestVotesReply.VoteGranted == true{
		m.PosVotes++
	}else {
		m.NegVotes++
	}
}
func(m* Candidate) ComputingTermNewerThanMe(RequestVotesReply *RequestVoteReply){
	if RequestVotesReply.Term > m.raft.currentTerm{
		m.termNewerThanMe = true
		if  m.NewestTerm < RequestVotesReply.Term{
			m.NewestTerm = RequestVotesReply.Term
		}
	}
}
func (m *Candidate) WaitToSendRequestVote() {

	SendDur := 100
	time.Sleep(time.Millisecond*time.Duration(SendDur))

}
func(m *Candidate) TermNewerThanMe()bool{
	return m.termNewerThanMe
}
func (m *Candidate) CallBack() {

	m.raft.Cond.L.Lock()
	for !m.MustLost() && !m.MustWin()  &&!m.TermNewerThanMe() {
		m.raft.Cond.Wait()
	}
	m.raft.Cond.L.Unlock()
	m.raft.mu.Lock()
	if m.raft.State.IsCandidate(){
		if m.TermNewerThanMe() || m.MustLost(){
			m.raft.PrintInfo("Find a newer term")
			m.raft.State = NewFollower(m.raft,HEARTBEATMIN,HEARTBEATMAX).SetCurrentTerm(m.NewestTerm).SetVoteFor(-1)
			m.raft.PrintInfo("ID :",m.raft.me," become a follower.","term of ID:",m.raft.me,":",m.raft.currentTerm)
			m.raft.persist()

		}else if m.MustWin(){
			m.raft.PrintInfo("Win")
			m.raft.State = NewLeader(m.raft)
		}else{

			m.raft.PrintInfo("Lost,ID: ",m.raft.me," try a new selection.")
			m.raft.State = NewCandidate(m.raft)
		}
	}else{
		m.raft.PrintInfo("ID: ",m.raft.me," changed from candidate to follower.")
	}


}


func NewCandidate(raft *Raft) *Candidate{
	raft.currentTerm++
	Candidate:= &Candidate{
		raft: raft,
		PosVotes:1,
		NegVotes: 0,
		termNewerThanMe:false,
		NewestTerm : raft.currentTerm,
	}
	Candidate.raft.tasksQueue = make([]Entries,0)

	raft.PrintInfo("ID :",raft.me," become a candidate.","term of ID:",raft.me,":",raft.currentTerm)
	return Candidate
}
func (m *Candidate) Job(){

	currentTerm := m.raft.currentTerm
	CandidateID := m.raft.me
	m.raft.mu.Unlock()
	for peer := 0 ; peer < len(m.raft.peers) ; peer++{
		if peer != m.raft.me {
			go func(peer int){
				m.raft.PrintInfo("ID :",m.raft.me," send requestVote to ID :",peer)
				RequestVotesArgs := RequestVoteArgs{
					CandidateTerm :currentTerm,
					CandidateId	  :CandidateID,
					LastLogTerm	  :m.raft.returnLastLogTerm(),
					LastLogIndex  :m.raft.returnLastLogIndex(),
				}
				RequestVotesReply := RequestVoteReply{}
				ok := m.raft.sendRequestVote(peer, &RequestVotesArgs, &RequestVotesReply)
				m.raft.mu.Lock()
				defer m.raft.mu.Unlock()
				if ok{
					m.raft.PrintInfo("ID: ",m.raft.me," received the voting result from ",peer,". It is ",RequestVotesReply.VoteGranted)

				}else{
					m.raft.PrintInfo("ID: ",m.raft.me,"  has not received the voting result from ",peer," my term :",m.raft.currentTerm)
				}
				m.raft.Cond.L.Lock()
				m.ComputingVotes(&RequestVotesReply,ok)

				if ok{
					//m.RequestVotes.RepliesList[peer] = &RequestVotesReply
					m.ComputingTermNewerThanMe(&RequestVotesReply)
				}
				m.raft.Cond.Broadcast()
				m.raft.Cond.L.Unlock()
			}(peer)
		}
	}

	m.CallBack()

}