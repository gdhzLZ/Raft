package raft

type State interface {
	IsLeader() bool
	IsCandidate() bool
	MyType() string
	Job()
}





