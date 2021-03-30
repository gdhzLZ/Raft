package raft

import "math/rand"
type Timer struct{
	WaitHeartBeatMin int
	WaitHeartBeatMax int

}
func RandInt(min int, max int) int{
	if min >= max || min == 0 || max == 0 {
		return max
	}
	return rand.Intn(max-min) + min
}
