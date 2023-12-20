package raft

import "log"

// Debugging
const Debug = false

func init() {

	//log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
	log.SetFlags(log.LstdFlags)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
