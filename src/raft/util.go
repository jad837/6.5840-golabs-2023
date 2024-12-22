package raft

import (
	"fmt"
	"log"
	"os"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
		format = fmt.Sprintf("[PID %d] %s", os.Getpid(), format)
		log.Printf(format, a...)
	}
	return
}
