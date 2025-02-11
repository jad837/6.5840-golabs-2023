package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity level: %s", v)
		}
	}
	return level
}

type logTopic string
type logLevel string

var debugVerbosity int
var debugStart time.Time

// Debugging
const Debug = false
const Info = true

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func DLogF(topic logTopic, logLevel logLevel, peerId int, format string, a ...interface{}) (n int, err error) {
	if logLevel == dTrace && debugVerbosity < 4 {
		return
	}
	if logLevel == dDebug && debugVerbosity < 3 {
		return
	}
	if logLevel == dWarn && debugVerbosity < 2 {
		return
	}
	if logLevel == dInfo && debugVerbosity < 1 {
		return
	}

	time := time.Since(debugStart).Microseconds()
	time /= 100
	prefix := fmt.Sprintf("%06d [S%d] [%s] [%s] ", time, peerId, logLevel, string(topic))
	format = prefix + format
	log.Printf(format, a...)
	return
}
