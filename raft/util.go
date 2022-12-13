package raft

import (
	"fmt"
	"log"
	"time"
)

// Debugging
const debug = -1

var debugStart time.Time

func init() {
	debugStart = time.Now()
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func DPrintf(format string, a ...interface{}) {
	if debug > 0 {
		time := time.Since(debugStart).Microseconds()
		time /= 1000
		prefix := fmt.Sprintf("%02d:%03d ", time/1000, time%1000)
		format = prefix + format
		log.Printf(format, a...)
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a < b {
		return b
	}
	return a
}
