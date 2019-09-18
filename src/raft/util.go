package raft

import (
	"log"
	"math/rand"
	"time"

	"go.uber.org/zap"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// [min, 2*min)
func getRandomDuration(min time.Duration) time.Duration {
	if min == 0 {
		zap.L().Panic("min is 0",
			zap.Duration("min", min))
	}

	delta := time.Duration(rand.Int63()) % min
	return min + delta
}

// non-blocking notify
func notify(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func min(a Index, b Index) Index {
	if a <= b {
		return a
	}
	return b
}
