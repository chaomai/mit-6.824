package raft

import (
	"context"
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
func notify(ch chan Notification, traceId ...uint32) {
	var id uint32
	if len(traceId) == 0 {
		id = rand.Uint32()
	} else if len(traceId) == 1 {
		id = traceId[0]
	} else {
		zap.L().Panic("notify accept at most one trace id")
	}

	select {
	case ch <- Notification{TraceId: id}:
	default:
	}
}

// cancel send elem to channel if cancel function of ctx is called.
func trySend(ctx context.Context, ch interface{}, elem interface{}) {
	switch t := ch.(type) {
	case chan appendResult:
		select {
		case <-ctx.Done():
			return
		case t <- elem.(appendResult):
		}
	case chan voteResult:
		select {
		case <-ctx.Done():
			return
		case t <- elem.(voteResult):
		}
	default:
		zap.L().Panic("unknown chan type", zap.Any("type", t))
	}
}

func min(a Index, b Index) Index {
	if a <= b {
		return a
	}
	return b
}
