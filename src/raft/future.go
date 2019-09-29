package raft

import (
	"sync"

	"go.uber.org/zap"
)

type Future interface {
	Error() error
}

type ErrorFuture struct {
	err       error
	errCh     chan error
	responded bool
}

func NewErrorFuture() *ErrorFuture {
	e := &ErrorFuture{
		errCh: make(chan error, 1),
	}
	return e
}

// only allowed to call in single goroutine, not concurrent safe
func (e *ErrorFuture) RespondErr(err error) {
	if e.errCh == nil {
		zap.L().Panic("send on a nil chan")
	}

	if e.responded {
		return
	}

	e.errCh <- err
	close(e.errCh)
	e.responded = true
}

// only allowed to call in single goroutine, not concurrent safe
func (e *ErrorFuture) Error() error {
	if e.err != nil {
		return e.err
	}

	if e.errCh == nil {
		zap.L().Panic("future waits for a nil chan")
	}

	e.err = <-e.errCh
	return e.err
}

type RPCFuture struct {
	sync.Mutex
	ErrorFuture
	request interface{}
	reply   interface{}
}

func NewRPCFuture(args interface{}) *RPCFuture {
	errFuture := NewErrorFuture()
	r := &RPCFuture{
		ErrorFuture: *errFuture,
		request:     args,
	}
	return r
}

// only allowed to call in single goroutine, not concurrent safe
func (rpc *RPCFuture) Respond(r interface{}, err error) {
	if rpc.responded {
		return
	}

	rpc.Lock()
	rpc.reply = r
	rpc.Unlock()

	rpc.RespondErr(err)
}

// only allowed to call in single goroutine, not concurrent safe
func (rpc *RPCFuture) Get() (reply interface{}, err error) {
	// ensure to see the newest rpc.reply
	rpc.Lock()
	if rpc.reply != nil {
		reply = rpc.reply
		err = rpc.err
		rpc.Unlock()
		return
	}
	rpc.Unlock()

	// wait
	_ = rpc.Error()

	rpc.Lock()
	reply = rpc.reply
	err = rpc.err
	rpc.Unlock()
	return
}

type CommitFuture struct {
	sync.Mutex
	ErrorFuture
	index Index
	term  Term
}

func NewCommitFuture(index Index, term Term) *CommitFuture {
	errFuture := NewErrorFuture()
	r := &CommitFuture{
		ErrorFuture: *errFuture,
		index:       index,
		term:        term,
	}
	return r
}

// only allowed to call in single goroutine, not concurrent safe
func (cf *CommitFuture) Respond(err error) {
	cf.RespondErr(err)
}

// only allowed to call in single goroutine, not concurrent safe
func (cf *CommitFuture) Get() (index Index, term Term, err error) {
	// wait
	_ = cf.Error()
	index = cf.index
	term = cf.term
	err = cf.err
	return
}
