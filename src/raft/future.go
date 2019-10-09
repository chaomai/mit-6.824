package raft

import (
	"context"
	"sync"

	"go.uber.org/zap"
)

type Future interface {
	Error() error
}

// concurrent safe
type ErrorFuture struct {
	mutex     sync.Mutex
	err       error
	errCh     chan error
	responded bool
}

func NewErrorFuture() *ErrorFuture {
	e := &ErrorFuture{
		errCh:     make(chan error, 1),
		responded: false,
	}
	return e
}

func (e *ErrorFuture) RespondErr(err error) {
	e.mutex.Lock()
	defer e.mutex.Unlock()
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

func (e *ErrorFuture) Error() error {
	e.mutex.Lock()
	if e.err != nil {
		e.mutex.Unlock()
		return e.err
	}
	e.mutex.Unlock()

	if e.errCh == nil {
		zap.L().Panic("future waits for a nil chan")
	}

	// wait
	err := <-e.errCh

	e.mutex.Lock()
	defer e.mutex.Unlock()
	e.err = err

	return e.err
}

// concurrent safe
type RPCFuture struct {
	ErrorFuture
	mutex     sync.Mutex
	request   interface{}
	reply     interface{}
	responded bool
}

func NewRPCFuture(args interface{}) *RPCFuture {
	errFuture := NewErrorFuture()
	r := &RPCFuture{
		ErrorFuture: *errFuture,
		request:     args,
		responded:   false,
	}
	return r
}

func (rpc *RPCFuture) Respond(r interface{}, err error) {
	rpc.mutex.Lock()
	defer rpc.mutex.Unlock()
	if rpc.responded {
		return
	}

	rpc.reply = r
	rpc.RespondErr(err)
	rpc.responded = true
}

func (rpc *RPCFuture) Get() (reply interface{}, err error) {
	// ensure to see the newest rpc.reply
	rpc.mutex.Lock()
	if rpc.reply != nil {
		reply = rpc.reply
		err = rpc.err
		rpc.mutex.Unlock()
		return
	}
	rpc.mutex.Unlock()

	// wait
	err = rpc.Error()

	rpc.mutex.Lock()
	reply = rpc.reply
	rpc.mutex.Unlock()
	return
}

// concurrent safe
type CommitFuture struct {
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

func (cf *CommitFuture) Respond(err error) {
	cf.RespondErr(err)
}

func (cf *CommitFuture) Get() (index Index, term Term, err error) {
	// wait
	err = cf.Error()
	index = cf.index
	term = cf.term
	return
}

func (cf *CommitFuture) GetWithCtx(ctx context.Context) (index Index, term Term, err error) {
	ch := make(chan struct{})
	// wait until context is canceled
	select {
	case <-ctx.Done():
		index = cf.index
		term = cf.term
		return
	case <-ch:
		// wait
		err = cf.Error()
		index = cf.index
		term = cf.term
		return
	}
}
