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

type RPCFuture struct {
	sync.Mutex
	ErrorFuture
	request      interface{}
	reply        interface{}
	rpcResponded bool
}

func NewRPCFuture(args interface{}) *RPCFuture {
	errFuture := NewErrorFuture()
	r := &RPCFuture{
		ErrorFuture: *errFuture,
		request:     args,
	}

	return r
}

func (rpc *RPCFuture) Respond(r interface{}, err error) {
	if rpc.rpcResponded {
		return
	}

	rpc.Lock()
	rpc.reply = r
	rpc.Unlock()

	rpc.RespondErr(err)
	rpc.rpcResponded = true
}

func (rpc *RPCFuture) Get() (reply interface{}, err error) {
	rpc.Lock()
	if rpc.reply != nil {
		reply = rpc.reply
		err = rpc.err
		rpc.Unlock()
		return
	}
	rpc.Unlock()

	_ = rpc.Error()
	reply = rpc.reply
	err = rpc.err
	return
}
