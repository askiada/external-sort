package batchingchannels

import (
	"context"

	"github.com/askiada/external-sort/vector"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// BatchingChannel implements the Channel interface, with the change that instead of producing individual elements
// on Out(), it batches together the entire internal buffer each time. Trying to construct an unbuffered batching channel
// will panic, that configuration is not supported (and provides no benefit over an unbuffered NativeChannel).
type BatchingChannel struct {
	input     chan string
	output    chan vector.Vector
	buffer    vector.Vector
	allocate  func(i int) vector.Vector
	g         *errgroup.Group
	sem       *semaphore.Weighted
	size      int
	maxWorker int64
	dCtx      context.Context
}

func NewBatchingChannel(ctx context.Context, maxWorker int64, size int, allocate func(i int) vector.Vector) *BatchingChannel {
	if size == 0 {
		panic("channels: BatchingChannel does not support unbuffered behaviour")
	}
	if size < 0 {
		panic("channels: invalid negative size in NewBatchingChannel")
	}
	g, dCtx := errgroup.WithContext(ctx)
	ch := &BatchingChannel{
		input:     make(chan string),
		output:    make(chan vector.Vector),
		size:      size,
		allocate:  allocate,
		maxWorker: maxWorker,
		g:         g,
		sem:       semaphore.NewWeighted(maxWorker),
		dCtx:      dCtx,
	}
	go ch.batchingBuffer()
	return ch
}

func (ch *BatchingChannel) In() chan<- string {
	return ch.input
}

// Out returns a <-chan interface{} in order that BatchingChannel conforms to the standard Channel interface provided
// by this package, however each output value is guaranteed to be of type []interface{} - a slice collecting the most
// recent batch of values sent on the In channel. The slice is guaranteed to not be empty or nil. In practice the net
// result is that you need an additional type assertion to access the underlying values.
func (ch *BatchingChannel) Out() <-chan vector.Vector {
	return ch.output
}

func (ch *BatchingChannel) ProcessOut(f func(vector.Vector) error) error {
	for val := range ch.Out() {
		if err := ch.sem.Acquire(ch.dCtx, 1); err != nil {
			return err
		}
		val := val
		ch.g.Go(func() error {
			defer ch.sem.Release(1)
			return f(val)
		})
	}
	err := ch.g.Wait()
	if err != nil {
		return err
	}
	return nil
}

func (ch *BatchingChannel) Len() int {
	return ch.size
}

func (ch *BatchingChannel) Cap() int {
	return ch.size
}

func (ch *BatchingChannel) Close() {
	close(ch.input)
}

func (ch *BatchingChannel) batchingBuffer() {
	ch.buffer = ch.allocate(ch.size)
	for {
		elem, open := <-ch.input
		if open {
			ch.buffer.PushBack(elem)
		} else {
			if ch.buffer.End() > 0 {
				ch.output <- ch.buffer
			}
			break
		}
		if ch.buffer.End() == ch.size {
			ch.output <- ch.buffer
			ch.buffer = ch.allocate(ch.size)
		}
	}

	close(ch.output)
}
