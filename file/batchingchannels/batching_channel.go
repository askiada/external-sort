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
	allocate  *vector.Allocate
	g         *errgroup.Group
	sem       *semaphore.Weighted
	dCtx      context.Context
	size      int
	maxWorker int64
}

func NewBatchingChannel(ctx context.Context, allocate *vector.Allocate, maxWorker int64, size int) *BatchingChannel {
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

// Out returns a <-chan vector.Vector in order that BatchingChannel conforms to the standard Channel interface provided
// by this package, however each output value is guaranteed to be of type vector.Vector - a vector collecting the most
// recent batch of values sent on the In channel. The vector is guaranteed to not be empty or nil.
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
	ch.buffer = ch.allocate.Vector(ch.size, ch.allocate.Key)
	for {
		text, open := <-ch.input
		if open {
			err := ch.buffer.PushBack(text)
			if err != nil {
				ch.g.Go(func() error {
					return err
				})
			}
		} else {
			if ch.buffer.Len() > 0 {
				ch.output <- ch.buffer
			}
			break
		}
		if ch.buffer.Len() == ch.size {
			curr := ch.buffer.Get(ch.buffer.Len()-1).Offset + int64(ch.buffer.Get(ch.buffer.Len()-1).Len)
			ch.output <- ch.buffer
			ch.buffer = ch.allocate.Vector(ch.size, ch.allocate.Key)
			ch.buffer.SetCurrOffet(curr)
		}
	}

	close(ch.output)
}
