package batchingchannels

import (
	"context"

	"github.com/askiada/external-sort/vector"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// BatchingChannel implements the Channel interface, with the change that instead of producing individual elements
// on Out(), it batches together the entire internal buffer each time. Trying to construct an unbuffered batching channel
// will panic, that configuration is not supported (and provides no benefit over an unbuffered NativeChannel).
type BatchingChannel struct {
	input           chan interface{}
	output          chan vector.Vector
	buffer          vector.Vector
	allocate        *vector.Allocate
	G               *errgroup.Group
	internalContext context.Context //nolint //containedcontext
	size            int
	maxWorker       int
}

func NewBatchingChannel(ctx context.Context, allocate *vector.Allocate, maxWorker, size int) *BatchingChannel {
	if size == 0 {
		panic("channels: BatchingChannel does not support unbuffered behaviour")
	}
	if size < 0 {
		panic("channels: invalid negative size in NewBatchingChannel")
	}
	errGrp, errGrpContext := errgroup.WithContext(ctx)
	errGrp.SetLimit(maxWorker)
	bChan := &BatchingChannel{
		input:           make(chan interface{}),
		output:          make(chan vector.Vector),
		size:            size,
		allocate:        allocate,
		maxWorker:       maxWorker,
		G:               errGrp,
		internalContext: errGrpContext,
	}
	go bChan.batchingBuffer()
	return bChan
}

func (ch *BatchingChannel) In() chan<- interface{} {
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
		val := val
		ch.G.Go(func() error {
			return f(val)
		})
	}
	err := ch.G.Wait()
	if err != nil {
		return errors.Wrap(err, "one of the task failed")
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
		row, open := <-ch.input
		if open {
			err := ch.buffer.PushBack(row)
			if err != nil {
				ch.G.Go(func() error {
					return errors.Wrap(err, "can't push back row")
				})
			}
		} else {
			if ch.buffer.Len() > 0 {
				ch.output <- ch.buffer
			}

			break
		}
		if ch.buffer.Len() == ch.size {
			ch.output <- ch.buffer
			ch.buffer = ch.allocate.Vector(ch.size, ch.allocate.Key)
		}
	}

	close(ch.output)
}
