package batchingchannels

import (
	"context"

	"github.com/askiada/external-sort/vector"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// BatchingChannel standard channel, with the change that instead of producing individual elements
// on Out(), it batches together n elements each time. Trying to construct an unbuffered batching channel
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

// NewBatchingChannel create a batching channel.
func NewBatchingChannel(ctx context.Context, allocate *vector.Allocate, maxWorker, size int) (*BatchingChannel, error) {
	if size == 0 {
		return nil, errors.New("does not support unbuffered behaviour")
	}
	if size < 0 {
		return nil, errors.New("does not support negative size")
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

	return bChan, nil
}

// In add element to input channel.
func (ch *BatchingChannel) In() chan<- interface{} {
	return ch.input
}

// Out returns a <-chan vector.Vector in order that BatchingChannel conforms to the standard Channel interface provided
// by this package, however each output value is guaranteed to be of type vector.Vector - a vector collecting the most
// recent batch of values sent on the In channel. The vector is guaranteed to not be empty or nil.
func (ch *BatchingChannel) Out() <-chan vector.Vector {
	return ch.output
}

// ProcessOut process specified function on each batch.
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

// Len return the maximum number of elements in a batch.
func (ch *BatchingChannel) Len() int {
	return ch.size
}

// Cap return the maximum capacity of a batch.
func (ch *BatchingChannel) Cap() int {
	return ch.size
}

// Close close the input channel.
func (ch *BatchingChannel) Close() {
	close(ch.input)
}

// batchingBuffer add input element to the next batch available.
// When the batch reach maximum size or the input channel is closed, it is passed to the output channel.
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
