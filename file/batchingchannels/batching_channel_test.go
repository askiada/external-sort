package batchingchannels_test

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/askiada/external-sort/file/batchingchannels"
	"github.com/askiada/external-sort/vector"
	"github.com/askiada/external-sort/vector/key"
	"github.com/stretchr/testify/assert"
)

type Int struct {
	value int
}

func AllocateInt(line string) (key.Key, error) {
	num, err := strconv.Atoi(line)
	if err != nil {
		return nil, err
	}
	return &Int{num}, nil
}

func (k *Int) Get() int {
	return k.value
}

func (k *Int) Less(other key.Key) bool {
	return k.value < other.(*Int).value
}
func testBatches(t *testing.T, ch *batchingchannels.BatchingChannel) {
	maxI := 10000
	expectedSum := (maxI - 1) * maxI / 2
	wg := &sync.WaitGroup{}
	wgInput := &sync.WaitGroup{}

	maxIn := 100
	wgInput.Add(maxIn)
	for j := 0; j < maxIn; j++ {
		go func(j int) {
			defer wgInput.Done()
			for i := maxI / maxIn * j; i < maxI*(j+1)/maxIn; i++ {
				ch.In() <- strconv.Itoa(i)
			}
		}(j)
	}

	go func() {
		wgInput.Wait()
		ch.Close()
	}()

	got := make(chan *vector.Element, maxI)
	wgSum := &sync.WaitGroup{}
	wgSum.Add(1)
	gotSum := 0
	go func() {
		defer wgSum.Done()
		for g := range got {
			gotSum += g.Key.(*Int).Get()
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := ch.ProcessOut(func(val vector.Vector) error {
			for i := 0; i < val.Len(); i++ {
				val := val.Get(i)
				got <- val
			}
			time.Sleep(3 * time.Millisecond)
			return nil
		})
		if err != nil {
			panic(err)
		}
	}()
	wg.Wait()
	close(got)
	wgSum.Wait()
	assert.Equal(t, expectedSum, gotSum)
}

func TestBatchingChannel(t *testing.T) {
	allocate := vector.DefaultVector(AllocateInt)
	ch := batchingchannels.NewBatchingChannel(context.Background(), allocate, 2, 50)
	testBatches(t, ch)

	ch = batchingchannels.NewBatchingChannel(context.Background(), allocate, 2, 3)
	testBatches(t, ch)

	ch = batchingchannels.NewBatchingChannel(context.Background(), allocate, 2, 1)
	testChannelConcurrentAccessors(t, "batching channel", ch)
}

func TestBatchingChannelCap(t *testing.T) {
	allocate := vector.DefaultVector(AllocateInt)
	ch := batchingchannels.NewBatchingChannel(context.Background(), allocate, 2, 5)
	if ch.Cap() != 5 {
		t.Error("incorrect capacity on infinite channel")
	}
}

func testChannelConcurrentAccessors(t *testing.T, name string, ch *batchingchannels.BatchingChannel) {
	// no asserts here, this is just for the race detector's benefit
	go ch.Len()
	go ch.Cap()

	go func() {
		ch.In() <- ""
	}()

	go func() {
		<-ch.Out()
	}()
}
