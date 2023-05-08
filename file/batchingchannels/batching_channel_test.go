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
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type intKey struct {
	value int
}

func allocateInt(row interface{}) (key.Key, error) {
	line, ok := row.(string)
	if !ok {
		return nil, errors.Errorf("can't convert interface{} to string: %+v", row)
	}
	num, err := strconv.Atoi(line)
	if err != nil {
		return nil, err
	}

	return &intKey{num}, nil
}

func (k *intKey) Get() int {
	return k.value
}

func (k *intKey) Less(other key.Key) bool {
	return k.value < other.(*intKey).value
}

func (k *intKey) Equal(other key.Key) bool {
	return k.value == other.(*intKey).value
}

func testBatches(t *testing.T, bChan *batchingchannels.BatchingChannel) {
	t.Helper()
	maxI := 10000
	expectedSum := (maxI - 1) * maxI / 2
	wgrp := &sync.WaitGroup{}
	wgrpInput := &sync.WaitGroup{}

	maxIn := 100
	wgrpInput.Add(maxIn)
	for idx := 0; idx < maxIn; idx++ {
		go func(j int) {
			defer wgrpInput.Done()
			for i := maxI / maxIn * j; i < maxI*(j+1)/maxIn; i++ {
				bChan.In() <- strconv.Itoa(i)
			}
		}(idx)
	}

	go func() {
		wgrpInput.Wait()
		bChan.Close()
	}()

	got := make(chan *vector.Element, maxI)
	wgSum := &sync.WaitGroup{}
	wgSum.Add(1)
	gotSum := 0
	go func() {
		defer wgSum.Done()
		for g := range got {
			gotSum += g.Key.(*intKey).Get()
		}
	}()
	wgrp.Add(1)
	go func() {
		defer wgrp.Done()
		err := bChan.ProcessOut(func(val vector.Vector) error {
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
	wgrp.Wait()
	close(got)
	wgSum.Wait()
	assert.Equal(t, expectedSum, gotSum)
}

func TestBatchingChannel(t *testing.T) {
	allocate := vector.DefaultVector(allocateInt, nil, nil)
	bChan, err := batchingchannels.NewBatchingChannel(context.Background(), allocate, 2, 50)
	require.NoError(t, err)
	testBatches(t, bChan)

	bChan, err = batchingchannels.NewBatchingChannel(context.Background(), allocate, 2, 3)
	require.NoError(t, err)
	testBatches(t, bChan)

	bChan, err = batchingchannels.NewBatchingChannel(context.Background(), allocate, 2, 1)
	require.NoError(t, err)
	testChannelConcurrentAccessors(t, bChan)
}

func TestBatchingChannelCap(t *testing.T) {
	allocate := vector.DefaultVector(allocateInt, nil, nil)
	bChan, err := batchingchannels.NewBatchingChannel(context.Background(), allocate, 2, 5)
	require.NoError(t, err)
	if bChan.Cap() != 5 {
		t.Error("incorrect capacity on infinite channel")
	}
}

func testChannelConcurrentAccessors(_ *testing.T, bChan *batchingchannels.BatchingChannel) {
	// no asserts here, this is just for the race detector's benefit
	go bChan.Len()
	go bChan.Cap()

	go func() {
		bChan.In() <- ""
	}()

	go func() {
		<-bChan.Out()
	}()
}
