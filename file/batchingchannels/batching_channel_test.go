package batchingchannels_test

import (
	"testing"

	"github.com/askiada/external-sort/file/batchingchannels"
)

/*
type s struct {
	child *s
	i     int
	a     string
	sl    []int
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
				ch.In() <- &s{
					child: &s{
						i:  i - 1,
						a:  "child",
						sl: []int{i - 1},
					},
					i:  i,
					a:  "parent",
					sl: []int{i},
				}
			}
		}(j)
	}

	go func() {
		wgInput.Wait()
		ch.Close()
	}()

	got := make(chan int, maxI)
	wgSum := &sync.WaitGroup{}
	wgSum.Add(1)
	gotSum := 0
	go func() {
		defer wgSum.Done()
		for g := range got {
			gotSum += g
		}
	}()
	maxOut := 100
	wg.Add(maxOut)
	for i := 0; i < maxOut; i++ {
		go func() {
			defer wg.Done()
			err := ch.ProcessOut(func(val []interface{}) error {
				//time.Sleep(100 * time.Millisecond)
				//fmt.Printf("address of slice %p \n", &val)
				for _, e := range val {
					got <- e.(*s).i
				}
				time.Sleep(400 * time.Millisecond)
				return nil
			})
			if err != nil {
				panic(err)
			}
		}()
	}
	wg.Wait()
	close(got)
	wgSum.Wait()
	assert.Equal(t, expectedSum, gotSum)
}

func TestBatchingChannel(t *testing.T) {
	ch := batchingchannels.NewBatchingChannel(50)
	testBatches(t, ch)

	ch = batchingchannels.NewBatchingChannel(3)
	testBatches(t, ch)

	ch = batchingchannels.NewBatchingChannel(1)
	testChannelConcurrentAccessors(t, "batching channel", ch)
}

func TestBatchingChannelCap(t *testing.T) {
	ch := batchingchannels.NewBatchingChannel(5)
	if ch.Cap() != 5 {
		t.Error("incorrect capacity on infinite channel")
	}
}.
*/
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
