package file

import (
	"fmt"
	"runtime"

	"github.com/askiada/external-sort/vector"
)

type MemUsage struct {
	MaxAlloc uint64
	MaxSys   uint64
	NumGc    uint32
}

func (mu *MemUsage) Collect() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	if m.Alloc > mu.MaxAlloc {
		mu.MaxAlloc = m.Alloc
	}
	if m.Sys > mu.MaxSys {
		mu.MaxSys = m.Sys
	}

	mu.NumGc = m.NumGC
}

func (mu *MemUsage) PrintMemUsage() {
	fmt.Printf("Max Alloc = %v MiB", bToMb(mu.MaxAlloc))
	fmt.Printf("\tMax Sys = %v MiB", bToMb(mu.MaxSys))
	fmt.Printf("\tNumGC = %v\n", mu.NumGc)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func (f *Info) MergeSort(chunkPaths []string, k int) (output []vector.Element, err error) {
	mu := &MemUsage{}
	// create a chunk per file path
	chunks := &chunks{list: make([]*chunkInfo, 0, len(chunkPaths))}
	for _, chunkPath := range chunkPaths {
		err := chunks.new(chunkPath, f.Allocate, k)
		if err != nil {
			return nil, err
		}
	}
	for chunks.len() > 0 {
		mu.Collect()
		toShrink := []int{}
		// search the smallest value across chunk buffers by comparing first elements only
		minChunk, minValue, minIdx := chunks.min()
		output = append(output, minValue)
		// remove the first element from the chunk we pulled the smallest value
		minChunk.buffer.FrontShift()
		if minChunk.buffer.End() == 0 {
			err = minChunk.pullSubset(k)
			if err != nil {
				return nil, err
			}
			// if after pulling data the chunk buffer is still empty then we can remove it
			if minChunk.buffer.End() == 0 {
				toShrink = append(toShrink, minIdx)
				err = chunks.shrink(toShrink)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	mu.PrintMemUsage()
	return output, chunks.close()
}
