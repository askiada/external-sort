package file

import (
	"fmt"
	"runtime"

	"github.com/askiada/external-sort/vector"
	"github.com/askiada/external-sort/writer"
	"github.com/cheggaaa/pb/v3"
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

func (f *Info) MergeSort(chunkPaths []string, k int, dropDuplicates bool) (err error) {
	var oldElem *vector.Element
	output := f.Allocate.Vector(k, f.Allocate.Key)
	if f.PrintMemUsage && f.mu == nil {
		f.mu = &MemUsage{}
	}
	if f.WithHeader {
		err = output.PushFrontNoKey(f.headers)
		if err != nil {
			return err
		}
	}
	// create a chunk per file path
	chunks := &chunks{list: make([]*chunkInfo, 0, len(chunkPaths))}
	for _, chunkPath := range chunkPaths {
		err := chunks.new(chunkPath, f.Allocate, k, f.WithHeader)
		if err != nil {
			return err
		}
	}
	f.outputWriter, err = f.Allocate.FnWriter(f.OutputFile)
	if err != nil {
		return err
	}
	defer f.outputWriter.Close()
	bar := pb.StartNew(f.totalRows)
	chunks.resetOrder()
	for {
		if f.PrintMemUsage {
			f.mu.Collect()
		}
		if chunks.len() == 0 || output.Len() == k {
			err = WriteBuffer(f.outputWriter, output)
			if err != nil {
				return err
			}
		}
		if chunks.len() == 0 {
			break
		}
		toShrink := []int{}
		// search the smallest value across chunk buffers by comparing first elements only
		minChunk, minValue, minIdx := chunks.min()
		if (!dropDuplicates || oldElem == nil) || (dropDuplicates && !minValue.Key.Equal(oldElem.Key)) {
			err = output.PushBack(minValue.Row)
			if err != nil {
				return err
			}
			oldElem = minValue
		}

		// remove the first element from the chunk we pulled the smallest value
		minChunk.buffer.FrontShift()
		isEmpty := false
		if minChunk.buffer.Len() == 0 {
			err = minChunk.pullSubset(k)
			if err != nil {
				return err
			}
			// if after pulling data the chunk buffer is still empty then we can remove it
			if minChunk.buffer.Len() == 0 {
				isEmpty = true
				toShrink = append(toShrink, minIdx)
				err = chunks.shrink(toShrink)
				if err != nil {
					return err
				}
			}
		}
		// when we get a new element in the first chunk we need to re-order it
		if !isEmpty {
			chunks.moveFirstChunkToCorrectIndex()
		}
		bar.Increment()
	}
	bar.Finish()
	if f.PrintMemUsage {
		f.mu.PrintMemUsage()
	}
	return chunks.close()
}

func WriteBuffer(w writer.Writer, rows vector.Vector) error {
	for i := 0; i < rows.Len(); i++ {
		err := w.Write(rows.Get(i).Row)
		if err != nil {
			return err
		}
	}
	rows.Reset()
	return nil
}
