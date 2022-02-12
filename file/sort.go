package file

import (
	"bufio"
	"fmt"
	"os"
	"runtime"

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

func (f *Info) MergeSort(chunkPaths []string, k int) (err error) {
	output := [][]byte{}
	if f.PrintMemUsage && f.mu == nil {
		f.mu = &MemUsage{}
	}
	// create a chunk per file path
	chunks := &chunks{list: make([]*chunkInfo, 0, len(chunkPaths))}
	if err != nil {
		return err
	}
	for _, chunkPath := range chunkPaths {
		err := chunks.new(f.InputPath, chunkPath, f.Allocate.EmptyKey, k)
		if err != nil {
			return err
		}
	}

	outputFile, err := os.Create(f.OutputPath)
	if err != nil {
		return err
	}
	// remember to close the file
	defer outputFile.Close()

	outputBuffer := bufio.NewWriter(outputFile)

	bar := pb.StartNew(f.totalRows)
	chunks.resetOrder()
	for {
		if f.PrintMemUsage {
			f.mu.Collect()
		}
		if chunks.len() == 0 || len(output) == k {
			err = WriteBuffer(outputBuffer, output)
			if err != nil {
				return err
			}
		}
		if chunks.len() == 0 {
			break
		}
		toShrink := []int{}
		// search the smallest value across chunk buffers by comparing first elements only
		minChunk, minValue, minIdx, err := chunks.min()
		if err != nil {
			return err
		}
		output = append(output, minValue)
		// remove the first element from the chunk we pulled the smallest value
		minChunk.buffer = minChunk.buffer[1:]
		isEmpty := false
		if len(minChunk.buffer) == 0 {
			err = minChunk.pullSubset(k)
			if err != nil {
				return err
			}
			// if after pulling data the chunk buffer is still empty then we can remove it
			if len(minChunk.buffer) == 0 {
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
	err = outputBuffer.Flush()
	if err != nil {
		return err
	}
	bar.Finish()
	if f.PrintMemUsage {
		f.mu.PrintMemUsage()
	}
	return chunks.close()
}

func WriteBuffer(buffer *bufio.Writer, rows [][]byte) error {
	for _, row := range rows {
		_, err := buffer.Write(row)
		if err != nil {
			return err
		}
	}
	rows = nil
	return nil
}
