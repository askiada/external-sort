package file

import (
	"fmt"
	"runtime"
	"strings"

	"github.com/askiada/external-sort/vector"
	"github.com/askiada/external-sort/writer"
	"github.com/cheggaaa/pb/v3"
	"github.com/pkg/errors"
)

type memUsage struct {
	MaxAlloc uint64
	MaxSys   uint64
	NumGc    uint32
}

func (mu *memUsage) Collect() {
	var mStats runtime.MemStats
	runtime.ReadMemStats(&mStats)
	if mStats.Alloc > mu.MaxAlloc {
		mu.MaxAlloc = mStats.Alloc
	}
	if mStats.Sys > mu.MaxSys {
		mu.MaxSys = mStats.Sys
	}

	mu.NumGc = mStats.NumGC
}

func (mu *memUsage) String() string {
	builder := strings.Builder{}
	builder.WriteString(fmt.Sprintf("Max Alloc = %v MiB", bToMb(mu.MaxAlloc)))
	builder.WriteString(fmt.Sprintf(" Max Sys = %v MiB", bToMb(mu.MaxSys)))
	builder.WriteString(fmt.Sprintf(" NumGC = %v\n", mu.NumGc))
	return builder.String()
}

const conversionMb = (1 << 20) //nolint

func bToMb(b uint64) uint64 {
	return b / conversionMb
}

func (f *Info) createChunks(chunkPaths []string, k int) (*chunks, error) {
	chunks := &chunks{list: make([]*chunkInfo, 0, len(chunkPaths))}
	for _, chunkPath := range chunkPaths {
		err := chunks.new(chunkPath, f.Allocate, k, f.WithHeader)
		if err != nil {
			return nil, errors.Wrapf(err, "can't create chunk %s", chunkPath)
		}
	}
	return chunks, nil
}

func (f *Info) MergeSort(chunkPaths []string, k int, dropDuplicates bool) (err error) {
	var oldElem *vector.Element
	output := f.Allocate.Vector(k, f.Allocate.Key)
	if f.PrintMemUsage && f.mu == nil {
		f.mu = &memUsage{}
	}
	if f.WithHeader {
		err = output.PushFrontNoKey(f.headers)
		if err != nil {
			return errors.Wrapf(err, "can't add headers %+v", f.headers)
		}
	}
	// create a chunk per file path
	createdChunks, err := f.createChunks(chunkPaths, k)
	if err != nil {
		return errors.Wrap(err, "can't create all chunks")
	}
	f.outputWriter, err = f.Allocate.FnWriter(f.OutputFile)
	if err != nil {
		return errors.Wrap(err, "can't get output writer file")
	}
	defer f.outputWriter.Close()
	bar := pb.StartNew(f.totalRows)
	createdChunks.resetOrder()
	for {
		if f.PrintMemUsage {
			f.mu.Collect()
		}
		if createdChunks.len() == 0 || output.Len() == k {
			err = WriteBuffer(f.outputWriter, output)
			if err != nil {
				return err
			}
		}
		if createdChunks.len() == 0 {
			break
		}
		toShrink := []int{}
		// search the smallest value across chunk buffers by comparing first elements only
		minChunk, minValue, minIdx := createdChunks.min()
		if (!dropDuplicates || oldElem == nil) || (dropDuplicates && !minValue.Key.Equal(oldElem.Key)) {
			err = output.PushBack(minValue.Row)
			if err != nil {
				return errors.Wrapf(err, "can't push back row %+v", minValue.Row)
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
				err = createdChunks.shrink(toShrink)
				if err != nil {
					return err
				}
			}
		}
		// when we get a new element in the first chunk we need to re-order it
		if !isEmpty {
			createdChunks.moveFirstChunkToCorrectIndex()
		}
		bar.Increment()
	}
	bar.Finish()
	if f.PrintMemUsage {
		logger.Debugln(f.mu.String())
	}
	return createdChunks.close()
}

func WriteBuffer(w writer.Writer, rows vector.Vector) error {
	for i := 0; i < rows.Len(); i++ {
		err := w.Write(rows.Get(i).Row)
		if err != nil {
			return errors.Wrap(err, "can't write buffer")
		}
	}
	rows.Reset()
	return nil
}
