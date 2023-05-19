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

func (f *Info) handleHeader(output vector.Vector) error {
	if f.WithHeader {
		err := output.PushFrontNoKey(f.headers)
		if err != nil {
			return errors.Wrapf(err, "can't add headers %+v", f.headers)
		}
	}
	return nil
}

type nextChunk struct {
	oldElem *vector.Element
}

func (nc *nextChunk) get(output vector.Vector, createdChunks *chunks, dropDuplicates bool) (*chunkInfo, int, error) {
	minChunk, minValue, minIdx := createdChunks.min()
	if (!dropDuplicates || nc.oldElem == nil) || (dropDuplicates && !minValue.Key.Equal(nc.oldElem.Key)) {
		err := output.PushBack(minValue.Row)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "can't push back row %+v", minValue.Row)
		}
		nc.oldElem = minValue
	}
	return minChunk, minIdx, nil
}

func updateChunks(createdChunks *chunks, minChunk *chunkInfo, minIdx, k int) error {
	minChunk.buffer.FrontShift()
	isEmpty := false
	if minChunk.buffer.Len() == 0 {
		err := minChunk.pullSubset(k)
		if err != nil {
			return errors.Wrapf(err, "can't pull subset from chunk %s", minChunk.filename)
		}
		// if after pulling data the chunk buffer is still empty then we can remove it
		if minChunk.buffer.Len() == 0 {
			isEmpty = true
			err = createdChunks.shrink([]int{minIdx})
			if err != nil {
				return errors.Wrapf(err, "can't shrink chunk at index %d", minIdx)
			}
		}
	}
	// when we get a new element in the first chunk we need to re-order it
	if !isEmpty {
		createdChunks.moveFirstChunkToCorrectIndex()
	}
	return nil
}

func (f *Info) prepareMergeSort(output vector.Vector, chunkPaths []string, outputBufferSize int) (*chunks, error) {
	err := f.handleHeader(output)
	if err != nil {
		return nil, errors.Wrap(err, "can't handle headers")
	}
	// create a chunk per file path
	createdChunks, err := f.createChunks(chunkPaths, outputBufferSize)
	if err != nil {
		return nil, errors.Wrap(err, "can't create all chunks")
	}
	f.outputWriter, err = f.Allocate.FnWriter(f.OutputFile)
	if err != nil {
		return nil, errors.Wrap(err, "can't get output writer file")
	}
	return createdChunks, nil
}

func (f *Info) runMergeSort(createdChunks *chunks, output vector.Vector, outputBufferSize int, dropDuplicates bool) error {
	bar := pb.StartNew(f.totalRows)
	createdChunks.resetOrder()
	smallestChunk := &nextChunk{}
	for {
		if f.PrintMemUsage {
			f.mu.Collect()
		}
		err := f.dumpOutput(createdChunks, output, outputBufferSize)
		if err != nil {
			return errors.Wrap(err, "can't dump output")
		}
		if createdChunks.len() == 0 {
			break
		}

		// search the smallest value across chunk buffers by comparing first elements only
		minChunk, minIdx, err := smallestChunk.get(output, createdChunks, dropDuplicates)
		if err != nil {
			return errors.Wrap(err, "can't get next chunk with smallest value")
		}
		// remove the first element from the chunk we pulled the smallest value
		err = updateChunks(createdChunks, minChunk, minIdx, outputBufferSize)
		if err != nil {
			return errors.Wrap(err, "can't update chunks")
		}
		bar.Increment()
	}
	bar.Finish()
	if f.PrintMemUsage {
		logger.Debugln(f.mu.String())
	}
	return nil
}

func (f *Info) dumpOutput(createdChunks *chunks, output vector.Vector, outputBufferSize int) error {
	if createdChunks.len() == 0 || output.Len() == outputBufferSize {
		err := writeBuffer(f.outputWriter, output)
		if err != nil {
			return err
		}
	}
	return nil
}

// MergeSort merge and sort a list of files.
// It is possilbe to drop duplicates and define the maximum size of the output buffer before flush.
func (f *Info) MergeSort(chunkPaths []string, outputBufferSize int, dropDuplicates bool) (err error) {
	output := f.Allocate.Vector(outputBufferSize, f.Allocate.Key)
	if f.PrintMemUsage && f.mu == nil {
		f.mu = &memUsage{}
	}
	createdChunks, err := f.prepareMergeSort(output, chunkPaths, outputBufferSize)
	if err != nil {
		return errors.Wrap(err, "can't prepare merge sort")
	}
	defer func() { err = f.outputWriter.Close() }()
	err = f.runMergeSort(createdChunks, output, outputBufferSize, dropDuplicates)
	if err != nil {
		return errors.Wrap(err, "can't run merge sort")
	}
	err = createdChunks.close()
	if err != nil {
		return errors.Wrap(err, "can't close created chunks")
	}
	return err
}

func writeBuffer(w writer.Writer, rows vector.Vector) error {
	for i := 0; i < rows.Len(); i++ {
		err := w.Write(rows.Get(i).Row)
		if err != nil {
			return errors.Wrap(err, "can't write buffer")
		}
	}
	rows.Reset()
	return nil
}
