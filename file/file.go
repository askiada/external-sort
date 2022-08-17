package file

import (
	"context"
	"io"
	"sync"

	"path"
	"strconv"

	"github.com/askiada/external-sort/file/batchingchannels"
	"github.com/askiada/external-sort/vector"
	"github.com/askiada/external-sort/writer"

	"github.com/pkg/errors"
)

type Info struct {
	mu            *MemUsage
	Allocate      *vector.Allocate
	InputReader   io.Reader
	OutputFile    string
	outputWriter  writer.Writer
	totalRows     int
	PrintMemUsage bool
}

// CreateSortedChunks Scan a file and divide it into small sorted chunks.
// Store all the chunks in a folder an returns all the paths.
func (f *Info) CreateSortedChunks(ctx context.Context, chunkFolder string, dumpSize int, maxWorkers int64) ([]string, error) {
	fn := "scan and sort and dump"
	if dumpSize <= 0 {
		return nil, errors.Wrap(errors.New("dump size must be greater than 0"), fn)
	}

	if f.PrintMemUsage && f.mu == nil {
		f.mu = &MemUsage{}
	}

	err := clearChunkFolder(chunkFolder)
	if err != nil {
		return nil, errors.Wrap(err, fn)
	}

	inputReader := f.Allocate.FnReader(f.InputReader)

	row := 0
	chunkPaths := []string{}

	mu := sync.Mutex{}

	batchChan := batchingchannels.NewBatchingChannel(ctx, f.Allocate, maxWorkers, dumpSize)
	batchChan.G.Go(func() error {
		for inputReader.Next() {
			if f.PrintMemUsage {
				f.mu.Collect()
			}
			elem, err := inputReader.Read()
			if err != nil {
				return errors.Wrap(err, fn)
			}
			batchChan.In() <- elem
			row++
		}
		batchChan.Close()
		if inputReader.Err() != nil {
			return errors.Wrap(inputReader.Err(), fn)
		}
		return nil
	})

	chunkIdx := 0
	err = batchChan.ProcessOut(func(v vector.Vector) error {
		mu.Lock()
		chunkIdx++
		chunkPath := path.Join(chunkFolder, "chunk_"+strconv.Itoa(chunkIdx)+".tsv")
		mu.Unlock()
		v.Sort()
		err := f.Allocate.Dump(v, chunkPath)
		if err != nil {
			return err
		}
		mu.Lock()
		chunkPaths = append(chunkPaths, chunkPath)
		mu.Unlock()
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, fn)
	}
	f.totalRows = row
	return chunkPaths, nil
}
