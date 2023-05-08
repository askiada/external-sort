package file

import (
	"context"
	"io"
	"path"
	"strconv"
	"sync"

	"github.com/askiada/external-sort/file/batchingchannels"
	"github.com/askiada/external-sort/vector"
	"github.com/askiada/external-sort/writer"
	"github.com/sirupsen/logrus"

	"github.com/pkg/errors"
)

var logger = logrus.StandardLogger()

type Info struct {
	mu            *MemUsage
	Allocate      *vector.Allocate
	InputReader   io.Reader
	OutputFile    io.Writer
	outputWriter  writer.Writer
	totalRows     int
	PrintMemUsage bool
	WithHeader    bool
	headers       interface{}
}

// CreateSortedChunks Scan a file and divide it into small sorted chunks.
// Store all the chunks in a folder an returns all the paths.
func (f *Info) CreateSortedChunks(ctx context.Context, chunkFolder string, dumpSize, maxWorkers int) ([]string, error) {
	if dumpSize <= 0 {
		return nil, errors.New("dump size must be greater than 0")
	}

	if f.PrintMemUsage && f.mu == nil {
		f.mu = &MemUsage{}
	}

	err := clearChunkFolder(chunkFolder)
	if err != nil {
		return nil, errors.Wrap(err, "can't clear chunk folder")
	}

	inputReader, err := f.Allocate.FnReader(f.InputReader)
	if err != nil {
		return nil, errors.Wrap(err, "can't get input reader")
	}
	count_rows := 0
	chunkPaths := []string{}

	mu := sync.Mutex{}

	batchChan := batchingchannels.NewBatchingChannel(ctx, f.Allocate, maxWorkers, dumpSize)
	batchChan.G.Go(func() error {
		for inputReader.Next() {
			if f.PrintMemUsage {
				f.mu.Collect()
			}
			row, err := inputReader.Read()
			if err != nil {
				return errors.Wrap(err, "can't read from input reader")
			}
			if f.WithHeader && f.headers == nil {
				f.headers = row
			} else {
				batchChan.In() <- row
			}
			count_rows++
		}
		batchChan.Close()
		if inputReader.Err() != nil {
			return errors.Wrap(inputReader.Err(), "input reader encountered an error")
		}
		return nil
	})

	chunkIdx := 0
	err = batchChan.ProcessOut(func(v vector.Vector) error {
		mu.Lock()
		chunkIdx++
		chunkPath := path.Join(chunkFolder, "chunk_"+strconv.Itoa(chunkIdx)+".tsv")
		logger.Infoln("Created chunk", chunkPath)
		mu.Unlock()
		v.Sort()
		if f.WithHeader {
			err = v.PushFrontNoKey(f.headers)
			if err != nil {
				return err
			}
		}
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
		return nil, errors.Wrap(err, "can't process batching channel")
	}
	f.totalRows = count_rows
	return chunkPaths, nil
}
