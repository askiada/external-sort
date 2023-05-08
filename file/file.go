package file

import (
	"context"
	"io"
	"path"
	"strconv"
	"sync"

	"github.com/askiada/external-sort/file/batchingchannels"
	"github.com/askiada/external-sort/reader"
	"github.com/askiada/external-sort/vector"
	"github.com/askiada/external-sort/writer"
	"github.com/sirupsen/logrus"

	"github.com/pkg/errors"
)

var logger = logrus.StandardLogger()

// Info set all parameters to process a file with chunks.
type Info struct {
	mu           *memUsage
	Allocate     *vector.Allocate
	InputReader  io.Reader
	OutputFile   io.Writer
	outputWriter writer.Writer

	headers       interface{}
	chunkPaths    []string
	localMutex    sync.Mutex
	totalRows     int
	chunkIndex    int
	PrintMemUsage bool
	WithHeader    bool
}

func (f *Info) check(dumpSize int) error {
	f.chunkIndex = 0
	f.chunkPaths = []string{}
	if dumpSize <= 0 {
		return errors.New("dump size must be greater than 0")
	}
	return nil
}

func (f *Info) processInputReader(batchChan *batchingchannels.BatchingChannel, inputReader reader.Reader) error {
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
		f.totalRows++
	}
	batchChan.Close()
	if inputReader.Err() != nil {
		return errors.Wrap(inputReader.Err(), "input reader encountered an error")
	}
	return nil
}

func (f *Info) processBatch(vec vector.Vector, chunkFolder string) error {
	f.localMutex.Lock()
	f.chunkIndex++
	chunkPath := path.Join(chunkFolder, "chunk_"+strconv.Itoa(f.chunkIndex)+".tsv")
	logger.Infoln("Created chunk", chunkPath)
	f.localMutex.Unlock()
	vec.Sort()
	if f.WithHeader {
		f.localMutex.Lock()
		err := vec.PushFrontNoKey(f.headers)
		if err != nil {
			f.localMutex.Unlock()
			return err
		}
		f.localMutex.Unlock()
	}
	err := f.Allocate.Dump(vec, chunkPath)
	if err != nil {
		return errors.Wrapf(err, "can't dump chunk %s", chunkPath)
	}
	f.localMutex.Lock()
	f.chunkPaths = append(f.chunkPaths, chunkPath)
	f.localMutex.Unlock()
	return nil
}

func (f *Info) runBatchingChannel(
	ctx context.Context,
	inputReader reader.Reader,
	chunkFolder string,
	dumpSize,
	maxWorkers int,
) ([]string, error) {
	batchChan, err := batchingchannels.NewBatchingChannel(ctx, f.Allocate, maxWorkers, dumpSize)
	if err != nil {
		return nil, errors.Wrap(err, "can't create new batching channel")
	}
	batchChan.G.Go(func() error { return f.processInputReader(batchChan, inputReader) })

	err = batchChan.ProcessOut(func(vec vector.Vector) error {
		err := f.processBatch(vec, chunkFolder)
		if err != nil {
			return errors.Wrap(err, "can't process batch")
		}
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "can't process batching channel")
	}
	return f.chunkPaths, nil
}

// CreateSortedChunks Scan a file and divide it into small sorted chunks.
// Store all the chunks in a folder an returns all the paths.
func (f *Info) CreateSortedChunks(ctx context.Context, chunkFolder string, dumpSize, maxWorkers int) ([]string, error) {
	if err := f.check(dumpSize); err != nil {
		return nil, errors.New("can't pass checks")
	}

	if f.PrintMemUsage && f.mu == nil {
		f.mu = &memUsage{}
	}

	err := clearChunkFolder(chunkFolder)
	if err != nil {
		return nil, errors.Wrap(err, "can't clear chunk folder")
	}

	inputReader, err := f.Allocate.FnReader(f.InputReader)
	if err != nil {
		return nil, errors.Wrap(err, "can't get input reader")
	}
	chunkPaths, err := f.runBatchingChannel(ctx, inputReader, chunkFolder, dumpSize, maxWorkers)
	if err != nil {
		return nil, errors.Wrap(err, "can't run batching channel")
	}
	return chunkPaths, nil
}
