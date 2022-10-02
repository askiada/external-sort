package file

import (
	"context"
	"io"
	"math/rand"
	"path"
	"strconv"
	"sync"

	"github.com/askiada/external-sort/file/batchingchannels"
	"github.com/askiada/external-sort/reader"
	"github.com/askiada/external-sort/vector"
	"github.com/askiada/external-sort/vector/key"
	"github.com/askiada/external-sort/writer"
	"github.com/pkg/errors"
)

// CreateSortedChunks Scan a file and divide it into small sorted chunks.
// Store all the chunks in a folder an returns all the paths.
func (f *Info) Shuffle(ctx context.Context, chunkFolder string, dumpSize int, maxWorkers int64, k int, seed int64, isGzip bool) ([]string, error) {
	fn := "scan and shuffle and dump"
	if dumpSize <= 0 {
		return nil, errors.Wrap(errors.New("dump size must be greater than 0"), fn)
	}

	if f.PrintMemUsage && f.mu == nil {
		f.mu = &MemUsage{}
	}
	if f.Allocate != nil {
		return nil, errors.New("allocate should not be defined when shuffling")
	}
	f.Allocate = vector.DefaultVector(
		func(row interface{}) (key.Key, error) {
			return key.AllocateIntFromSlice(row, 0)
		},
		func(r io.Reader) (reader.Reader, error) {
			return reader.NewStdScanner(r, isGzip)
		},
		func(w io.Writer) (writer.Writer, error) {
			return writer.NewStdSliceWriter(w, false, isGzip), nil
		},
	)

	err := clearChunkFolder(chunkFolder)
	if err != nil {
		return nil, errors.Wrap(err, fn)
	}

	inputReader, err := f.Allocate.FnReader(f.InputReader)
	if err != nil {
		return nil, errors.Wrap(err, fn)
	}
	countRows := 0
	chunkPaths := []string{}

	mu := sync.Mutex{}
	r := rand.New(rand.NewSource(seed))
	batchChan := batchingchannels.NewBatchingChannel(ctx, f.Allocate, maxWorkers, dumpSize)
	batchChan.G.Go(func() error {
		for inputReader.Next() {
			if f.PrintMemUsage {
				f.mu.Collect()
			}
			row, err := inputReader.Read()
			if err != nil {
				return errors.Wrap(err, fn)
			}
			if f.WithHeader && f.headers == nil {
				f.headers = []string{"##!!##", row.(string)}
			} else {
				newRow := []string{strconv.FormatInt(r.Int63(), 10), row.(string)}
				batchChan.In() <- newRow
			}
			countRows++
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
		return nil, errors.Wrap(err, fn)
	}
	f.totalRows = countRows

	f.Allocate = vector.DefaultVector(
		func(row interface{}) (key.Key, error) {
			return key.AllocateIntFromSlice(row, 0)
		},
		func(r io.Reader) (reader.Reader, error) {
			return reader.NewStdSliceScanner(r, isGzip)
		},
		func(w io.Writer) (writer.Writer, error) {
			return writer.NewStdSliceWriter(w, true, isGzip), nil
		},
	)
	err = f.MergeSort(chunkPaths, k, false)
	if err != nil {
		return nil, errors.Wrap(err, fn)
	}
	return chunkPaths, nil
}
