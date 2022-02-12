package file

import (
	"bufio"
	"context"
	"sync"

	"io"
	"path"
	"strconv"

	"github.com/askiada/external-sort/file/batchingchannels"
	"github.com/askiada/external-sort/vector"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/pkg/errors"
)

type Info struct {
	mu            *MemUsage
	Reader        io.Reader
	Allocate      *vector.Allocate
	OutputPath    string
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

	err := clearFolder(chunkFolder)
	if err != nil {
		return nil, errors.Wrap(err, fn)
	}
	row := 0
	chunkPaths := []string{}
	scanner := bufio.NewScanner(f.Reader)
	mu := sync.Mutex{}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	batchChan := batchingchannels.NewBatchingChannel(context.Background(), f.Allocate, maxWorkers, dumpSize)
	go func() {
		defer wg.Done()
		for scanner.Scan() {
			if f.PrintMemUsage {
				f.mu.Collect()
			}
			text := scanner.Text()
			batchChan.In() <- text
			row++
		}
		batchChan.Close()
	}()

	chunkIdx := 0
	err = batchChan.ProcessOut(func(v vector.Vector) error {
		mu.Lock()
		chunkIdx++
		chunkPath := path.Join(chunkFolder, "chunk_"+strconv.Itoa(chunkIdx)+".tsv")
		mu.Unlock()
		v.Sort()
		err := vector.Dump(v, chunkPath)
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
	wg.Wait()
	if scanner.Err() != nil {
		return nil, errors.Wrap(scanner.Err(), fn)
	}
	f.totalRows = row
	return chunkPaths, nil
}

// addNewDump Add a goroutine to create a new chunk file.
// TODO: Too many parameters.
func addNewDump(ctx context.Context, g *errgroup.Group, ans vector.Vector, sem *semaphore.Weighted, chunkFolder string, chunkIdx int) (chunkPath string, err error) {
	fn := "add dump"
	if err := sem.Acquire(ctx, 1); err != nil {
		return "", err
	}
	chunkPath = path.Join(chunkFolder, "chunk_"+strconv.Itoa(chunkIdx)+".tsv")
	g.Go(func() error {
		defer sem.Release(1)
		err := vector.Dump(ans, chunkPath)
		if err != nil {
			return errors.Wrap(err, fn)
		}
		return nil
	})
	return chunkPath, nil
}
