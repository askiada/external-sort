package file

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"path"
	"strconv"
	"sync"

	"github.com/askiada/external-sort/file/batchingchannels"
	"github.com/askiada/external-sort/vector"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

type Info struct {
	mu            *MemUsage
	Reader        io.Reader
	Allocate      func(int) vector.Vector
	OutputPath    string
	totalRows     int
	PrintMemUsage bool
}

// CreateSortedChunks Scan a file and divide it into small sorted chunks.
// Store all the chunks in a folder an returns all the paths.
func (f *Info) CreateSortedChunks(ctx context.Context, chunkFolder string, dumpSize int, maxWorkers int64) ([]string, error) {
	if dumpSize <= 0 {
		return nil, errors.New("dump size must be greater than 0")
	}

	if f.PrintMemUsage && f.mu == nil {
		f.mu = &MemUsage{}
	}

	err := clearFolder(chunkFolder)
	if err != nil {
		return nil, errors.Wrap(err, "cleaning up temporary folder")
	}
	row := 0
	chunkPaths := []string{}
	scanner := bufio.NewScanner(f.Reader)
	mu := sync.Mutex{}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	batchChan := batchingchannels.NewBatchingChannel(context.Background(), maxWorkers, dumpSize, f.Allocate)
	var globalErr error
	go func() {
		defer wg.Done()
		chunkIdx := 0
		globalErr = batchChan.ProcessOut(func(v vector.Vector) error {
			mu.Lock()
			chunkIdx++
			chunkPath := path.Join(chunkFolder, "chunk_"+strconv.Itoa(chunkIdx)+".tsv")
			mu.Unlock()
			err := v.Sort()
			if err != nil {
				return errors.Wrap(err, "sorting vector")
			}
			err = v.Dump(chunkPath)
			if err != nil {
				return errors.Wrap(err, "dumping vector")
			}
			mu.Lock()
			chunkPaths = append(chunkPaths, chunkPath)
			mu.Unlock()
			return nil
		})
		if f.PrintMemUsage {
			fmt.Println("Done creating chunks")
		}
	}()

	for scanner.Scan() {
		if globalErr != nil {
			return nil, globalErr
		}
		if f.PrintMemUsage {
			f.mu.Collect()
		}
		text := scanner.Text()
		batchChan.In() <- text
		row++
	}
	if scanner.Err() != nil {
		return nil, errors.Wrap(scanner.Err(), "scanning file")
	}
	batchChan.Close()
	wg.Wait()
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
		err := ans.Dump(chunkPath)
		if err != nil {
			return errors.Wrap(err, fn)
		}
		return nil
	})
	return chunkPath, nil
}
