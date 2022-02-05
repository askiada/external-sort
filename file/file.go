package file

import (
	"bufio"
	"context"

	"io"
	"path"
	"strconv"

	"github.com/askiada/external-sort/vector"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/pkg/errors"
)

type Info struct {
	mu            *MemUsage
	Reader        io.Reader
	Allocate      func(int) vector.Vector
	OutputPath    string
	totalRows     int
	PrintMemUsage bool
}

// Sort Perform a naive sort of a reader and put the results in ascending order in a Vector.
func (f *Info) Sort(file io.Reader) error {
	ans := f.Allocate(0)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		text := scanner.Text()
		err := vector.Sort(ans, text)
		if err != nil {
			return err
		}
	}
	if scanner.Err() != nil {
		return scanner.Err()
	}
	return nil
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
	chunkIdx := 0
	chunkPaths := []string{}
	scanner := bufio.NewScanner(f.Reader)

	g, _ := errgroup.WithContext(ctx)
	sem := semaphore.NewWeighted(maxWorkers)
	ans := f.Allocate(dumpSize)
	for {
		if f.PrintMemUsage {
			f.mu.Collect()
		}
		next := scanner.Scan()
		// create a new chunk every time we reach the maximum length and the last chunk (could be smaller)
		if !next || row%dumpSize == 0 {
			if row > 0 && ans != nil {
				chunkPath, err := addNewDump(ctx, g, ans, sem, chunkFolder, chunkIdx)
				if err != nil {
					return nil, errors.Wrap(err, fn)
				}
				chunkPaths = append(chunkPaths, chunkPath)
				chunkIdx++
				ans = f.Allocate(dumpSize)
			}
		}
		if !next {
			break
		}
		text := scanner.Text()
		err := vector.Sort(ans, text)
		if err != nil {
			return nil, errors.Wrap(err, fn)
		}
		row++
	}
	if scanner.Err() != nil {
		return nil, errors.Wrap(scanner.Err(), fn)
	}
	if err := g.Wait(); err != nil {
		return nil, errors.Wrap(err, fn)
	}
	f.totalRows = row
	if ans == nil {
		return chunkPaths, nil
	}
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
