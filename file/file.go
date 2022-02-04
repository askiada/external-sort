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
	Reader   io.Reader
	Allocate func(int) vector.Vector
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
		next := scanner.Scan()
		if !next || row%dumpSize == 0 {
			if row > 0 && ans != nil {
				if err := sem.Acquire(ctx, 1); err != nil {
					return nil, err
				}
				localVec := ans
				chunkPath := path.Join(chunkFolder, "chunk_"+strconv.Itoa(chunkIdx)+".tsv")
				g.Go(func() error {
					defer sem.Release(1)
					err := localVec.Dump(chunkPath)
					if err != nil {
						return errors.Wrap(err, fn)
					}
					return nil
				})
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
	if ans == nil {
		return chunkPaths, nil
	}
	return chunkPaths, nil
}
