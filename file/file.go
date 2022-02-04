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

	vectors := make(chan vector.Vector)
	g, _ := errgroup.WithContext(ctx)
	sem := semaphore.NewWeighted(maxWorkers)
	var globalErr error
	go func() {
		for vec := range vectors {
			if err := sem.Acquire(ctx, 1); err != nil {
				globalErr = err
			}
			localVec := vec
			chunkPath := path.Join(chunkFolder, "chunk_"+strconv.Itoa(chunkIdx)+".tsv")
			g.Go(func() error {
				defer sem.Release(1)
				if globalErr != nil {
					return errors.Wrap(globalErr, fn)
				}
				err := localVec.Dump(chunkPath)
				if err != nil {
					return errors.Wrap(err, fn)
				}
				return nil
			})
			chunkPaths = append(chunkPaths, chunkPath)
			chunkIdx++
		}
	}()
	var ans vector.Vector
	for scanner.Scan() {
		if row%dumpSize == 0 {
			if row != 0 {
				vectors <- ans
			}
			ans = f.Allocate(dumpSize)
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

	chunkPath, err := dumpChunk(ans, chunkFolder, chunkIdx)
	if err != nil {
		return nil, errors.Wrap(err, fn)
	}
	chunkPaths = append(chunkPaths, chunkPath)
	return chunkPaths, nil
}

func dumpChunk(ans vector.Vector, folder string, chunkIdx int) (string, error) {
	fn := "dump chunk"
	chunkPath := path.Join(folder, "chunk_"+strconv.Itoa(chunkIdx)+".tsv")
	err := ans.Dump(chunkPath)
	if err != nil {
		return "", errors.Wrap(err, fn)
	}
	return chunkPath, nil
}
