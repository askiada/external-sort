package file

import (
	"bufio"

	"io"
	"path"
	"strconv"

	"github.com/askiada/external-sort/vector"

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
func (f *Info) CreateSortedChunks(chunkFolder string, dumpSize int) ([]string, error) {
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
	var ans vector.Vector
	for scanner.Scan() {
		if row%dumpSize == 0 {
			if row != 0 {
				chunkPath, err := dumpChunk(ans, chunkFolder, chunkIdx)
				if err != nil {
					return nil, errors.Wrap(err, fn)
				}
				chunkPaths = append(chunkPaths, chunkPath)
				chunkIdx++
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
