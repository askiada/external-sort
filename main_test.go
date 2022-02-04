package main_test

import (
	"errors"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/askiada/external-sort/file"
	"github.com/askiada/external-sort/vector"

	"github.com/stretchr/testify/assert"
)

func prepareChunks(t *testing.T, filname string, chunkSize int) (*file.Info, []string) {
	t.Helper()
	f, err := os.Open(filname)
	assert.NoError(t, err)

	fI := &file.Info{
		Reader:   f,
		Allocate: vector.AllocateIntVector,
	}
	chunkPaths, err := fI.CreateSortedChunks("testdata/chunks", chunkSize)
	assert.NoError(t, err)

	t.Cleanup(func() {
		defer f.Close()
		dir, err := ioutil.ReadDir("testdata/chunks")
		assert.NoError(t, err)
		for _, d := range dir {
			err = os.RemoveAll(path.Join("testdata/chunks", d.Name()))
			assert.NoError(t, err)
		}
	})

	return fI, chunkPaths
}

func Test(t *testing.T) {
	tcs := map[string]struct {
		filename       string
		expectedErr    error
		expectedOutput []interface{}
	}{
		"empty file": {
			filename: "testdata/emptyfile.tsv",
		},
		"one elem": {
			filename:       "testdata/oneelem.tsv",
			expectedOutput: []interface{}{1},
		},
		"100 elems": {
			filename:       "testdata/100elems.tsv",
			expectedOutput: []interface{}{3, 4, 5, 6, 6, 7, 7, 7, 8, 8, 9, 9, 10, 10, 15, 18, 18, 18, 18, 21, 22, 22, 25, 25, 25, 25, 25, 26, 26, 27, 27, 28, 28, 29, 29, 29, 30, 30, 31, 31, 33, 33, 34, 36, 37, 39, 39, 39, 40, 41, 41, 42, 43, 43, 47, 47, 49, 50, 50, 52, 52, 53, 54, 55, 55, 55, 56, 57, 57, 59, 60, 61, 62, 63, 67, 71, 71, 72, 72, 73, 74, 75, 78, 79, 80, 80, 82, 89, 89, 89, 91, 91, 92, 92, 93, 93, 94, 97, 97, 99},
		},
	}

	for name, tc := range tcs {
		filename := tc.filename
		expectedOutput := tc.expectedOutput
		expectedErr := tc.expectedErr
		t.Run(name, func(t *testing.T) {
			for chunkSize := 1; chunkSize < 152; chunkSize += 10 {
				for bufferSize := 1; bufferSize < 152; bufferSize += 10 {
					fI, chunkPaths := prepareChunks(t, filename, chunkSize)
					got, err := fI.MergeSort(chunkPaths, bufferSize)
					assert.ElementsMatch(t, got, expectedOutput)
					assert.True(t, errors.Is(err, expectedErr))
				}
			}
		})
	}
}
