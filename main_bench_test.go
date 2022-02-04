package main_test

import (
	"context"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/askiada/external-sort/file"
	"github.com/askiada/external-sort/vector"
	"github.com/stretchr/testify/assert"
)

func BenchmarkMergeSort(b *testing.B) {
	filename := "test.tsv"
	chunkSize := 100000
	bufferSize := 5000
	f, err := os.Open(filename)
	assert.NoError(b, err)

	fI := &file.Info{
		Reader:   f,
		Allocate: vector.AllocateIntVector,
	}
	chunkPaths, err := fI.CreateSortedChunks(context.Background(), "testdata/chunks", chunkSize, 10)
	assert.NoError(b, err)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := fI.MergeSort(chunkPaths, bufferSize)
		_ = got
		_ = err
	}
	f.Close()
	dir, err := ioutil.ReadDir("testdata/chunks")
	assert.NoError(b, err)
	for _, d := range dir {
		err = os.RemoveAll(path.Join("testdata/chunks", d.Name()))
		assert.NoError(b, err)
	}
}
