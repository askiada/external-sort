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
	chunkSize := 10000
	bufferSize := 5000
	f, err := os.Open(filename)
	assert.NoError(b, err)

	fI := &file.Info{
		Reader:     f,
		Allocate:   vector.AllocateIntVector,
		OutputPath: "testdata/chunks/output.tsv",
	}
	chunkPaths, err := fI.CreateSortedChunks(context.Background(), "testdata/chunks", chunkSize, 100)
	assert.NoError(b, err)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = fI.MergeSort(chunkPaths, bufferSize)
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
