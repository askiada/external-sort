package main_test

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/askiada/external-sort/file"
	"github.com/askiada/external-sort/reader"
	"github.com/askiada/external-sort/vector"
	"github.com/askiada/external-sort/vector/key"
	"github.com/askiada/external-sort/writer"
	"github.com/stretchr/testify/assert"
)

func BenchmarkMergeSort(b *testing.B) {
	filename := "test.tsv"
	f, err := os.Open(filename)
	assert.NoError(b, err)
	chunkSize := 10000
	bufferSize := 5000
	fI := &file.Info{
		InputReader: f,
		Allocate:    vector.DefaultVector(key.AllocateInt, reader.NewStdScanner, func(w io.Writer) writer.Writer { return writer.NewStdWriter(w) }),
		OutputFile:  "testdata/chunks/output.tsv",
	}
	chunkPaths, err := fI.CreateSortedChunks(context.Background(), "testdata/chunks", chunkSize, 100)
	assert.NoError(b, err)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = fI.MergeSort(chunkPaths, bufferSize)
		_ = err
	}
	dir, err := ioutil.ReadDir("testdata/chunks")
	assert.NoError(b, err)
	for _, d := range dir {
		err = os.RemoveAll(path.Join("testdata/chunks", d.Name()))
		assert.NoError(b, err)
	}
}
