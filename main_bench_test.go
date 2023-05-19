package main_test

import (
	"context"
	"io"
	"os"
	"path"
	"testing"

	"github.com/askiada/external-sort/file"
	"github.com/askiada/external-sort/internal/rw"
	"github.com/askiada/external-sort/reader"
	"github.com/askiada/external-sort/vector"
	"github.com/askiada/external-sort/vector/key"
	"github.com/askiada/external-sort/writer"
	"github.com/stretchr/testify/assert"
)

func BenchmarkMergeSort(b *testing.B) {
	filename := "test.tsv"
	ctx := context.Background()
	inputOutput := rw.NewInputOutput(ctx)
	err := inputOutput.SetInputReader(ctx, filename)
	assert.NoError(b, err)
	err = inputOutput.SetOutputWriter(ctx, "testdata/chunks/output.tsv")
	assert.NoError(b, err)
	chunkSize := 10000
	bufferSize := 5000
	fileInfo := &file.Info{
		InputReader: inputOutput.Input,
		Allocate: vector.DefaultVector(
			key.AllocateInt,
			func(r io.Reader) (reader.Reader, error) { return reader.NewStdScanner(r, false) },
			func(w io.Writer) (writer.Writer, error) { return writer.NewStdWriter(w), nil },
		),
		OutputFile: inputOutput.Output,
	}
	inputOutput.Do(func() (err error) {
		chunkPaths, err := fileInfo.CreateSortedChunks(context.Background(), "testdata/chunks", chunkSize, 100)
		assert.NoError(b, err)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err = fileInfo.MergeSort(chunkPaths, bufferSize, false)
			_ = err
		}

		return nil
	})
	err = inputOutput.Err()
	assert.NoError(b, err)
	dir, err := os.ReadDir("testdata/chunks")
	assert.NoError(b, err)
	for _, d := range dir {
		err = os.RemoveAll(path.Join("testdata/chunks", d.Name()))
		assert.NoError(b, err)
	}
}
