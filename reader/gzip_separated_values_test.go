package reader_test

import (
	"bufio"
	"context"
	"os"
	"testing"

	"github.com/askiada/external-sort/internal/rw"
	"github.com/askiada/external-sort/reader"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test(t *testing.T) {
	t.Skip("to rework")
	f, err := os.Open("/mnt/c/Users/Alex/Downloads/recordings.59.tsv.gz")
	require.NoError(t, err)
	rder, err := reader.NewGZipSeparatedValues(bufio.NewReader(f), '\t')
	require.NoError(t, err)
	count := 0
	for rder.Next() {
		row, err := rder.Read()
		require.NoError(t, err)
		_ = row
		count++
	}
	assert.Equal(t, 2853701, count)
	require.NoError(t, rder.Err())
}

func TestS3(t *testing.T) {
	t.Skip("to rework")
	ctx := context.Background()
	i := rw.NewInputOutput(ctx)
	err := i.SetInputReader(ctx, "s3://blokur-data/ml-title/remote/1/f15c2cf2e3ab46589419e6441b64e3bd/artifacts/input/word2vec/refine/recordings.59.tsv.gz")
	require.NoError(t, err)

	gzipReader, err := reader.NewGZipSeparatedValues(i.Input, '\t')
	require.NoError(t, err)
	count := 0
	for gzipReader.Next() {
		row, err := gzipReader.Read()
		require.NoError(t, err)
		_ = row
		count++
	}
	assert.Equal(t, 2853701, count)
	require.NoError(t, gzipReader.Err())
}
