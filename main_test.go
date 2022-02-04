package main_test

import (
	"errors"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/askiada/external-sort/file"
	"github.com/askiada/external-sort/vector"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func prepareChunks(t *testing.T, filname string, chunkSize int) (*file.Info, []string) {
	t.Helper()
	f, err := os.Open(filname)
	assert.NoError(t, err)

	fI := &file.Info{
		Reader:   f,
		Allocate: vector.AllocateTableVector("", 0),
	}
	chunkPaths, err := fI.CreateSortedChunks("testdata/chunks", chunkSize)
	assert.NoError(t, err)

	t.Cleanup(func() {
		defer f.Close()
		dir, err := os.ReadDir("testdata/chunks")
		assert.NoError(t, err)
		for _, d := range dir {
			err = os.RemoveAll(path.Join("testdata/chunks", d.Name()))
			assert.NoError(t, err)
		}
	})

	return fI, chunkPaths
}

func TestMergeSort(t *testing.T) {
	got, err := os.ReadFile("testdata/table.sorted.tsv")
	require.NoError(t, err)

	tableSorted := strings.Split(string(got), "\n")
	tableSorted = tableSorted[:len(tableSorted)-1]

	tcs := map[string]struct {
		filename       string
		expectedErr    error
		expectedOutput []string
	}{
		"empty file": {
			filename: "testdata/emptyfile.tsv",
		},
		"one elem": {
			filename:       "testdata/oneelem.tsv",
			expectedOutput: []string{"1"},
		},
		"100 elems": {
			filename:       "testdata/100elems.tsv",
			expectedOutput: []string{"3", "4", "5", "6", "6", "7", "7", "7", "8", "8", "9", "9", "10", "10", "15", "18", "18", "18", "18", "21", "22", "22", "25", "25", "25", "25", "25", "26", "26", "27", "27", "28", "28", "29", "29", "29", "30", "30", "31", "31", "33", "33", "34", "36", "37", "39", "39", "39", "40", "41", "41", "42", "43", "43", "47", "47", "49", "50", "50", "52", "52", "53", "54", "55", "55", "55", "56", "57", "57", "59", "60", "61", "62", "63", "67", "71", "71", "72", "72", "73", "74", "75", "78", "79", "80", "80", "82", "89", "89", "89", "91", "91", "92", "92", "93", "93", "94", "97", "97", "99"},
		},
		"table file": {
			filename:       "testdata/table.shuffled.tsv",
			expectedOutput: tableSorted,
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
					elements, err := fI.MergeSort(chunkPaths, bufferSize)
					got := make([]string, 0, len(elements))
					for _, e := range elements {
						got = append(got, e.Value())
					}
					assert.ElementsMatch(t, got, expectedOutput)
					assert.True(t, errors.Is(err, expectedErr))
				}
			}
		})
	}
}
