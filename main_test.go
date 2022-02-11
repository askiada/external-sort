package main_test

import (
	"bufio"
	"context"
	"errors"
	"os"
	"path"
	"strconv"
	"testing"

	"github.com/askiada/external-sort/file"
	"github.com/askiada/external-sort/vector"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type allocator func(int) vector.Vector

func prepareChunks(ctx context.Context, t *testing.T, filename, outputFilename string, chunkSize int, allocate allocator) (*file.Info, []string) {
	t.Helper()
	f, err := os.Open(filename)
	assert.NoError(t, err)

	fI := &file.Info{
		Reader:     f,
		Allocate:   allocate,
		OutputPath: outputFilename,
	}
	chunkPaths, err := fI.CreateSortedChunks(ctx, "testdata/chunks", chunkSize, 10)
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

func Test(t *testing.T) {
	tcs := map[string]struct {
		filename       string
		outputFilename string
		expectedErr    error
		expectedOutput []string
	}{
		"empty file": {
			filename:       "testdata/emptyfile.tsv",
			outputFilename: "testdata/chunks/output.tsv",
		},
		"one elem": {
			filename:       "testdata/oneelem.tsv",
			expectedOutput: []string{"1"},
			outputFilename: "testdata/chunks/output.tsv",
		},
		"100 elems": {
			filename:       "testdata/100elems.tsv",
			expectedOutput: []string{"3", "4", "5", "6", "6", "7", "7", "7", "8", "8", "9", "9", "10", "10", "15", "18", "18", "18", "18", "21", "22", "22", "25", "25", "25", "25", "25", "26", "26", "27", "27", "28", "28", "29", "29", "29", "30", "30", "31", "31", "33", "33", "34", "36", "37", "39", "39", "39", "40", "41", "41", "42", "43", "43", "47", "47", "49", "50", "50", "52", "52", "53", "54", "55", "55", "55", "56", "57", "57", "59", "60", "61", "62", "63", "67", "71", "71", "72", "72", "73", "74", "75", "78", "79", "80", "80", "82", "89", "89", "89", "91", "91", "92", "92", "93", "93", "94", "97", "97", "99"},
			outputFilename: "testdata/chunks/output.tsv",
		},
	}

	for name, tc := range tcs {
		filename := tc.filename
		outputFilename := tc.outputFilename
		expectedOutput := tc.expectedOutput
		expectedErr := tc.expectedErr
		for chunkSize := 1; chunkSize < 152; chunkSize += 10 {
			for bufferSize := 1; bufferSize < 152; bufferSize += 10 {
				chunkSize := chunkSize
				bufferSize := bufferSize
				t.Run(name+"_"+strconv.Itoa(chunkSize)+"_"+strconv.Itoa(bufferSize), func(t *testing.T) {
					ctx := context.Background()
					fI, chunkPaths := prepareChunks(ctx, t, filename, outputFilename, chunkSize, vector.AllocateIntVector)
					fI.OutputPath = outputFilename
					err := fI.MergeSort(chunkPaths, bufferSize)
					assert.NoError(t, err)
					outputFile, err := os.Open(outputFilename)
					assert.NoError(t, err)
					outputScanner := bufio.NewScanner(outputFile)
					count := 0
					for outputScanner.Scan() {
						assert.Equal(t, expectedOutput[count], outputScanner.Text())
						count++
					}
					assert.NoError(t, outputScanner.Err())
					assert.Equal(t, len(expectedOutput), count)
					assert.True(t, errors.Is(err, expectedErr))
					outputFile.Close()
				})
			}
		}
	}
}

func TestSimple(t *testing.T) {
	tcs := map[string]struct {
		filename       string
		outputFilename string
		expectedErr    error
		expectedOutput []string
	}{
		"100 elems": {
			filename:       "testdata/100elems.tsv",
			expectedOutput: []string{"3", "4", "5", "6", "6", "7", "7", "7", "8", "8", "9", "9", "10", "10", "15", "18", "18", "18", "18", "21", "22", "22", "25", "25", "25", "25", "25", "26", "26", "27", "27", "28", "28", "29", "29", "29", "30", "30", "31", "31", "33", "33", "34", "36", "37", "39", "39", "39", "40", "41", "41", "42", "43", "43", "47", "47", "49", "50", "50", "52", "52", "53", "54", "55", "55", "55", "56", "57", "57", "59", "60", "61", "62", "63", "67", "71", "71", "72", "72", "73", "74", "75", "78", "79", "80", "80", "82", "89", "89", "89", "91", "91", "92", "92", "93", "93", "94", "97", "97", "99"},
			outputFilename: "testdata/chunks/output.tsv",
		},
	}

	for name, tc := range tcs {
		filename := tc.filename
		outputFilename := tc.outputFilename
		expectedOutput := tc.expectedOutput
		expectedErr := tc.expectedErr
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			fI, chunkPaths := prepareChunks(ctx, t, filename, outputFilename, 21, vector.AllocateIntVector)
			err := fI.MergeSort(chunkPaths, 10)
			assert.NoError(t, err)
			outputFile, err := os.Open(outputFilename)
			assert.NoError(t, err)
			outputScanner := bufio.NewScanner(outputFile)
			count := 0
			for outputScanner.Scan() {
				assert.Equal(t, expectedOutput[count], outputScanner.Text())
				count++
			}
			assert.NoError(t, outputScanner.Err())
			assert.Equal(t, len(expectedOutput), count)
			assert.True(t, errors.Is(err, expectedErr))
			outputFile.Close()
		})
	}
}

func TestTableVector(t *testing.T) {
	tcs := map[string]struct {
		filename       string
		outputFilename string
		want           []string
	}{
		"table_vector": {
			filename: "testdata/table_vector.tsv",
			want: []string{
				"9	1	b",
				"6	2	a",
				"8	3	z",
				"13	4	z",
				"4	5	z",
				"11	6	z",
				"1	7	z",
				"15	8	z",
				"3	9	z",
				"5	10	z",
				"2	11	z",
				"12	12	z",
				"14	13	z",
				"7	14	z",
				"10	15	z",
			},
			outputFilename: "testdata/chunks/output.tsv",
		},
	}

	ctx := context.Background()
	for name, tc := range tcs {
		tc := tc
		t.Run(name, func(t *testing.T) {
			f, chunkPaths := prepareChunks(ctx, t, tc.filename, tc.outputFilename, 3, vector.AllocateTableVector("\t", 1))
			err := f.MergeSort(chunkPaths, 3)
			assert.NoError(t, err)

			outputFile, err := os.Open(tc.outputFilename)
			assert.NoError(t, err)
			defer outputFile.Close()

			outputScanner := bufio.NewScanner(outputFile)
			var got []string
			for outputScanner.Scan() {
				got = append(got, outputScanner.Text())
				require.NoError(t, outputScanner.Err())
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("(-want +got):\n%s", diff)
			}
		})
	}
}
