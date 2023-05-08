package main_test

import (
	"bufio"
	"context"
	"errors"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"testing"

	"github.com/askiada/external-sort/file"
	"github.com/askiada/external-sort/vector"
	"github.com/askiada/external-sort/vector/key"

	"github.com/stretchr/testify/assert"
)

func prepareChunks(ctx context.Context, t *testing.T, allocate *vector.Allocate, filename, outputFilename string, chunkSize int) (*file.Info, []string) {
	t.Helper()
	f, err := os.Open(filename)
	assert.NoError(t, err)

	fI := &file.Info{
		Reader:     f,
		Allocate:   allocate,
		InputPath:  filename,
		OutputPath: outputFilename,
	}
	chunkPaths, err := fI.CreateSortedChunks(ctx, "testdata/chunks", chunkSize, 10)
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

func TestBasics(t *testing.T) {
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
	allocate := vector.DefaultVector(key.AllocateInt)
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
					fI, chunkPaths := prepareChunks(ctx, t, allocate, filename, outputFilename, chunkSize)
					fI.OutputPath = outputFilename
					err := fI.MergeSort(chunkPaths, bufferSize, bufferSize)
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

func Test100Elems(t *testing.T) {
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
	allocate := vector.DefaultVector(key.AllocateInt)
	for name, tc := range tcs {
		filename := tc.filename
		outputFilename := tc.outputFilename
		expectedOutput := tc.expectedOutput
		expectedErr := tc.expectedErr
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			fI, chunkPaths := prepareChunks(ctx, t, allocate, filename, outputFilename, 50)
			err := fI.MergeSort(chunkPaths, 10, 10)
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

func Test10Elems(t *testing.T) {
	tcs := map[string]struct {
		filename       string
		outputFilename string
		expectedErr    error
		expectedOutput []string
	}{
		"100 elems": {
			filename:       "testdata/10elems.tsv",
			expectedOutput: []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"},
			outputFilename: "testdata/chunks/output.tsv",
		},
	}
	allocate := vector.DefaultVector(key.AllocateInt)
	for name, tc := range tcs {
		filename := tc.filename
		outputFilename := tc.outputFilename
		expectedOutput := tc.expectedOutput
		expectedErr := tc.expectedErr
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			fI, chunkPaths := prepareChunks(ctx, t, allocate, filename, outputFilename, 50)
			err := fI.MergeSort(chunkPaths, 10, 10)
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

func TestTsvKey(t *testing.T) {
	tcs := map[string]struct {
		filename       string
		outputFilename string
		expectedErr    error
		expectedOutput []string
	}{
		"Tsv file": {
			filename: "testdata/multifields.tsv",
			expectedOutput: []string{"3	D	equipment",
				"7	G	inflation",
				"6	H	delivery",
				"9	I	child",
				"5	J	magazine",
				"8	M	garbage",
				"1	N	guidance",
				"10	S	feedback",
				"2	T	library",
				"4	Z	news"},
			outputFilename: "testdata/chunks/output.tsv",
		},
	}
	allocate := &vector.Allocate{
		Vector: vector.AllocateSlice,
		Key: func(line string) (key.Key, error) {
			return key.AllocateTsv(line, 1)
		},
		EmptyKey: key.AllocateEmptyTsv,
	}
	for name, tc := range tcs {
		filename := tc.filename
		outputFilename := tc.outputFilename
		expectedOutput := tc.expectedOutput
		expectedErr := tc.expectedErr
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			fI, chunkPaths := prepareChunks(ctx, t, allocate, filename, outputFilename, 21)
			err := fI.MergeSort(chunkPaths, 10, 10)
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
