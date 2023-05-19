package main_test

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"strconv"
	"testing"

	"github.com/askiada/external-sort/file"
	"github.com/askiada/external-sort/internal/rw"
	"github.com/askiada/external-sort/reader"
	"github.com/askiada/external-sort/vector"
	"github.com/askiada/external-sort/vector/key"
	"github.com/askiada/external-sort/writer"

	"github.com/stretchr/testify/assert"
)

func prepareChunks(
	ctx context.Context,
	t *testing.T,
	allocate *vector.Allocate,
	filename, outputFilename string,
	chunkSize int,
	mergeSort bool,
	bufferSize int,
	withHeaders bool,
	dropDuplicates bool,
) *file.Info {
	t.Helper()
	inputOutput := rw.NewInputOutput(ctx)
	err := inputOutput.SetInputReader(ctx, filename)
	assert.NoError(t, err)
	err = inputOutput.SetOutputWriter(ctx, outputFilename)
	assert.NoError(t, err)
	fileInfo := &file.Info{
		InputReader: inputOutput.Input,
		Allocate:    allocate,
		OutputFile:  inputOutput.Output,
		WithHeader:  withHeaders,
	}
	inputOutput.Do(func() (err error) {
		chunkPaths, err := fileInfo.CreateSortedChunks(ctx, "testdata/chunks", chunkSize, 10)
		assert.NoError(t, err)
		if mergeSort {
			return fileInfo.MergeSort(chunkPaths, bufferSize, dropDuplicates)
		}
		return nil
	})
	err = inputOutput.Err()
	assert.NoError(t, err)

	return fileInfo
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
			filename: "testdata/100elems.tsv",
			expectedOutput: []string{
				"3", "4", "5", "6", "6",
				"7", "7", "7", "8", "8",
				"9", "9", "10", "10", "15",
				"18", "18", "18", "18", "21",
				"22", "22", "25", "25", "25",
				"25", "25", "26", "26", "27",
				"27", "28", "28", "29", "29",
				"29", "30", "30", "31", "31",
				"33", "33", "34", "36", "37",
				"39", "39", "39", "40", "41",
				"41", "42", "43", "43", "47",
				"47", "49", "50", "50", "52",
				"52", "53", "54", "55", "55",
				"55", "56", "57", "57", "59",
				"60", "61", "62", "63", "67",
				"71", "71", "72", "72", "73",
				"74", "75", "78", "79", "80",
				"80", "82", "89", "89", "89",
				"91", "91", "92", "92", "93",
				"93", "94", "97", "97", "99",
			},
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

					allocate := vector.DefaultVector(
						key.AllocateInt,
						func(r io.Reader) (reader.Reader, error) { return reader.NewStdScanner(r, false) },
						func(w io.Writer) (writer.Writer, error) { return writer.NewStdWriter(w), nil },
					)
					prepareChunks(ctx, t, allocate, filename, outputFilename, chunkSize, true, bufferSize, false, false)

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
			filename: "testdata/100elems.tsv",
			expectedOutput: []string{
				"3", "4", "5", "6", "6",
				"7", "7", "7", "8", "8",
				"9", "9", "10", "10", "15",
				"18", "18", "18", "18", "21",
				"22", "22", "25", "25", "25",
				"25", "25", "26", "26", "27",
				"27", "28", "28", "29", "29",
				"29", "30", "30", "31", "31",
				"33", "33", "34", "36", "37",
				"39", "39", "39", "40", "41",
				"41", "42", "43", "43", "47",
				"47", "49", "50", "50", "52",
				"52", "53", "54", "55", "55",
				"55", "56", "57", "57", "59",
				"60", "61", "62", "63", "67",
				"71", "71", "72", "72", "73",
				"74", "75", "78", "79", "80",
				"80", "82", "89", "89", "89",
				"91", "91", "92", "92", "93",
				"93", "94", "97", "97", "99",
			},
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
			allocate := vector.DefaultVector(
				key.AllocateInt,
				func(r io.Reader) (reader.Reader, error) { return reader.NewStdScanner(r, false) },
				func(w io.Writer) (writer.Writer, error) { return writer.NewStdWriter(w), nil },
			)
			prepareChunks(ctx, t, allocate, filename, outputFilename, 21, true, 10, false, false)
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

func Test100ElemsWithDuplicates(t *testing.T) {
	tcs := map[string]struct {
		filename       string
		outputFilename string
		expectedErr    error
		expectedOutput []string
	}{
		"100 elems with duplicates": {
			filename: "testdata/100elems.tsv",
			expectedOutput: []string{
				"3", "4", "5", "6", "7",
				"8", "9", "10", "15", "18",
				"21", "22", "25", "26", "27",
				"28", "29", "30", "31", "33",
				"34", "36", "37", "39", "40",
				"41", "42", "43", "47", "49",
				"50", "52", "53", "54", "55",
				"56", "57", "59", "60", "61",
				"62", "63", "67", "71", "72",
				"73", "74", "75", "78", "79",
				"80", "82", "89", "91", "92",
				"93", "94", "97", "99",
			},
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
			allocate := vector.DefaultVector(
				key.AllocateInt,
				func(r io.Reader) (reader.Reader, error) { return reader.NewStdScanner(r, false) },
				func(w io.Writer) (writer.Writer, error) { return writer.NewStdWriter(w), nil },
			)
			prepareChunks(ctx, t, allocate, filename, outputFilename, 21, true, 10, false, true)
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

func Test100ElemsWithHeaders(t *testing.T) {
	tcs := map[string]struct {
		filename       string
		outputFilename string
		expectedErr    error
		expectedOutput []string
	}{
		"100 elems with headers": {
			filename: "testdata/100elemsWithHeaders.tsv",
			expectedOutput: []string{
				"headers", "3", "4", "5", "6", "6",
				"7", "7", "7", "8", "8",
				"9", "9", "10", "10", "15",
				"18", "18", "18", "18", "21",
				"22", "22", "25", "25", "25",
				"25", "25", "26", "26", "27",
				"27", "28", "28", "29", "29",
				"29", "30", "30", "31", "31",
				"33", "33", "34", "36", "37",
				"39", "39", "39", "40", "41",
				"41", "42", "43", "43", "47",
				"47", "49", "50", "50", "52",
				"52", "53", "54", "55", "55",
				"55", "56", "57", "57", "59",
				"60", "61", "62", "63", "67",
				"71", "71", "72", "72", "73",
				"74", "75", "78", "79", "80",
				"80", "82", "89", "89", "89",
				"91", "91", "92", "92", "93",
				"93", "94", "97", "97", "99",
			},
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
			allocate := vector.DefaultVector(
				key.AllocateInt,
				func(r io.Reader) (reader.Reader, error) { return reader.NewStdScanner(r, false) },
				func(w io.Writer) (writer.Writer, error) { return writer.NewStdWriter(w), nil },
			)
			prepareChunks(ctx, t, allocate, filename, outputFilename, 21, true, 10, true, false)
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

func Test100ElemsWithHeadersWithDuplicates(t *testing.T) {
	tcs := map[string]struct {
		filename       string
		outputFilename string
		expectedErr    error
		expectedOutput []string
	}{
		"100 elems with headers and duplicates": {
			filename: "testdata/100elemsWithHeaders.tsv",
			expectedOutput: []string{
				"headers", "3", "4", "5", "6", "7",
				"8", "9", "10", "15", "18",
				"21", "22", "25", "26", "27",
				"28", "29", "30", "31", "33",
				"34", "36", "37", "39", "40",
				"41", "42", "43", "47", "49",
				"50", "52", "53", "54", "55",
				"56", "57", "59", "60", "61",
				"62", "63", "67", "71", "72",
				"73", "74", "75", "78", "79",
				"80", "82", "89", "91", "92",
				"93", "94", "97", "99",
			},
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
			allocate := vector.DefaultVector(
				key.AllocateInt,
				func(r io.Reader) (reader.Reader, error) { return reader.NewStdScanner(r, false) },
				func(w io.Writer) (writer.Writer, error) { return writer.NewStdWriter(w), nil },
			)
			prepareChunks(ctx, t, allocate, filename, outputFilename, 21, true, 10, true, true)
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
			expectedOutput: []string{
				"3	D	equipment",
				"7	G	inflation",
				"6	H	delivery",
				"9	I	child",
				"5	J	magazine",
				"8	M	garbage",
				"1	N	guidance",
				"10	S	feedback",
				"2	T	library",
				"4	Z	news",
			},
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

			allocate := vector.DefaultVector(
				func(row interface{}) (key.Key, error) { return key.AllocateTsv(row, 1) },
				func(r io.Reader) (reader.Reader, error) { return reader.NewSeparatedValues(r, '\t'), nil },
				func(w io.Writer) (writer.Writer, error) { return writer.NewSeparatedValues(w, '\t'), nil },
			)
			prepareChunks(ctx, t, allocate, filename, outputFilename, 21, true, 10, false, false)
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

func prepareChunksShuffle(
	ctx context.Context,
	t *testing.T,
	filename, outputFilename string,
	chunkSize int,
	mergeSort bool,
	bufferSize int,
	withHeaders bool,
	dropDuplicates,
	isGzip bool,
) *file.Info {
	t.Helper()
	inputOutput := rw.NewInputOutput(ctx)
	err := inputOutput.SetInputReader(ctx, filename)
	assert.NoError(t, err)
	err = inputOutput.SetOutputWriter(ctx, outputFilename)
	assert.NoError(t, err)
	fileInfo := &file.Info{
		InputReader: inputOutput.Input,
		OutputFile:  inputOutput.Output,
		WithHeader:  withHeaders,
	}
	inputOutput.Do(func() (err error) {
		_, err = fileInfo.Shuffle(ctx, "testdata/chunks", chunkSize, 10, bufferSize, 13, isGzip)
		assert.NoError(t, err)
		return nil
	})
	err = inputOutput.Err()
	assert.NoError(t, err)

	return fileInfo
}

func Test100ElemsShuffle(t *testing.T) {
	t.Skip("to rework")
	tcs := map[string]struct {
		filename       string
		outputFilename string
		expectedErr    error
		expectedOutput []string
	}{
		"100 elems": {
			filename: "testdata/100elems.tsv",
			expectedOutput: []string{
				"3", "4", "5", "6", "6",
				"7", "7", "7", "8", "8",
				"9", "9", "10", "10", "15",
				"18", "18", "18", "18", "21",
				"22", "22", "25", "25", "25",
				"25", "25", "26", "26", "27",
				"27", "28", "28", "29", "29",
				"29", "30", "30", "31", "31",
				"33", "33", "34", "36", "37",
				"39", "39", "39", "40", "41",
				"41", "42", "43", "43", "47",
				"47", "49", "50", "50", "52",
				"52", "53", "54", "55", "55",
				"55", "56", "57", "57", "59",
				"60", "61", "62", "63", "67",
				"71", "71", "72", "72", "73",
				"74", "75", "78", "79", "80",
				"80", "82", "89", "89", "89",
				"91", "91", "92", "92", "93",
				"93", "94", "97", "97", "99",
			},
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
			prepareChunksShuffle(ctx, t, filename, outputFilename, 21, false, 10, false, false, false)
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

func Test100ElemsShuffleWithHeaders(t *testing.T) {
	t.Skip("to rework")
	tcs := map[string]struct {
		filename       string
		outputFilename string
		expectedErr    error
		expectedOutput []string
	}{
		"100 elems with headers": {
			filename: "testdata/100elemsWithHeaders.tsv",
			expectedOutput: []string{
				"headers", "3", "4", "5", "6", "6",
				"7", "7", "7", "8", "8",
				"9", "9", "10", "10", "15",
				"18", "18", "18", "18", "21",
				"22", "22", "25", "25", "25",
				"25", "25", "26", "26", "27",
				"27", "28", "28", "29", "29",
				"29", "30", "30", "31", "31",
				"33", "33", "34", "36", "37",
				"39", "39", "39", "40", "41",
				"41", "42", "43", "43", "47",
				"47", "49", "50", "50", "52",
				"52", "53", "54", "55", "55",
				"55", "56", "57", "57", "59",
				"60", "61", "62", "63", "67",
				"71", "71", "72", "72", "73",
				"74", "75", "78", "79", "80",
				"80", "82", "89", "89", "89",
				"91", "91", "92", "92", "93",
				"93", "94", "97", "97", "99",
			},
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
			prepareChunksShuffle(ctx, t, filename, outputFilename, 21, false, 10, true, false, false)
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

func Test100ElemsShuffleGzip(t *testing.T) {
	t.Skip("to rework")
	tcs := map[string]struct {
		filename       string
		outputFilename string
		expectedErr    error
		expectedOutput []string
	}{
		"100 elems with headers": {
			filename: "testdata/100elems.tsv.gz",
			expectedOutput: []string{
				"headers", "3", "4", "5", "6", "6",
				"7", "7", "7", "8", "8",
				"9", "9", "10", "10", "15",
				"18", "18", "18", "18", "21",
				"22", "22", "25", "25", "25",
				"25", "25", "26", "26", "27",
				"27", "28", "28", "29", "29",
				"29", "30", "30", "31", "31",
				"33", "33", "34", "36", "37",
				"39", "39", "39", "40", "41",
				"41", "42", "43", "43", "47",
				"47", "49", "50", "50", "52",
				"52", "53", "54", "55", "55",
				"55", "56", "57", "57", "59",
				"60", "61", "62", "63", "67",
				"71", "71", "72", "72", "73",
				"74", "75", "78", "79", "80",
				"80", "82", "89", "89", "89",
				"91", "91", "92", "92", "93",
				"93", "94", "97", "97", "99",
			},
			outputFilename: "testdata/chunks/output.tsv.gz",
		},
	}

	for name, tc := range tcs {
		filename := tc.filename
		outputFilename := tc.outputFilename
		expectedOutput := tc.expectedOutput
		expectedErr := tc.expectedErr
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			prepareChunksShuffle(ctx, t, filename, outputFilename, 21, false, 10, true, false, true)
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
