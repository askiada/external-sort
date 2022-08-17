package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/askiada/external-sort/file"
	"github.com/askiada/external-sort/internal"
	"github.com/askiada/external-sort/reader"
	"github.com/askiada/external-sort/vector"
	"github.com/askiada/external-sort/vector/key"
	"github.com/askiada/external-sort/writer"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "external-sort",
		Short: "Perform an external sorting on an input file",
		RunE:  rootRun,
	}

	rootCmd.PersistentFlags().StringVarP(&internal.InputFile, internal.InputFileName, "i", viper.GetString(internal.InputFileName), "input file path.")
	rootCmd.PersistentFlags().StringVarP(&internal.OutputFile, internal.OutputFileName, "o", viper.GetString(internal.OutputFileName), "output file path.")
	rootCmd.PersistentFlags().StringVarP(&internal.ChunkFolder, internal.ChunkFolderName, "c", viper.GetString(internal.ChunkFolderName), "chunk folder.")

	rootCmd.PersistentFlags().IntVarP(&internal.ChunkSize, internal.ChunkSizeName, "s", viper.GetInt(internal.ChunkSizeName), "chunk size.")
	rootCmd.PersistentFlags().Int64VarP(&internal.MaxWorkers, internal.MaxWorkersName, "w", viper.GetInt64(internal.MaxWorkersName), "max worker.")
	rootCmd.PersistentFlags().IntVarP(&internal.OutputBufferSize, internal.OutputBufferSizeName, "b", viper.GetInt(internal.OutputBufferSizeName), "output buffer size.")
	rootCmd.PersistentFlags().StringSliceVarP(&internal.TsvFields, internal.TsvFieldsName, "t", viper.GetStringSlice(internal.TsvFieldsName), "")

	fmt.Println("Input file", internal.InputFile)
	fmt.Println("Output file", internal.OutputFile)
	fmt.Println("Chunk folder", internal.ChunkFolder)
	fmt.Println("TSV Fields", internal.TsvFields)

	cobra.CheckErr(rootCmd.Execute())
}

func rootRun(cmd *cobra.Command, args []string) error {
	start := time.Now()
	// open a file
	inputReader, err := os.Open(internal.InputFile)
	if err != nil {
		return err
	}
	defer inputReader.Close()
	tsvFields := []int{}
	for _, field := range internal.TsvFields {
		i, err := strconv.Atoi(field)
		if err != nil {
			return err
		}
		tsvFields = append(tsvFields, i)
	}
	fI := &file.Info{
		InputReader: inputReader,
		OutputFile:  internal.OutputFile,
		Allocate: vector.DefaultVector(func(row interface{}) (key.Key, error) {
			return key.AllocateTsv(row, tsvFields...)
		}, func(r io.Reader) reader.Reader { return reader.NewSeparatedValues(r, '\t') }, func(w io.Writer) writer.Writer { return writer.NewSeparatedValues(w, '\t') }),
		PrintMemUsage: false,
	}

	// create small files with maximum 30 rows in each
	chunkPaths, err := fI.CreateSortedChunks(context.Background(), internal.ChunkFolder, internal.ChunkSize, internal.MaxWorkers)
	if err != nil {
		return err
	}
	// perform a merge sort on all the chunks files.
	// we sort using a buffer so we don't have to load the entire chunks when merging
	err = fI.MergeSort(chunkPaths, internal.OutputBufferSize)
	if err != nil {
		return err
	}
	elapsed := time.Since(start)
	fmt.Println(elapsed)
	return nil
}
