package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/askiada/external-sort/file"
	"github.com/askiada/external-sort/internal"
	"github.com/askiada/external-sort/vector"
	"github.com/askiada/external-sort/vector/key"
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

	fmt.Println("Input file", internal.InputFile)
	fmt.Println("Output file", internal.OutputFile)
	fmt.Println("Chunk foler", internal.ChunkFolder)
	cobra.CheckErr(rootCmd.Execute())
}

func rootRun(cmd *cobra.Command, args []string) error {
	start := time.Now()
	inputPath := internal.InputFile
	// open a file
	f, err := os.Open(inputPath)
	if err != nil {
		return err
	}
	defer f.Close()
	fI := &file.Info{
		Reader: f,
		Allocate: vector.DefaultVector(func(line string) (key.Key, error) {
			return key.AllocateTsv(line, 0)
		}),
		OutputPath:    internal.OutputFile,
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
