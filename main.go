package main

import (
	"context"
	"io"
	"strconv"
	"time"

	"github.com/askiada/external-sort/file"
	"github.com/askiada/external-sort/internal"
	"github.com/askiada/external-sort/internal/rw"
	"github.com/askiada/external-sort/reader"
	"github.com/askiada/external-sort/vector"
	"github.com/askiada/external-sort/vector/key"
	"github.com/askiada/external-sort/writer"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var logger = logrus.StandardLogger()

func main() {
	rootCmd := &cobra.Command{
		Use:   "external-sort",
		Short: "Perform an external sorting on an input file",
		RunE:  rootRun,
	}
	rootCmd.PersistentFlags().BoolVarP(&internal.WithHeader, internal.WithHeaderName, "i", viper.GetBool(internal.WithHeaderName), "Input file has headers.")
	rootCmd.PersistentFlags().StringSliceVarP(&internal.InputFiles, internal.InputFileNames, "i", viper.GetStringSlice(internal.InputFileNames), "input file path.")
	rootCmd.PersistentFlags().StringVarP(&internal.OutputFile, internal.OutputFileName, "o", viper.GetString(internal.OutputFileName), "output file path.")
	rootCmd.PersistentFlags().StringVarP(&internal.ChunkFolder, internal.ChunkFolderName, "c", viper.GetString(internal.ChunkFolderName), "chunk folder.")

	rootCmd.PersistentFlags().IntVarP(&internal.ChunkSize, internal.ChunkSizeName, "s", viper.GetInt(internal.ChunkSizeName), "chunk size.")
	rootCmd.PersistentFlags().Int64VarP(&internal.MaxWorkers, internal.MaxWorkersName, "w", viper.GetInt64(internal.MaxWorkersName), "max worker.")
	rootCmd.PersistentFlags().IntVarP(&internal.OutputBufferSize, internal.OutputBufferSizeName, "b", viper.GetInt(internal.OutputBufferSizeName), "output buffer size.")
	rootCmd.PersistentFlags().StringSliceVarP(&internal.TsvFields, internal.TsvFieldsName, "t", viper.GetStringSlice(internal.TsvFieldsName), "")

	rootCmd.Flags().StringVar(&internal.S3Region, internal.S3RegionName, viper.GetString(internal.S3RegionName), "the bucket region")
	rootCmd.Flags().IntVar(&internal.S3RetryMaxAttempts, internal.S3RetryMaxAttemptsName, viper.GetInt(internal.S3RetryMaxAttemptsName), "the number of retries per S3 request before failing")

	logger.Infoln("Input files", internal.InputFiles)
	logger.Infoln("With header", internal.WithHeader)
	logger.Infoln("Output file", internal.OutputFile)
	logger.Infoln("Chunk folder", internal.ChunkFolder)
	logger.Infoln("TSV Fields", internal.TsvFields)
	cobra.CheckErr(rootCmd.Execute())
}

func rootRun(cmd *cobra.Command, args []string) error {
	start := time.Now()
	ctx := context.Background()
	i := rw.NewInputOutput(ctx)
	err := i.SetInputReader(ctx, internal.InputFiles...)
	if err != nil {
		return err
	}
	err = i.SetOutputWriter(ctx, internal.OutputFile)
	if err != nil {
		return err
	}
	tsvFields := []int{}
	for _, field := range internal.TsvFields {
		i, err := strconv.Atoi(field)
		if err != nil {
			return err
		}
		tsvFields = append(tsvFields, i)
	}
	fI := &file.Info{
		WithHeader:  internal.WithHeader,
		InputReader: i.Input,
		OutputFile:  i.Output,
		Allocate: vector.DefaultVector(
			func(row interface{}) (key.Key, error) {
				return key.AllocateTsv(row, tsvFields...)
			},
			func(r io.Reader) (reader.Reader, error) { return reader.NewGZipSeparatedValues(r, '\t') }, func(w io.Writer) (writer.Writer, error) {
				return writer.NewGZipSeparatedValues(w, '\t')
			},
		),
		PrintMemUsage: false,
	}
	i.Do(func() error {
		// create small files with maximum 30 rows in each
		chunkPaths, err := fI.CreateSortedChunks(context.Background(), internal.ChunkFolder, internal.ChunkSize, internal.MaxWorkers)
		if err != nil {
			return errors.Wrap(err, "can't create sorted chunks")
		}
		// perform a merge sort on all the chunks files.
		// we sort using a buffer so we don't have to load the entire chunks when merging
		err = fI.MergeSort(chunkPaths, internal.OutputBufferSize, true)
		if err != nil {
			return errors.Wrap(err, "can't merge sort")
		}
		elapsed := time.Since(start)
		logger.Infoln("It took", elapsed)
		return nil
	})
	err = i.Err()
	if err != nil {
		return errors.Wrap(err, "can't finish")
	}
	return nil
}
