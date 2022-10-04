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

type command struct {
	rootCmd    *cobra.Command
	sortCmd    *cobra.Command
	shuffleCmd *cobra.Command
}

func newCommand() *command {
	root := &command{
		rootCmd: &cobra.Command{
			Use:   "external",
			Short: "Perform an external task on an input file",
		},
		sortCmd: &cobra.Command{
			Use:   "sort",
			Short: "Perform an external sorting on an input file",
			RunE:  sortRun,
		},
		shuffleCmd: &cobra.Command{
			Use:   "shuffle",
			Short: "Perform an external sorting on an input file",
			RunE:  shuffleRun,
		},
	}
	root.rootCmd.PersistentFlags().BoolVarP(&internal.WithHeader, internal.WithHeaderName, "e", viper.GetBool(internal.WithHeaderName), "Input file has headers.")
	root.rootCmd.PersistentFlags().StringSliceVarP(&internal.InputFiles, internal.InputFileNames, "i", viper.GetStringSlice(internal.InputFileNames), "input file path.")
	root.rootCmd.PersistentFlags().StringVarP(&internal.OutputFile, internal.OutputFileName, "o", viper.GetString(internal.OutputFileName), "output file path.")
	root.rootCmd.PersistentFlags().StringVarP(&internal.ChunkFolder, internal.ChunkFolderName, "c", viper.GetString(internal.ChunkFolderName), "chunk folder.")

	root.rootCmd.PersistentFlags().IntVarP(&internal.ChunkSize, internal.ChunkSizeName, "s", viper.GetInt(internal.ChunkSizeName), "chunk size.")
	root.rootCmd.PersistentFlags().Int64VarP(&internal.MaxWorkers, internal.MaxWorkersName, "w", viper.GetInt64(internal.MaxWorkersName), "max worker.")
	root.rootCmd.PersistentFlags().IntVarP(&internal.OutputBufferSize, internal.OutputBufferSizeName, "b", viper.GetInt(internal.OutputBufferSizeName), "output buffer size.")
	root.sortCmd.PersistentFlags().StringSliceVarP(&internal.TsvFields, internal.TsvFieldsName, "t", viper.GetStringSlice(internal.TsvFieldsName), "")

	root.rootCmd.Flags().StringVar(&internal.S3Region, internal.S3RegionName, viper.GetString(internal.S3RegionName), "the bucket region")
	root.rootCmd.Flags().IntVar(&internal.S3RetryMaxAttempts, internal.S3RetryMaxAttemptsName, viper.GetInt(internal.S3RetryMaxAttemptsName), "the number of retries per S3 request before failing")

	root.shuffleCmd.PersistentFlags().BoolVarP(&internal.IsGzip, internal.IsGzipName, "t", viper.GetBool(internal.IsGzipName), "")

	root.rootCmd.AddCommand(root.sortCmd, root.shuffleCmd)
	return root
}

func main() {
	root := newCommand()
	cobra.CheckErr(root.rootCmd.Execute())
}

func sortRun(cmd *cobra.Command, args []string) error {
	logger.Infoln("Input files", internal.InputFiles)
	logger.Infoln("With header", internal.WithHeader)
	logger.Infoln("Output file", internal.OutputFile)
	logger.Infoln("Chunk folder", internal.ChunkFolder)
	logger.Infoln("TSV Fields", internal.TsvFields)

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

func shuffleRun(cmd *cobra.Command, args []string) error {
	logger.Infoln("Input files", internal.InputFiles)
	logger.Infoln("With header", internal.WithHeader)
	logger.Infoln("Output file", internal.OutputFile)
	logger.Infoln("Chunk folder", internal.ChunkFolder)
	logger.Infoln("GZip file", internal.IsGzip)
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

	fI := &file.Info{
		WithHeader:    internal.WithHeader,
		InputReader:   i.Input,
		OutputFile:    i.Output,
		PrintMemUsage: false,
	}
	i.Do(func() error {
		// create small files with maximum 30 rows in each
		_, err := fI.Shuffle(context.Background(), internal.ChunkFolder, internal.ChunkSize, internal.MaxWorkers, internal.OutputBufferSize, time.Now().Unix(), internal.IsGzip)
		if err != nil {
			return errors.Wrap(err, "can't create shuflled chunks")
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
