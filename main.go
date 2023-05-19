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

func setFlags(root *command) {
	root.rootCmd.PersistentFlags().BoolVarP(
		&internal.WithHeader,
		internal.WithHeaderName,
		"e",
		viper.GetBool(internal.WithHeaderName),
		"Input file has headers.",
	)
	root.rootCmd.PersistentFlags().StringSliceVarP(
		&internal.InputFiles,
		internal.InputFileNames,
		"i",
		viper.GetStringSlice(internal.InputFileNames),
		"input file path.",
	)
	root.rootCmd.PersistentFlags().StringVarP(
		&internal.OutputFile,
		internal.OutputFileName,
		"o",
		viper.GetString(internal.OutputFileName),
		"output file path.",
	)
	root.rootCmd.PersistentFlags().StringVarP(
		&internal.ChunkFolder,
		internal.ChunkFolderName,
		"c",
		viper.GetString(internal.ChunkFolderName),
		"chunk folder.",
	)

	root.rootCmd.PersistentFlags().IntVarP(
		&internal.ChunkSize,
		internal.ChunkSizeName,
		"s",
		viper.GetInt(internal.ChunkSizeName),
		"chunk size.",
	)
	root.rootCmd.PersistentFlags().IntVarP(
		&internal.MaxWorkers,
		internal.MaxWorkersName,
		"w",
		viper.GetInt(internal.MaxWorkersName),
		"max worker.",
	)
	root.rootCmd.PersistentFlags().IntVarP(
		&internal.OutputBufferSize,
		internal.OutputBufferSizeName,
		"b",
		viper.GetInt(internal.OutputBufferSizeName),
		"output buffer size.",
	)
	root.sortCmd.PersistentFlags().StringSliceVarP(
		&internal.TsvFields,
		internal.TsvFieldsName,
		"t",
		viper.GetStringSlice(internal.TsvFieldsName),
		"",
	)

	root.rootCmd.Flags().StringVar(
		&internal.S3Region,
		internal.S3RegionName,
		viper.GetString(internal.S3RegionName),
		"the bucket region",
	)
	root.rootCmd.Flags().IntVar(
		&internal.S3RetryMaxAttempts,
		internal.S3RetryMaxAttemptsName,
		viper.GetInt(internal.S3RetryMaxAttemptsName),
		"the number of retries per S3 request before failing",
	)

	root.shuffleCmd.PersistentFlags().BoolVarP(&internal.IsGzip,
		internal.IsGzipName,
		"t",
		viper.GetBool(internal.IsGzipName),
		"",
	)
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
			PreRun: func(cmd *cobra.Command, args []string) {
				cmd.SetContext(cmd.Parent().Context())
			},
			RunE: sortRun,
		},
		shuffleCmd: &cobra.Command{
			Use:   "shuffle",
			Short: "Perform an external shufflin on an input file",
			PreRun: func(cmd *cobra.Command, args []string) {
				cmd.SetContext(cmd.Parent().Context())
			},
			RunE: shuffleRun,
		},
	}
	root.rootCmd.AddCommand(root.sortCmd, root.shuffleCmd)
	return root
}

func main() {
	root := newCommand()
	setFlags(root)
	ctx := context.Background()
	cobra.CheckErr(root.rootCmd.ExecuteContext(ctx))
}

func sortRun(cmd *cobra.Command, _ []string) error {
	logger.Infoln("Input files", internal.InputFiles)
	logger.Infoln("With header", internal.WithHeader)
	logger.Infoln("Output file", internal.OutputFile)
	logger.Infoln("Chunk folder", internal.ChunkFolder)
	logger.Infoln("TSV Fields", internal.TsvFields)

	start := time.Now()
	inputOutput := rw.NewInputOutput(cmd.Context())
	err := inputOutput.SetInputReader(cmd.Context(), internal.InputFiles...)
	if err != nil {
		return errors.Wrap(err, "can't set input reader")
	}
	err = inputOutput.SetOutputWriter(cmd.Context(), internal.OutputFile)
	if err != nil {
		return errors.Wrap(err, "can't set output writer")
	}
	tsvFields := []int{}
	for _, field := range internal.TsvFields {
		i, err := strconv.Atoi(field)
		if err != nil {
			return errors.Wrapf(err, "can't convert field %s", field)
		}
		tsvFields = append(tsvFields, i)
	}
	fileInfo := &file.Info{
		WithHeader:  internal.WithHeader,
		InputReader: inputOutput.Input,
		OutputFile:  inputOutput.Output,
		Allocate: vector.DefaultVector(
			func(row interface{}) (key.Key, error) {
				k, err := key.AllocateTsv(row, tsvFields...)
				if err != nil {
					return nil, errors.Wrapf(err, "can't allocate tsv %+v", row)
				}
				return k, nil
			},
			func(r io.Reader) (reader.Reader, error) {
				gzipReader, err := reader.NewGZipSeparatedValues(r, '\t')
				if err != nil {
					return nil, errors.Wrap(err, "can't create Gzip reader")
				}
				return gzipReader, nil
			},
			func(w io.Writer) (writer.Writer, error) {
				gzipWriter, err := writer.NewGZipSeparatedValues(w, '\t')
				if err != nil {
					return nil, errors.Wrap(err, "can't create Gzip writer")
				}
				return gzipWriter, nil
			},
		),
		PrintMemUsage: false,
	}
	inputOutput.Do(func() error {
		// create small files with maximum 30 rows in each
		chunkPaths, err := fileInfo.CreateSortedChunks(cmd.Context(), internal.ChunkFolder, internal.ChunkSize, internal.MaxWorkers)
		if err != nil {
			return errors.Wrap(err, "can't create sorted chunks")
		}
		// perform a merge sort on all the chunks files.
		// we sort using a buffer so we don't have to load the entire chunks when merging
		err = fileInfo.MergeSort(chunkPaths, internal.OutputBufferSize, true)
		if err != nil {
			return errors.Wrap(err, "can't merge sort")
		}
		elapsed := time.Since(start)
		logger.Infoln("It took", elapsed)
		return nil
	})
	err = inputOutput.Err()
	if err != nil {
		return errors.Wrap(err, "can't finish")
	}
	return nil
}

func shuffleRun(cmd *cobra.Command, _ []string) error {
	logger.Infoln("Input files", internal.InputFiles)
	logger.Infoln("With header", internal.WithHeader)
	logger.Infoln("Output file", internal.OutputFile)
	logger.Infoln("Chunk folder", internal.ChunkFolder)
	logger.Infoln("GZip file", internal.IsGzip)
	start := time.Now()
	inputOutput := rw.NewInputOutput(cmd.Context())
	err := inputOutput.SetInputReader(cmd.Context(), internal.InputFiles...)
	if err != nil {
		return errors.Wrap(err, "can't set input reader")
	}
	err = inputOutput.SetOutputWriter(cmd.Context(), internal.OutputFile)
	if err != nil {
		return errors.Wrap(err, "can't set output writer")
	}

	fileInfo := &file.Info{
		WithHeader:    internal.WithHeader,
		InputReader:   inputOutput.Input,
		OutputFile:    inputOutput.Output,
		PrintMemUsage: false,
	}
	inputOutput.Do(func() error {
		// create small files with maximum 30 rows in each
		_, err := fileInfo.Shuffle(
			cmd.Context(),
			internal.ChunkFolder,
			internal.ChunkSize,
			internal.MaxWorkers,
			internal.OutputBufferSize,
			time.Now().Unix(),
			internal.IsGzip,
		)
		if err != nil {
			return errors.Wrap(err, "can't create shuflled chunks")
		}
		elapsed := time.Since(start)
		logger.Infoln("It took", elapsed)
		return nil
	})
	err = inputOutput.Err()
	if err != nil {
		return errors.Wrap(err, "can't finish")
	}
	return nil
}
