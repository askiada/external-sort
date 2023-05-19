package internal

// this file contains the settings for environment variables.

import (
	"github.com/spf13/viper"
)

// Argument names.
const (
	WithHeaderName       = "with_header"
	InputFileNames       = "input_paths"
	OutputFileName       = "output_path"
	ChunkFolderName      = "chunk_folder"
	ChunkSizeName        = "chunk_size"
	MaxWorkersName       = "max_workers"
	OutputBufferSizeName = "output_buffer_size"
	TsvFieldsName        = "tsv_fields"

	S3RegionName           = "s3_region"
	S3RetryMaxAttemptsName = "s3_retry_max_attempts"

	IsGzipName = "is_gzip"
)

// Environment variables.
var (
	WithHeader       bool
	InputFiles       []string
	TsvFields        []string
	OutputFile       string
	ChunkFolder      string
	ChunkSize        int
	MaxWorkers       int
	OutputBufferSize int

	S3Region           string
	S3RetryMaxAttempts int
	IsGzip             bool
)

func init() {
	viper.AutomaticEnv()
	viper.SetDefault(WithHeaderName, false)
	viper.SetDefault(InputFileNames, "")
	viper.SetDefault(OutputFileName, "")
	viper.SetDefault(ChunkFolderName, "")
	viper.SetDefault(ChunkSizeName, 0)
	viper.SetDefault(MaxWorkersName, 0)
	viper.SetDefault(OutputBufferSizeName, 0)
	viper.SetDefault(TsvFieldsName, []string{"0"})

	viper.SetDefault(S3RegionName, "eu-west-1")
	viper.SetDefault(S3RetryMaxAttemptsName, 10) //nolint //gomnd
	viper.SetDefault(IsGzipName, false)
}
