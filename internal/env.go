package internal

// this file contains the settings for environment variables.

import (
	"github.com/spf13/viper"
)

// Argument names.
const (
	InputFileName        = "input_path"
	OutputFileName       = "output_path"
	ChunkFolderName      = "chunk_folder"
	ChunkSizeName        = "chunk_size"
	MaxWorkersName       = "max_workers"
	OutputBufferSizeName = "output_buffer_size"
	TsvFieldsName        = "tsv_fields"
)

// Environment variables.
var (
	InputFile        string
	TsvFields        []string
	OutputFile       string
	ChunkFolder      string
	ChunkSize        int
	MaxWorkers       int64
	OutputBufferSize int
)

func init() {
	viper.AutomaticEnv()
	viper.SetDefault(InputFileName, "")
	viper.SetDefault(OutputFileName, "")
	viper.SetDefault(ChunkFolderName, "")
	viper.SetDefault(ChunkSizeName, 0)
	viper.SetDefault(MaxWorkersName, 0)
	viper.SetDefault(OutputBufferSizeName, 0)
	viper.SetDefault(TsvFieldsName, []string{"0"})
}
