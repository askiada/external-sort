package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/askiada/external-sort/file"
	"github.com/askiada/external-sort/vector"
	"github.com/askiada/external-sort/vector/key"
)

func main() {
	start := time.Now()
	/*
		NOT USED. Just to show how to get a io.Reader from a ftp file
			s, err := sftp.NewSFTPClient(host, key, user, pass)
			if err != nil {
				return nil, err
			}
			defer s.Close()
			f, err := s.Client.OpenFile(filename, os.O_RDONLY)
			if err != nil {
				panic(err)
			}
			defer f.Close()
	*/

	// open a file
	f, err := os.Open("/Users/alex/Downloads/works.tsv")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	fI := &file.Info{
		Reader: f,
		Allocate: vector.DefaultVector(func(line string) (key.Key, error) {
			return key.AllocateTsv(line, 0)
		}),
		OutputPath:    "output.tsv",
		PrintMemUsage: false,
	}

	// line 1
	// CreateSortedChunks max memory = 100000*1
	// CreateSortedChunks max memory = 100000*1*maxWorkers

	// create small files with maximum 30 rows in each
	chunkPaths, err := fI.CreateSortedChunks(context.Background(), "data/chunks", 1000000, 4)
	if err != nil {
		panic(err)
	}
	// perform a merge sort on all the chunks files.
	// we sort using a buffer so we don't have to load the entire chunks when merging
	err = fI.MergeSort(chunkPaths, 1000)
	if err != nil {
		panic(err)
	}
	elapsed := time.Since(start)
	fmt.Println(elapsed)
}
