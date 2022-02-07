package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/askiada/external-sort/file"
	"github.com/askiada/external-sort/vector"
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
		Reader:        f,
		Allocate:      vector.AllocateStringVector,
		OutputPath:    "output.tsv",
		PrintMemUsage: false,
	}

	// create small files with maximum 30 rows in each
	chunkPaths, err := fI.CreateSortedChunks(context.Background(), "data/chunks", 100000, 100)
	if err != nil {
		panic(err)
	}
	// perform a merge sort on all the chunks files.
	// we sort using a buffer so we don't have to load the entire chunks when merging
	err = fI.MergeSort(chunkPaths, 1)
	if err != nil {
		panic(err)
	}
	elapsed := time.Since(start)
	fmt.Println(elapsed)
}
