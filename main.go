package main

import (
	"context"
	"fmt"
	"os"

	"github.com/askiada/external-sort/file"
	"github.com/askiada/external-sort/vector"
)

func main() {
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
	f, err := os.Open("data/10elems.tsv")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	fI := &file.Info{
		Reader:   f,
		Allocate: vector.AllocateIntVector,
	}

	// create small files with maximum 30 rows in each
	chunkPaths, err := fI.CreateSortedChunks(context.Background(), "data/chunks", 4, 2)
	if err != nil {
		panic(err)
	}
	// perform a merge sort on all the chunks files.
	// we sort using a buffer so we don't have to load the entire chunks when merging
	output, err := fI.MergeSort(chunkPaths, 3)
	if err != nil {
		panic(err)
	}
	// this output could be saved on hard drive
	// or we can imagine send events everytime an element is added to it
	// of course it will require MergeSort to return a channel
	fmt.Println(output)
}
