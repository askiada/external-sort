## external-sort

This repo describes an algorithm to sort data from a stream with a fixed amount of memory necessary that can be specified.

The overall idea is:

-   Get a io.Reader using sftp.OpenFile()
-   Scan the entire file and dump to a temporary file every M rows and M must fit in memory. The M rows are sorted using binary search

-   You now have P small files on Hard drive. Each file is sorted and contains maximum M rows
    We know choose K = M/100
-   Load the first K rows of every chunk in memory. This is a buffer
-   It means P slices are in memory (memory used is still M)
-   Perform a merge sort on the P slices. There are comments in the function MergeSort to describe how it is done.

If one of the slice becomes empty when merge sorting
then load the next K rows from the chunk file associated to the slice and carry on (very important to order correctly the final file)

If I’m correct the maximum RAM used is M + size of output buffer

The maximum hard drives used is the size P\*M (size of the file) as long as you don't store the final output on drive.

Currently we hold the final sorted file in memory and we should just return a channel that can be consumed by another application.

There are many parts we could parallelise to improve the speed a lot.

## Why are you using vector?

Mmh because this the way I imagined a solutionto the problem in the first place. But I don't think the name is still so accurate.

On the other hand, it brings a "nice" interface that allow to create models and decide how to compare rows depending on the shape of the file you want to sort.

For example, all the tests were done using IntVector. It reads a line form the file and convert it to integer so we can compare numbers.

## Test

You can look at an intersting file `testdata/100elems.tsv`. It contains 100 rows with one integer per row. And the test succesfully order it for any size of chunks or buffer.

```sh
make test
```

## Show some stuff

```sh
make run
```

Print on stdout how we ordered 10 integers. The original file can be find `data/10elems.tsv`
