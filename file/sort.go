package file

import (
	"github.com/askiada/external-sort/vector"
)

func MergeSort(chunkPaths []string, k int, allocate func() vector.Vector) (output []interface{}, err error) {
	// create a chunk per file path
	chunks := &chunks{list: make([]*chunkInfo, 0, len(chunkPaths))}
	for _, chunkPath := range chunkPaths {
		err := chunks.new(chunkPath, allocate)
		if err != nil {
			return nil, err
		}
	}
	for {
		toShrink := []int{}
		for i := 0; i < chunks.len(); i++ {
			chunk := chunks.get(i)
			// when a chunk buffer is empty check if we can pull more elements
			if chunk.buffer.End() == 0 {
				err = chunk.pullSubset(k)
				if err != nil {
					return nil, err
				}
				// if after pulling data the chunk buffer is still empty then we can remove it
				if chunk.buffer.End() == 0 {
					toShrink = append(toShrink, i)
				}
			}
		}
		// remove all chunks with no more data available
		err = chunks.shrink(toShrink)
		if err != nil {
			return nil, err
		}
		// we first need to shrink the eventual last chunk before this condition
		if chunks.len() == 0 {
			break
		}
		// search the smallest value across chunk buffers by comparing first elements only
		minChunk, minValue := chunks.min()
		output = append(output, minValue)
		// remove the first element from the chunk we pulled the smallest value
		minChunk.buffer.FrontShift()
	}
	return output, chunks.close()
}
