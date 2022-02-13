package file

import (
	"bufio"
	"os"
	"sort"

	"github.com/askiada/external-sort/vector"

	"github.com/pkg/errors"
)

// chunkInfo Describe a chunk.
type chunkInfo struct {
	file     *os.File
	scanner  *bufio.Scanner
	buffer   vector.Vector
	filename string
}

// pullSubset Add to vector the specified number of elements.
// It stops if there is no elements left to add.
func (c *chunkInfo) pullSubset(size int) (err error) {
	i := 0
	for i < size && c.scanner.Scan() {
		text := c.scanner.Text()
		c.buffer.PushBack(text)
		i++
	}
	if c.scanner.Err() != nil {
		return c.scanner.Err()
	}
	return nil
}

// chunks Pull of chunks.
type chunks struct {
	list []*chunkInfo
}

// new Create a new chunk and initialize it.
func (c *chunks) new(chunkPath string, allocate *vector.Allocate, size int) error {
	f, err := os.Open(chunkPath)
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(f)
	elem := &chunkInfo{
		filename: chunkPath,
		file:     f,
		scanner:  scanner,
		buffer:   allocate.Vector(size, allocate.Key),
	}
	err = elem.pullSubset(size)
	if err != nil {
		return err
	}
	c.list = append(c.list, elem)
	return nil
}

// close Close the file descriptors of all the chunks.
func (c *chunks) close() error {
	for _, chunk := range c.list {
		err := chunk.file.Close()
		if err != nil {
			return errors.Wrap(err, "close")
		}
	}
	return nil
}

// shrink Remove all the chunks at the specified indexes
// it removes the local file created and close the file descriptor.
func (c *chunks) shrink(toShrink []int) error {
	for i, shrinkIndex := range toShrink {
		shrinkIndex -= i
		err := c.list[shrinkIndex].file.Close()
		if err != nil {
			return err
		}
		err = os.Remove(c.list[shrinkIndex].filename)
		if err != nil {
			return err
		}
		// we want to preserve order
		c.list = append(c.list[:shrinkIndex], c.list[shrinkIndex+1:]...)
	}
	return nil
}

// len total number of chunks.
func (c *chunks) len() int {
	return len(c.list)
}

// resetOrder Put all the chunks in ascending order
// Compare the first element of each chunk.
func (c *chunks) resetOrder() {
	if len(c.list) > 1 {
		sort.Slice(c.list, func(i, j int) bool {
			return vector.Less(c.list[i].buffer.Get(0), c.list[j].buffer.Get(0))
		})
	}
}

// moveFirstChunkToCorrectIndex Check where the first chunk should using the first value in the buffer.
func (c *chunks) moveFirstChunkToCorrectIndex() {
	elem := c.list[0]
	c.list = c.list[1:]
	pos := sort.Search(len(c.list), func(i int) bool {
		return !vector.Less(c.list[i].buffer.Get(0), elem.buffer.Get(0))
	})
	// TODO: c.list = c.list[1:] and the following line create an unecessary allocation.
	c.list = append(c.list[:pos], append([]*chunkInfo{elem}, c.list[pos:]...)...)
}

// min Check all the first elements of all the chunks and returns the smallest value.
func (c *chunks) min() (minChunk *chunkInfo, minValue *vector.Element, minIdx int) {
	minValue = c.list[0].buffer.Get(0)
	minIdx = 0
	minChunk = c.list[0]
	return minChunk, minValue, minIdx
}
