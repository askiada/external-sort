package file

import (
	"bufio"
	"os"

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

type chunks struct {
	list []*chunkInfo
}

// new Create a new chunk and initialize it.
func (c *chunks) new(chunkPath string, allocate func() vector.Vector) error {
	f, err := os.Open(chunkPath)
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(f)
	c.list = append(c.list, &chunkInfo{
		filename: chunkPath,
		file:     f,
		scanner:  scanner,
		buffer:   allocate(),
	})
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

// get Access the i-th chunk and returns it.
func (c *chunks) get(i int) *chunkInfo {
	return c.list[i]
}

// shrink Remove all the chunks at the specified indexes
// it removes the local file created and close the file descriptor.
func (c *chunks) shrink(toShrink []int) error {
	for _, shrinkIndex := range toShrink {
		err := c.list[shrinkIndex].file.Close()
		if err != nil {
			return err
		}
		err = os.Remove(c.list[shrinkIndex].filename)
		if err != nil {
			return err
		}
		c.list[shrinkIndex] = c.list[len(c.list)-1]
		c.list = c.list[:len(c.list)-1]
	}
	return nil
}

func (c *chunks) len() int {
	return len(c.list)
}

// min Check all the first elements of all the chunks and returns the smallest value.
func (c chunks) min() (minChunk *chunkInfo, minValue interface{}) {
	for i, chunk := range c.list {
		currValue := chunk.buffer.Get(0)
		if i == 0 {
			minChunk = chunk
			minValue = currValue
			continue
		}
		if chunk.buffer.Less(currValue, minValue) {
			minChunk = chunk
			minValue = currValue
		}
	}
	return minChunk, minValue
}
