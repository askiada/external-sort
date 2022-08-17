package reader

import (
	"bufio"
	"io"
)

type StdScanner struct {
	r *bufio.Scanner
}

func NewStdScanner(r io.Reader) Reader {
	s := &StdScanner{
		r: bufio.NewScanner(r),
	}
	return s
}

func (s *StdScanner) Next() bool {
	return s.r.Scan()
}
func (s *StdScanner) Read() (interface{}, error) {
	return s.r.Text(), nil
}
func (s *StdScanner) Err() error {
	return s.r.Err()
}
