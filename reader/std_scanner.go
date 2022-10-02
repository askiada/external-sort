package reader

import (
	"bufio"
	"compress/gzip"
	"io"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var logger = logrus.StandardLogger()

type StdScanner struct {
	r  *bufio.Scanner
	gr *gzip.Reader
}

func NewStdScanner(r io.Reader, isGzip bool) (Reader, error) {
	var newR *bufio.Scanner
	s := &StdScanner{}
	if isGzip {
		gr, err := gzip.NewReader(r)
		if err != nil {
			return nil, errors.Wrap(err, "can't create gzip reader")
		}
		s.gr = gr
		newR = bufio.NewScanner(gr)
	} else {
		newR = bufio.NewScanner(r)
	}
	s.r = newR
	logger.Infoln("Created standard scanner")
	return s, nil
}

func (s *StdScanner) Next() bool {
	next := s.r.Scan()
	if !next && s.gr != nil {
		s.gr.Close()
	}
	return next
}
func (s *StdScanner) Read() (interface{}, error) {
	return s.r.Text(), nil
}
func (s *StdScanner) Err() error {
	return s.r.Err()
}

type StdSliceScanner struct {
	r  *bufio.Scanner
	gr *gzip.Reader
}

func NewStdSliceScanner(r io.Reader, isGzip bool) (Reader, error) {
	var newR *bufio.Scanner
	s := &StdSliceScanner{}
	if isGzip {
		gr, err := gzip.NewReader(r)
		if err != nil {
			return nil, errors.Wrap(err, "can't create gzip reader")
		}
		s.gr = gr
		newR = bufio.NewScanner(gr)
	} else {
		newR = bufio.NewScanner(r)
	}
	s.r = newR
	return s, nil
}

func (s *StdSliceScanner) Next() bool {
	next := s.r.Scan()
	if !next && s.gr != nil {
		s.gr.Close()
	}
	return next
}
func (s *StdSliceScanner) Read() (interface{}, error) {
	line := s.r.Text()
	before, after, found := strings.Cut(line, "##!!##")
	if !found {
		return nil, errors.New("can't cut row")
	}
	return []string{before, after}, nil
}
func (s *StdSliceScanner) Err() error {
	return s.r.Err()
}
