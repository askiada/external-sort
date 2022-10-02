package reader

import (
	"compress/gzip"
	"encoding/csv"
	"io"

	"github.com/pkg/errors"
)

type GZipSeparatedValuesReader struct {
	row []string
	r   *csv.Reader
	gr  *gzip.Reader
	err error
}

func NewGZipSeparatedValues(r io.Reader, separator rune) (*GZipSeparatedValuesReader, error) {
	gr, err := gzip.NewReader(r)
	if err != nil {
		return nil, errors.Wrap(err, "can't create gzip reader")
	}

	s := &GZipSeparatedValuesReader{
		gr: gr,
		r:  csv.NewReader(gr),
	}
	s.r.Comma = separator
	return s, nil
}

func (s *GZipSeparatedValuesReader) Next() bool {
	s.row, s.err = s.r.Read()
	if errors.Is(s.err, io.EOF) {
		s.err = nil
		s.gr.Close()
		return false
	}
	return true
}

func (s *GZipSeparatedValuesReader) Read() (interface{}, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.row, nil
}

func (s *GZipSeparatedValuesReader) Err() error {
	return s.err
}

var _ Reader = &GZipSeparatedValuesReader{}
