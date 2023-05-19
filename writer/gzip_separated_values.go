package writer

import (
	"compress/gzip"
	"encoding/csv"
	"io"

	"github.com/pkg/errors"
)

type GZipSeparatedValuesWriter struct {
	w  *csv.Writer
	gw *gzip.Writer
}

func NewGZipSeparatedValues(w io.Writer, separator rune) (Writer, error) {
	gw := gzip.NewWriter(w)
	s := &GZipSeparatedValuesWriter{
		gw: gw,
		w:  csv.NewWriter(gw),
	}
	s.w.Comma = separator
	return s, nil
}

func (s *GZipSeparatedValuesWriter) Write(elem interface{}) error {
	line, ok := elem.([]string)
	if !ok {
		return errors.Errorf("can't converte interface{} to []string: %+v", elem)
	}
	err := s.w.Write(line)
	if err != nil {
		return errors.Wrap(err, "can't write line")
	}
	return nil
}

func (s *GZipSeparatedValuesWriter) Close() (err error) {
	defer func() { err = s.gw.Close() }()
	s.w.Flush()
	if s.w.Error() != nil {
		return errors.Wrap(s.w.Error(), "can't close writer")
	}
	return err
}
