package writer

import (
	"encoding/csv"
	"io"

	"github.com/pkg/errors"
)

type SeparatedValuesWriter struct {
	w *csv.Writer
}

func NewSeparatedValues(w io.Writer, separator rune) Writer {
	s := &SeparatedValuesWriter{
		w: csv.NewWriter(w),
	}
	s.w.Comma = separator
	return s
}

func (s *SeparatedValuesWriter) Write(elem interface{}) error {
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

func (s *SeparatedValuesWriter) Close() error {
	s.w.Flush()
	if s.w.Error() != nil {
		return errors.Wrap(s.w.Error(), "can't close writer")
	}
	return nil
}
