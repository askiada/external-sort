package writer

import (
	"bufio"
	"io"

	"github.com/pkg/errors"
)

type StdWriter struct {
	w *bufio.Writer
}

func NewStdWriter(w io.Writer) Writer {
	s := &StdWriter{
		w: bufio.NewWriter(w),
	}
	return s
}

func (w *StdWriter) Write(elem interface{}) error {
	line, ok := elem.(string)
	if !ok {
		return errors.Errorf("can't converte interface{} to string: %+v", elem)
	}
	_, err := w.w.WriteString(line + "\n")
	if err != nil {
		return errors.Wrap(err, "can't write string")
	}
	return err
}

func (w *StdWriter) Close() error {
	err := w.w.Flush()
	if err != nil {
		return errors.Wrap(err, "can't close writer")
	}
	return nil
}
