package writer

import (
	"bufio"
	"compress/gzip"
	"io"
	"strings"

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

type StdSliceWriter struct {
	skipFirst bool
	w         *bufio.Writer
	gw        *gzip.Writer
}

func NewStdSliceWriter(w io.Writer, skipFirst, isGzip bool) Writer {
	var newR *bufio.Writer
	ssw := &StdSliceWriter{
		skipFirst: skipFirst,
	}
	if isGzip {
		ssw.gw = gzip.NewWriter(w)
		newR = bufio.NewWriter(ssw.gw)
	} else {
		newR = bufio.NewWriter(w)
	}
	ssw.w = newR
	return ssw
}

func (w *StdSliceWriter) Write(elem interface{}) error {
	line, ok := elem.([]string)
	if !ok {
		return errors.Errorf("can't converte interface{} to string: %+v", elem)
	}
	if w.skipFirst {
		line = line[1:]
	}
	_, err := w.w.WriteString(strings.Join(line, "##!!##") + "\n")
	if err != nil {
		return errors.Wrap(err, "can't write string")
	}
	return err
}

func (w *StdSliceWriter) Close() error {
	if w.gw != nil {
		defer w.gw.Close()
	}
	err := w.w.Flush()
	if err != nil {
		return errors.Wrap(err, "can't close writer")
	}
	return nil
}
