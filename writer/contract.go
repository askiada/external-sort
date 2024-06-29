package writer

import "io"

type Writer interface {
	Write(row interface{}) (err error)
	Close() (err error)
}

type Config func(w io.Writer) (Writer, error)
