package writer

import "io"

type Writer interface {
	Write(interface{}) error
	Close() error
}

type Config func(w io.Writer) (Writer, error)
