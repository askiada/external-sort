package reader

import (
	"io"
)

type Reader interface {
	Next() bool
	Read() (interface{}, error)
	Err() error
}
type Config func(r io.Reader) (Reader, error)
