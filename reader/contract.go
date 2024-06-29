package reader

import (
	"io"
)

// Reader define a basic reader.
type Reader interface {
	Next() bool
	Read() (interface{}, error)
	Err() error
}

// Config function type to convert a io.Reader to a Reader.
type Config func(r io.Reader) (Reader, error)
