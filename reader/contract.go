package reader

type Reader interface {
	Next() bool
	Read() (interface{}, error)
	Err() error
}
