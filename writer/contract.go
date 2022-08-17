package writer

type Writer interface {
	Write(interface{}) error
	Close() error
}
