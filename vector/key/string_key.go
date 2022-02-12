package key

type String struct {
	value string
}

func AllocateString(line string) (Key, error) {
	return &String{line}, nil
}

func (k *String) Get() interface{} {
	return k.value
}
func (k *String) Less(other Key) bool {
	return k.value < other.(*String).value
}
