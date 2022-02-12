package key

type String struct {
	value string
}

func AllocateString(line string) (Key, error) {
	return &String{line}, nil
}
func AllocateEmptyString() Key {
	return &String{}
}

func (k *String) Less(other Key) bool {
	return k.value < other.(*String).value
}

func (k *String) String() string {
	return k.value
}

func (k *String) FromString(text string) error {
	k.value = text
	return nil
}
