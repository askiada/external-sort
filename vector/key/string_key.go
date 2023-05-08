package key

import "strings"

type String struct {
	value string
}

func AllocateString(line string) (Key, error) {
	return &String{line}, nil
}

func (k *String) Less(other Key) bool {
	return k.value < other.(*String).value
}

func (k *String) Equal(other Key) bool {
	return k.value == other.(*String).value
}

type UpperString struct {
	value string
}

func AllocateUpperString(line string) (Key, error) {
	return &UpperString{strings.TrimSpace(strings.ToUpper(line))}, nil
}

func (k *UpperString) Less(other Key) bool {
	return k.value < other.(*UpperString).value
}

func (k *UpperString) Equal(other Key) bool {
	return k.value == other.(*UpperString).value
}
