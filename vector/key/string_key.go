package key

import "strings"

// String define an string key.
type String struct {
	value string
}

// AllocateString create a new string key.
func AllocateString(line string) (Key, error) {
	return &String{line}, nil
}

// Less compare two string keys.
func (k *String) Less(other Key) bool {
	return k.value < other.(*String).value //nolint //forcetypeassert
}

// Equal check tow string keys are equal.
func (k *String) Equal(other Key) bool {
	return k.value == other.(*String).value //nolint //forcetypeassert
}

// UpperString define an string key.
type UpperString struct {
	value string
}

// AllocateString create a new string key. It trims space and change the string to uppercase.
func AllocateUpperString(line string) (Key, error) {
	return &UpperString{strings.TrimSpace(strings.ToUpper(line))}, nil
}

// Less compare two upper string keys.
func (k *UpperString) Less(other Key) bool {
	return k.value < other.(*UpperString).value //nolint //forcetypeassert
}

// Equal check tow upper string keys are equal.
func (k *UpperString) Equal(other Key) bool {
	return k.value == other.(*UpperString).value //nolint //forcetypeassert
}
