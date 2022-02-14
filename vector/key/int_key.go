package key

import "strconv"

type Int struct {
	value int
}

func AllocateInt(line string) (Key, error) {
	num, err := strconv.Atoi(line)
	if err != nil {
		return nil, err
	}
	return &Int{num}, nil
}

func (k *Int) Less(other Key) bool {
	return k.value < other.(*Int).value
}
