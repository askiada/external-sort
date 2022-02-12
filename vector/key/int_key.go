package key

import (
	"bytes"
	"fmt"
	"strconv"
)

type Int struct {
	value int
	text  string
}

// dropCR drops a terminal \r from the data.
func dropCR(data []byte) []byte {
	if len(data) > 0 && data[len(data)-1] == '\r' {
		return data[0 : len(data)-1]
	}
	return data
}

func AllocateEmptyInt() Key {
	return &Int{}
}

func AllocateInt(line string) (Key, error) {
	fmt.Println(line)
	bline := []byte(line)
	if i := bytes.IndexByte(bline, '\n'); i >= 0 {
		// We have a full newline-terminated line.
		bline = dropCR(bline[0:i])
	}
	num, err := strconv.Atoi(string(bline))
	if err != nil {
		return nil, err
	}
	return &Int{value: num, text: string(bline)}, nil
}

func (k *Int) Less(other Key) bool {
	return k.value < other.(*Int).value
}

func (k *Int) String() string {
	return k.text
}

func (k *Int) FromString(text string) error {
	value, err := strconv.Atoi(text)
	if err != nil {
		return err
	}
	k.value = value
	k.text = text
	return nil
}
