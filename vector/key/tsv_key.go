package key

import (
	"strings"

	"github.com/pkg/errors"
)

func AllocateTsv(line string, pos int) (Key, error) {
	splitted := strings.Split(line, "\t")
	if len(splitted) < pos+1 {
		return nil, errors.Errorf("can't allocate tsv key line is invalid: %s", line)
	}
	return &String{splitted[pos]}, nil
}

func AllocateEmptyTsv() Key {
	return &Tsv{}
}

func (k *Tsv) Get() interface{} {
	return k.value
}

func (k *Tsv) Less(other Key) bool {
	return k.value < other.(*Tsv).value
}

func (k *Tsv) String() string {
	return k.value
}

func (k *Tsv) FromString(text string) error {
	k.value = text
	return nil
}
