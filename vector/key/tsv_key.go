package key

import (
	"strings"

	"github.com/pkg/errors"
)

type Tsv struct {
	value string
}

func AllocateTsv(line string, pos int) (Key, error) {
	splitted := strings.Split(line, "\t")
	if len(splitted) < pos+1 {
		return nil, errors.Errorf("can't allocate tsv key line is invalid: %s", line)
	}
	return &String{splitted[pos]}, nil
}

func (k *Tsv) Less(other Key) bool {
	return k.value < other.(*Tsv).value
}
