package key

import (
	"strings"

	"github.com/pkg/errors"
)

const salt = "##!##"

func AllocateTsv(row interface{}, pos ...int) (Key, error) {
	splitted, ok := row.([]string)
	if !ok {
		return nil, errors.Errorf("can't convert interface{} to []string: %+v", row)
	}
	k := strings.Builder{}
	for i, p := range pos {
		if len(splitted) < p+1 {
			return nil, errors.Errorf("can't allocate tsv key line is invalid: %s", row)
		}
		k.WriteString(splitted[p])
		if i < len(pos)-1 {
			k.WriteString(salt)
		}
	}

	// fmt.Println(row, pos, k.String())

	return &String{k.String()}, nil
}
