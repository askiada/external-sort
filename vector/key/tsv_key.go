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
	strBuilder := strings.Builder{}
	for i, p := range pos {
		if len(splitted) < p+1 {
			return nil, errors.Errorf("can't allocate tsv key line is invalid: %s", row)
		}
		strBuilder.WriteString(splitted[p])
		if i < len(pos)-1 {
			strBuilder.WriteString(salt)
		}
	}

	return &String{strBuilder.String()}, nil
}
