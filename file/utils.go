package file

import (
	"os"
	"path"
	"strings"

	"github.com/pkg/errors"
)

// clearChunkFolder Remove all files from a folder.
func clearChunkFolder(folder string) error {
	err := os.MkdirAll(folder, os.ModePerm)
	if err != nil {
		return errors.Wrap(err, "can't create folder")
	}
	dir, err := os.ReadDir(folder)
	if err != nil {
		return errors.Wrap(err, "can't read chunk folder")
	}
	for _, d := range dir {
		if !strings.HasPrefix(d.Name(), "chunk") {
			continue
		}
		err = os.RemoveAll(path.Join(folder, d.Name()))
		if err != nil {
			return errors.Wrap(err, "can't clear chunk folder")
		}
	}
	return nil
}
