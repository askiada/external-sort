package file

import (
	"io/ioutil"
	"os"
	"path"

	"github.com/pkg/errors"
)

// clearFolder Remove all files from a folder.
func clearFolder(folder string) error {
	fn := "clear folder"
	err := os.MkdirAll(folder, os.ModePerm)
	if err != nil {
		return errors.Wrap(err, fn)
	}
	dir, err := ioutil.ReadDir(folder)
	if err != nil {
		return errors.Wrap(err, fn)
	}
	for _, d := range dir {
		err = os.RemoveAll(path.Join(folder, d.Name()))
		if err != nil {
			return errors.Wrap(err, fn)
		}
	}

	return nil
}
