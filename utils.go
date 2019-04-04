package fifofs

import (
	"path/filepath"
	"github.com/moisespsena/go-path-helpers"
	"github.com/moisespsena-go/error-wrap"
	"os"
	"github.com/satori/go.uuid"
)

func Rpj(pth ...string) (string, error) {
	p := filepath.Join(pth...)
	if p != "." {
		dir := filepath.Dir(p)
		if !path_helpers.IsExistingDir(dir) {
			perms, err := path_helpers.ResolvPerms(dir)
			if err != nil {
				return "", errwrap.Wrap(err, "Resolv perms")
			}
			err = os.MkdirAll(dir, os.FileMode(perms))
			if err != nil {
				return "", errwrap.Wrap(err, "MkdirAll")
			}
		}
	}
	return p, nil
}

func NewId() string {
	u1 := uuid.Must(uuid.NewV1())
	return u1.String()
}

func IdToPath(id string) string {
	return id[:2] + string(os.PathSeparator) + id[2:6] + string(os.PathSeparator) + id + ".raw"
}

func IdFromPath(pth string) string {
	pth = filepath.Base(pth)
	return pth[0:len(pth)-4]
}