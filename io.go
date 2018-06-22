package fifofs

import (
	"io"
)

type MessageReader struct {
	done func()error
	r io.ReadCloser
}

func (r *MessageReader) Read(p []byte) (n int, err error) {
	return r.r.Read(p)
}

func (r *MessageReader) Close() (err error) {
	err = r.r.Close()
	if err == nil {
		return r.done()
	}
	return err
}

