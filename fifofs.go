package fifofs

import (
	"io"
	"path/filepath"
	"os"
	"github.com/moisespsena/go-error-wrap"
	"gopkg.in/yaml.v2"
	"github.com/moisespsena/go-path-helpers"
	"sync"
)

type Queue struct {
	dir       string
	statePath string
	state     *State
	lock      sync.Mutex
}

func NewQueue(dir string) (*Queue, error) {
	q := &Queue{dir: filepath.Clean(dir), statePath: filepath.Join(dir, "state.yaml")}

	if path_helpers.IsExistingRegularFile(q.statePath) {
		f, err := os.Open(q.statePath)
		if err != nil {
			return nil, errwrap.Wrap(err, "Open State file")
		}
		defer f.Close()
		dec := yaml.NewDecoder(f)
		state := &State{}
		err = dec.Decode(state)
		if err != nil {
			return nil, errwrap.Wrap(err, "Decode State")
		}
		q.state = state
	}
	return q, nil
}

func (q *Queue) Get() (*Message, error) {
	q.lock.Lock()
	defer q.lock.Unlock()

	if q.state.Size == 0 {
		return nil, io.EOF
	}
	firstId := q.state.First
	m := &Message{id: firstId, dir: &q.dir}
	stat, err := os.Stat(m.FilePath())
	if err != nil {
		return nil, errwrap.Wrap(err, "Stat")
	}
	m.Size = stat.Size() - 37
	f, err := os.Open(m.FilePath())
	if err != nil {
		return nil, errwrap.Wrap(err, "Open")
	}
	nextId := make([]byte, 37)
	n, err := f.Read(nextId)
	if err != nil {
		return nil, errwrap.Wrap(err, "Read header")
	}
	if n != 37 {
		return nil, errwrap.Wrap(err, "Unexpected header")
	}
	if nextId[0] == '\x00' {
		q.state.First = ""
		q.state.Last = ""
	} else {
		q.state.First = string(nextId[0:36])
	}
	q.state.Size--
	err = q.saveState()
	if err != nil {
		return nil, err
	}
	m.ReadCloser = &MessageReader{func() error {
		return q.removeMessage(m)
	}, f}
	return m, nil
}

func (q *Queue) NewMessage() (*Message, io.WriteCloser, error) {
	m := &Message{}
	m.id = NewId()
	pth, err := Rpj(q.dir, IdToPath(m.id))
	if err != nil {
		return nil, nil, errwrap.Wrap(err, "MakeDir")
	}
	m.dir = &q.dir
	w, err := os.Create(pth)
	if err != nil {
		return nil, nil, errwrap.Wrap(err, "Create file")
	}
	s := len(m.id)
	nextId := make([]byte, s+1)
	nextId[s] = '\n'
	_, err = w.Write(nextId)
	if err != nil {
		defer q.removeMessage(m)
		w.Close()
		return nil, nil, errwrap.Wrap(err, "Write Header")
	}
	return m, w, nil
}

func (q *Queue) PutBuffer(r io.Reader, buf []byte) (*Message, error) {
	return q.PutBuffer(r, nil)
}

func (q *Queue) Put(r io.Reader) (*Message, error) {
	q.lock.Lock()
	defer q.lock.Unlock()

	m, w, err := q.NewMessage()
	if err != nil {
		return nil, err
	}
	_, err = io.Copy(w, r)
	if err != nil {
		defer q.removeMessage(m)
		w.Close()
		return nil, errwrap.Wrap(err, "Write Data")
	}
	return q.saveMessage(m)
}

func (q *Queue) saveState() error {
	statePath := q.statePath
	f, err := os.Create(statePath)
	if err != nil {
		return errwrap.Wrap(err, "Create State File")
	}
	defer f.Close()
	enc := yaml.NewEncoder(f)
	err = enc.Encode(q.state)
	if err != nil {
		return errwrap.Wrap(err, "Write State File")
	}
	return nil
}

func (q *Queue) saveMessage(m *Message) (*Message, error) {
	if q.state.Size > 0 {
		lastPath := filepath.Join(q.dir, IdToPath(q.state.Last))
		w, err := os.OpenFile(lastPath, os.O_RDWR, 0666)
		if err != nil {
			return nil, errwrap.Wrap(err, "Open previous message for set next ID")
		}
		defer w.Close()
		_, err = w.WriteString(m.id)
		if err != nil {
			return nil, errwrap.Wrap(err, "Write ID in previous message")
		}
	}

	if q.state.Size == 0 {
		q.state.First = m.id
	}
	q.state.Size++
	q.state.Last = m.id

	err := q.saveState()
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (q *Queue) PutString(data string) (*Message, error) {
	return q.PutBytes([]byte(data))
}

func (q *Queue) PutBytes(data []byte) (*Message, error) {
	m, w, err := q.NewMessage()
	if err != nil {
		return nil, err
	}
	q.lock.Lock()
	defer q.lock.Unlock()
	if len(data) > 0 {
		_, err = w.Write(data)
		if err != nil {
			return nil, errwrap.Wrap(err, "Write Data")
		}
	}
	return q.saveMessage(m)
}

func (q *Queue) removeMessage(m *Message) error {
	d := *m.dir
	p := m.FilePath()
	err := os.Remove(p)
	if err != nil {
		return errwrap.Wrap(err, "Remove")
	}

	var names []string

	for {
		p = filepath.Dir(p)
		if p == d {
			break
		}
		f, err := os.Open(p)
		if err != nil {
			return errwrap.Wrap(err, "ReadDir")
		}
		names, err = f.Readdirnames(2)
		if err != nil && err != io.EOF {
			return errwrap.Wrap(err, "Readdirnames")
		}
		if len(names) > 0 {
			break
		}
		err = os.Remove(p)
		if err != nil {
			return errwrap.Wrap(err, "Remove")
		}
	}
	return nil
}

type Message struct {
	id   string
	dir  *string
	w    io.WriteCloser
	io.ReadCloser
	Size int64
}

func (m *Message) ID() string {
	return m.id
}

func (m *Message) FilePath() string {
	return filepath.Join(*m.dir, IdToPath(m.id))
}
