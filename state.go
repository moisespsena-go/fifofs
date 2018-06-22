package fifofs

type State struct {
	Size int
	Last string
	First string
}

func (q *Queue) Size() int {
	return q.state.Size
}

func (q *Queue) IsEmpty() bool {
	return q.state.Size == 0
}