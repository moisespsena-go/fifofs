package fifofs

import (
	"io"
	"fmt"
	"net"

	"github.com/moisespsena/go-error-wrap"
	"strconv"
	"bufio"
)

type QueueServer struct {
	BuffSize int
	Queue *Queue
	Addr  string
	Log   func(args ...interface{})
	Error func(args ...interface{})
}

const STATUS_OK = 0
const STATUS_CLIENT_ERR = 1
const STATUS_SERVER_ERR = 2
const STATUS_EOF = 3

func (qs *QueueServer) sendStatus(con net.Conn, status int, msg interface{}) {
	con.Write([]byte(strconv.Itoa(status) + "\r\n"))
	if msg != nil {
		msgs := fmt.Sprint(msg)
		con.Write([]byte(fmt.Sprintf("%0x\r\n%v\r\n", len(msgs), msgs)))
	}
}

func (qs *QueueServer) handleRequest(con net.Conn) error {
	cmdb := make([]byte, 3)
	_, err := con.Read(cmdb)
	if err != nil {
		return errwrap.Wrap(err, "Read Command Name")
	}
	cmd := string(cmdb)
	switch cmd {
	case "q\r\n":
		return io.EOF
	case "s\r\n":
		qs.sendStatus(con, STATUS_OK, strconv.Itoa(qs.Queue.state.Size))
	case "g\r\n":
		m, err := qs.Queue.Get()
		if err != nil {
			if err == io.EOF {
				con.Write([]byte("1\r\n"))
				return nil
			}
			qs.sendStatus(con, STATUS_SERVER_ERR, errwrap.Wrap(err, "Get"))
			return nil
		}
		qs.sendStatus(con, STATUS_OK, m.id)
		defer m.Close()
		con.Write([]byte(fmt.Sprintf("%0x\r\n", m.Size)))
		io.Copy(con, m)
		con.Write([]byte("\r\n"))
	case "p\r\n":
		r := bufio.NewReader(con)
		s, err := r.ReadString('\n')
		if err != nil {
			qs.sendStatus(con, STATUS_CLIENT_ERR, errwrap.Wrap(err, "Read Message Size"))
			return nil
		}
		// skip CRLF
		s = s[0:len(s)-2]
		size, err := strconv.ParseInt("0x" + s, 0, 64)
		if err != nil {
			qs.sendStatus(con, STATUS_CLIENT_ERR, errwrap.Wrap(err, "Parse Message Size"))
			return nil
		}

		m, err := qs.Queue.PutBuffer(io.LimitReader(r, size), make([]byte, qs.BuffSize))
		if err != nil {
			qs.sendStatus(con, STATUS_SERVER_ERR, errwrap.Wrap(err, "PutBuffer"))
			return nil
		}
		qs.sendStatus(con, STATUS_OK, m.id)
	default:
		qs.sendStatus(con, STATUS_CLIENT_ERR, errwrap.Wrap(err, "Invalid Command %q", string(cmd)))
	}
	return nil
}

func (qs *QueueServer) HandleRequest(con net.Conn) {
	for {
		err := qs.handleRequest(con)
		if err != nil {
			con.Close()
			break
		}
	}
}

func (qs *QueueServer) Forever() error {
	if qs.BuffSize == 0 {
		qs.BuffSize = 1024 * 10
	}

	l, err := net.Listen("tcp", qs.Addr)
	if err != nil {
		return errwrap.Wrap(err, "Error listening")
	}
	// Close the listener when the application closes.
	defer l.Close()
	qs.Log("Listening on " + qs.Addr)
	for {
		// Listen for an incoming connection.
		conn, err := l.Accept()
		if err != nil {
			return errwrap.Wrap(err, "Error Accepting")
		}
		// Handle connections in a new goroutine.
		go qs.HandleRequest(conn)
	}
}