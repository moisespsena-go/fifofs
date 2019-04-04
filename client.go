package fifofs

import (
	"net"
	"bufio"
	"github.com/moisespsena-go/error-wrap"
	"io"
	"strconv"
)

type Client struct {
	Addr string
	con  net.Conn
	r    *bufio.Reader
}

func NewClient(addr string) *Client {
	return &Client{Addr: addr}
}

func (c *Client) Connect() error {
	con, err := net.Dial("tcp", c.Addr)
	if err != nil {
		return err
	}
	c.con = con
	c.r = bufio.NewReader(con)
	return nil
}

type RemoteMessage struct {
	ID   string
	Data []byte
}

type RemoteError struct {
	Message string
}

func (r RemoteError) Error() string {
	return r.Message
}

func (c *Client) Put(data []byte) (id string, err error) {
	err = c.sendCommand("p")
	if err != nil {
		return "", err
	}
	status, err := c.readStatus()
	if err != nil {
		return "", err
	}
	switch status {
	case STATUS_SERVER_ERR:
		msg, err := c.readMessage()
		if err != nil {
			return "", errwrap.Wrap(err, "Read Server Error")
		}
		return "", &RemoteError{string(msg)}
	case STATUS_CLIENT_ERR:
		msg, err := c.readMessage()
		if err != nil {
			return "", errwrap.Wrap(err, "Read Client Error")
		}
		return "", &RemoteError{string(msg)}
	case STATUS_OK:
		msgID, err := c.readMessage()
		if err != nil {
			return "", errwrap.Wrap(err, "Read Message ID")
		}
		return string(msgID), nil
	}
	return "", nil
}

func (c *Client) readMessage() ([]byte, error) {
	line, err := c.r.ReadString('\n')
	if err != nil {
		return nil, errwrap.Wrap(err, "Read Message Size")
	}
	line = line[0 : len(line)-2]

	size, err := strconv.ParseInt("0x"+line, 0, 64)
	if err != nil {
		return nil, errwrap.Wrap(err, "Parse Message Size")
	}

	buf := make([]byte, size)
	_, err = c.r.Read(buf)
	if err != nil {
		return nil, errwrap.Wrap(err, "Read Message Data")
	}
	return buf, nil
}

func (c *Client) readStatus() (int, error) {
	line, err := c.r.ReadString('\n')
	if err != nil {
		return 0, errwrap.Wrap(err, "Read status")
	}
	line = line[0 : len(line)-2]
	status, err := strconv.Atoi(line)
	if err != nil {
		return 0, errwrap.Wrap(err, "Parse status")
	}
	return status, nil
}

func (c *Client) sendCommand(cmd string) error {
	_, err := c.con.Write([]byte(cmd + "\r\n"))
	if err != nil {
		return errwrap.Wrap(err, "Send Command name")
	}
	return nil
}

func (c *Client) Get() (m *RemoteMessage, err error) {
	err = c.sendCommand("g")
	if err != nil {
		return nil, err
	}
	status, err := c.readStatus()
	if err != nil {
		return nil, err
	}

	switch status {
	case STATUS_EOF:
		return nil, io.EOF
	case STATUS_SERVER_ERR:
		msg, err := c.readMessage()
		if err != nil {
			return nil, errwrap.Wrap(err, "Read Server Error")
		}
		return nil, &RemoteError{string(msg)}
	case STATUS_CLIENT_ERR:
		msg, err := c.readMessage()
		if err != nil {
			return nil, errwrap.Wrap(err, "Read Client Error")
		}
		return nil, &RemoteError{string(msg)}
	case STATUS_OK:
		msgID, err := c.readMessage()
		if err != nil {
			return nil, errwrap.Wrap(err, "Read Message ID")
		}
		m := &RemoteMessage{ID: string(msgID)}
		m.Data, err = c.readMessage()
		if err != nil {
			return nil, errwrap.Wrap(err, "Read Message Data")
		}
		return m, nil
	}
	return nil, nil
}
