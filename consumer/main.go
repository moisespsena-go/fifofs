package main

import (
	"fmt"

	"io/ioutil"
	"github.com/moisespsena/go-fifofs"
	"github.com/moisespsena/go-error-wrap"
	"io"
)

func main() {
	queue, err := fifofs.NewQueue("data")
	if err != nil {
		panic(errwrap.Wrap(err, "New"))
	}
	message, err := queue.Get()
	if err != nil {
		if err == io.EOF {
			println("EMPTY")
			return
		}
		panic(errwrap.Wrap(err, "Get"))
	}
	defer message.Close()
	fmt.Println("Message ID:", message.ID())
	b, err := ioutil.ReadAll(message)
	if err != nil {
		panic(errwrap.Wrap(err, "Get DATA"))
	}

	fmt.Println("Message DATA:", string(b))
}
