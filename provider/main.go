package main

import (
	"fmt"
	"time"

	"github.com/moisespsena/go-fifofs"
	"github.com/moisespsena/go-error-wrap"
)

func main() {
	q, err := fifofs.NewQueue("data")
	if err != nil {
		panic(errwrap.Wrap(err, "New"))
	}
	t := time.Now()
	m, err := q.PutString(fmt.Sprint(t))
	if err != nil {
		panic(errwrap.Wrap(err, "Put"))
	}
	fmt.Println("Message ID:", m.ID())
}
