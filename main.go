package main

import (
	"fmt"
	"time"

	"github.com/dihedron/go-workers/workers"
)

func myHandler(context interface{}, task interface{}) bool {
	//fmt.Printf("running on %v\n", task)
	return true
}

// Payment represents a bank payment.
type Payment struct {
	from   int
	to     int
	amount int
}

func main() {

	max := 1000000
	pool := workers.New(1)
	pool.Start(myHandler)

	t0 := time.Now()
	for i := 0; i < max; i++ {
		pool.Submit(Payment{from: i, to: i, amount: i})
	}
	pool.WaitForCompletion()
	t1 := time.Now()
	fmt.Printf("The call took %v to run on %d messages\n", t1.Sub(t0), max)
}
