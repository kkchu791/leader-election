package main

import (
	"fmt"
	"sync"
	"time"
)

func node(name string, duration int) {
	time.Sleep(time.Duration(duration) * time.Millisecond)
	fmt.Println(name, " election timeout")
}

func main() {

	var wg sync.WaitGroup
	wg.Go(func() {
		node("A", 1600)
	})
	wg.Go(func() {
		node("B", 3000)
	})

	wg.Wait()
}
