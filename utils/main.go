package utils

import "sync"

func RunAsync(wg *sync.WaitGroup, fn func()) {
	wg.Add(1)
	go func() {
		defer wg.Done() // Run at the very end whatever happened
		// Always gets called, even if fn() panics or returns early
		fn()
	}()
}
