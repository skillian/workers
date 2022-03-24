package workers

import (
	"context"
	"sync"
)

// Worker functions are passed into the Work function to process work
// requests in separate goroutines.
type Worker[TRequest any, TResult any] func(context.Context, TRequest) (TResult, error)

// WorkOption is an option to modify the behavior of the Work function
type WorkOption func(*workConfig)

type workConfig struct {
	numWorkers int
	channelCap int
}

// WorkerCount specifies the number of worker goroutines to create in
// the Work function.
func WorkerCount(n int) WorkOption {
	return func(wc *workConfig) {
		wc.numWorkers = n
	}
}

// ResultChannelCapacity defines the Work function's output channel
// capacity.
func ResultChannelCapacity(n int) WorkOption {
	return func(wc *workConfig) {
		wc.channelCap = n
	}
}

type WorkResult[TResult any] struct {
	Res TResult
	Err error
}

func Work[TRequest any, TResult any](ctx context.Context, requests chan TRequest, worker Worker[TRequest, TResult], options ...WorkOption) chan WorkResult[TResult] {
	wc := workConfig{numWorkers: 1}
	for _, opt := range options {
		opt(&wc)
	}
	var results chan WorkResult[TResult]
	if wc.channelCap > 0 {
		results = make(chan WorkResult[TResult], wc.channelCap)
	} else {
		results = make(chan WorkResult[TResult], wc.numWorkers)
	}
	wg := sync.WaitGroup{}
	wg.Add(wc.numWorkers)
	for i := 0; i < wc.numWorkers; i++ {
		go func(workerId int) {
			defer wg.Done()
			select {
			case req, ok := <-requests:
				if !ok {
					return
				}
				res, err := worker(ctx, req)
				results <- WorkResult[TResult]{
					Res: res,
					Err: err,
				}
			case <-ctx.Done():
				return
			}
		}(i)
	}
	go func() {
		wg.Wait()
		close(results)
	}()
	return results
}
