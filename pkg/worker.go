package pkg

import (
	"context"
	"fmt"
	"sync"

	"github.com/murakmii/gokurou/pkg/html"
	"github.com/murakmii/gokurou/pkg/worker"
)

type Worker struct{}

const (
	ctxKeyGlobalWorkerNumber = "GLOBAL_WORKER_NUMBER"
	ctxKeyLocalWorkerNumber  = "LOCAL_WORKER_NUMBER"
)

func NewWorker() *Worker {
	return &Worker{}
}

func GWNFromContext(ctx context.Context) uint16 {
	value, ok := ctx.Value(ctxKeyGlobalWorkerNumber).(uint16)
	if !ok {
		panic(fmt.Errorf("can't fetch global worker number from context"))
	}

	return value
}

func (w *Worker) Start(ctx context.Context, wg *sync.WaitGroup, conf *Configuration, localWorkerNum uint16) {
	go func() {
		defer wg.Done()

		syncer, err := conf.NewSynchronizer(conf)
		if err != nil {
			// TODO: error logging
			return
		}

		globalWorkerNum, err := syncer.GetNextGlobalWorkerNumber()
		if err != nil {
			// TODO: error logging
			return
		}

		ctx, cancel := w.buildWorkerContext(ctx, globalWorkerNum, localWorkerNum)

		artifactInputCh, artifactErrCh := w.startArtifactCollector(ctx, conf)
		popCh, pushCh, frontierErrCh := w.startURLFrontier(ctx, conf)
		results := make([]error, 0, 2)

		_ = worker.NewDataPipeline(artifactInputCh, popCh, pushCh)

		// TODO: Crawler

		for {
			select {
			case err := <-artifactErrCh:
				results = append(results, err)
				if err != nil {
					cancel()
				}

			case err := <-frontierErrCh:
				results = append(results, err)
				if err != nil {
					cancel()
				}
			}

			if len(results) == 2 {
				break
			}
		}

		// TODO: error logging
	}()
}

func (w *Worker) buildWorkerContext(ctx context.Context, globalWorkerNum, localWorkerNum uint16) (context.Context, context.CancelFunc) {
	ctx = context.WithValue(ctx, ctxKeyGlobalWorkerNumber, globalWorkerNum)
	ctx = context.WithValue(ctx, ctxKeyLocalWorkerNumber, localWorkerNum)
	return context.WithCancel(ctx)
}

func (w *Worker) startArtifactCollector(ctx context.Context, conf *Configuration) (chan<- interface{}, <-chan error) {
	errCh := make(chan error, 1)
	inputCh := make(chan interface{}, 5)

	artifactCollector, err := conf.NewArtifactCollector(ctx, conf)
	if err != nil {
		errCh <- err
		return inputCh, errCh
	}

	go func() {
		for {
			select {
			case artifact := <-inputCh:
				if err := artifactCollector.Collect(artifact); err != nil {
					errCh <- err
					return
				}

			case <-ctx.Done():
				errCh <- artifactCollector.Finish()
				return
			}
		}
	}()

	return inputCh, errCh
}

func (w *Worker) startURLFrontier(ctx context.Context, conf *Configuration) (<-chan *html.SanitizedURL, chan<- *html.SanitizedURL, <-chan error) {
	popCh := make(chan *html.SanitizedURL, 5)
	pushCh := make(chan *html.SanitizedURL, 10)
	errCh := make(chan error, 1)

	urlFrontier, err := conf.NewURLFrontier(ctx, conf)
	if err != nil {
		errCh <- err
		return popCh, pushCh, errCh
	}

	go func() {
		ctx, cancel := context.WithCancel(ctx)
		childErrCh := make(chan error)
		results := make([]error, 0, 2)

		// Pop loop
		go func() {
			for {
				url, err := urlFrontier.Pop()
				if err != nil {
					childErrCh <- err
					cancel()
					return
				}

				select {
				case popCh <- url:
					// nop
				case <-ctx.Done():
					childErrCh <- nil
					return
				}
			}
		}()

		// Push loop
		go func() {
			for {
				select {
				case url := <-pushCh:
					if err := urlFrontier.Push(url); err != nil {
						childErrCh <- err
						cancel()
						return
					}
				case <-ctx.Done():
					childErrCh <- nil
					return
				}
			}
		}()

		for {
			select {
			case pubErr := <-childErrCh:
				results = append(results, pubErr)
			}

			if len(results) == 2 {
				break
			}
		}

		if err := urlFrontier.Finish(); err != nil {
			errCh <- err
			return
		}

		if results[0] != nil {
			errCh <- results[0]
			return
		}

		if results[1] != nil {
			errCh <- results[1]
			return
		}

		errCh <- nil
	}()

	return popCh, pushCh, errCh
}
