package pkg

import (
	"context"
	"sync"

	"github.com/murakmii/gokurou/pkg/html"
	"github.com/murakmii/gokurou/pkg/worker"
)

type Worker struct {
	localWorkerNum  uint16
	globalWorkerNum uint16
}

func NewWorker() *Worker {
	return &Worker{}
}

func (w *Worker) Start(ctx context.Context, wg *sync.WaitGroup, conf *Configuration, localWorkerNum uint16) {
	syncer := conf.NewSynchronizer()

	w.localWorkerNum = localWorkerNum
	globalWorkerNum, err := syncer.GetNextGlobalWorkerNumber()
	if err != nil {
		// TODO: エラーハンドル
		return
	}
	w.globalWorkerNum = globalWorkerNum

	go func() {
		defer wg.Done()

		ctx, cancel := context.WithCancel(ctx)

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

func (w *Worker) startArtifactCollector(ctx context.Context, conf *Configuration) (chan<- interface{}, <-chan error) {
	artifactCollector := conf.NewArtifactCollector()

	inputCh := make(chan interface{}, artifactCollector.DeclareBufferSize())
	errCh := make(chan error)

	go func() {
		if err := artifactCollector.Init(conf); err != nil {
			errCh <- err
			return
		}

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
	pushCh := make(chan *html.SanitizedURL)
	errCh := make(chan error, 1)

	go func() {
		urlFrontier := conf.NewURLFrontier()
		if err := urlFrontier.Init(conf); err != nil {
			errCh <- err
			return
		}

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
