package pkg

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/murakmii/gokurou/pkg/html"
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

		resultCh := make(chan error, 3)
		results := make([]error, 0, 3)

		frontier, popCh, pushCh := w.startURLFrontier(ctx, conf, syncer, resultCh)
		artifactCollector, acCh := w.startArtifactCollector(ctx, conf, resultCh)
		crawler := w.startCrawler(ctx, conf, popCh, NewOutputPipeline(acCh, pushCh), resultCh)

		for {
			select {
			case err := <-resultCh:
				results = append(results, err)
				if err != nil {
					cancel()
				}
			}

			if len(results) == 3 {
				break
			}
		}

		resOwners := []ResourceOwner{crawler, frontier, artifactCollector, syncer}
		for _, resOwner := range resOwners {
			if resOwner == nil {
				continue
			}

			if err := resOwner.Finish(); err != nil {
				// TODO: error logging
			}
		}
	}()
}

func (w *Worker) buildWorkerContext(ctx context.Context, globalWorkerNum, localWorkerNum uint16) (context.Context, context.CancelFunc) {
	ctx = context.WithValue(ctx, ctxKeyGlobalWorkerNumber, globalWorkerNum)
	ctx = context.WithValue(ctx, ctxKeyLocalWorkerNumber, localWorkerNum)
	return context.WithCancel(ctx)
}

func (w *Worker) startArtifactCollector(ctx context.Context, conf *Configuration, resultCh chan<- error) (ArtifactCollector, chan<- interface{}) {
	inputCh := make(chan interface{}, 5)

	ac, err := conf.NewArtifactCollector(ctx, conf)
	if err != nil {
		resultCh <- err
		return nil, inputCh
	}

	go func() {
		for {
			select {
			case artifact := <-inputCh:
				if err := ac.Collect(artifact); err != nil {
					resultCh <- err
					return
				}

			case <-ctx.Done():
				resultCh <- nil
				return
			}
		}
	}()

	return ac, inputCh
}

func (w *Worker) startURLFrontier(ctx context.Context, conf *Configuration, syncer Synchronizer, resultCh chan<- error) (URLFrontier, <-chan *html.SanitizedURL, chan<- *html.SanitizedURL) {
	popCh := make(chan *html.SanitizedURL, 5)
	pushCh := make(chan *html.SanitizedURL, 10)

	urlFrontier, err := conf.NewURLFrontier(ctx, conf)
	if err != nil {
		resultCh <- err
		return nil, popCh, pushCh
	}

	go func() {
		ctx, cancel := context.WithCancel(ctx)
		childErrCh := make(chan error)
		results := make([]error, 0, 2)

		// Pop loop
		go func() {
			for {
				url, err := urlFrontier.Pop(ctx)
				if err != nil {
					childErrCh <- err
					cancel()
					return
				}

				if url == nil {
					select {
					case <-time.After(1 * time.Second):
						// nop
					case <-ctx.Done():
						childErrCh <- nil
						return
					}
				} else {
					locked, err := syncer.LockByIPAddrOf(url.Host())
					if err != nil {
						childErrCh <- err
						return
					}

					if !locked {
						// TODO: IPアドレスでロックできなかったURLはとりあえず捨てている
						continue
					}

					select {
					case popCh <- url:
						// nop
					case <-ctx.Done():
						childErrCh <- nil
						return
					}
				}
			}
		}()

		// Push loop
		go func() {
			for {
				select {
				case url := <-pushCh:
					if err := urlFrontier.Push(ctx, url); err != nil {
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

		for i := 0; i < len(results); i++ {
			if results[i] != nil {
				resultCh <- results[i]
				return
			}
		}

		resultCh <- nil
	}()

	return urlFrontier, popCh, pushCh
}

func (w *Worker) startCrawler(ctx context.Context, conf *Configuration, popCh <-chan *html.SanitizedURL, out *OutputPipeline, resultCh chan<- error) Crawler {
	crawler, err := conf.NewCrawler(ctx, conf)
	if err != nil {
		resultCh <- err
		return nil
	}

	go func() {
		for {
			select {
			case url := <-popCh:
				if err := crawler.Crawl(ctx, url, out); err != nil {
					resultCh <- err
					return
				}

			case <-ctx.Done():
				resultCh <- nil
				return
			}
		}
	}()

	return crawler
}
