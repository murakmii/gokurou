package pkg

import (
	"context"
	"sync"
	"time"

	"github.com/murakmii/gokurou/pkg/html"
)

type Worker struct{}

func NewWorker() *Worker {
	return &Worker{}
}

func (w *Worker) Start(ctx context.Context, wg *sync.WaitGroup, conf *Configuration) {
	go func() {
		defer wg.Done()

		logger := LoggerFromContext(ctx)

		syncer, err := conf.NewSynchronizer(conf)
		if err != nil {
			logger.Errorf("failed to initialize synchronizer: %v", err)
			return
		}

		gwn, err := syncer.AllocNextGWN()
		if err != nil {
			logger.Errorf("failed to allocate global worker number: %v", err)
			return
		}

		ctx, cancel := WorkerContext(ctx, gwn)
		logger = LoggerFromContext(ctx)

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
				logger.Errorf("failed to finish component: %v", err)
			}
		}
	}()
}

func (w *Worker) startArtifactCollector(ctx context.Context, conf *Configuration, resultCh chan<- error) (ArtifactCollector, chan<- interface{}) {
	ctx = ComponentContext(ctx, "artifact-collector")
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
	ctx = ComponentContext(ctx, "url-frontier")
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
	ctx = ComponentContext(ctx, "crawler")
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
