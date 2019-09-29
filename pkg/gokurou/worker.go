package gokurou

import (
	"context"
	"time"

	"github.com/murakmii/gokurou/pkg/gokurou/www"

	"github.com/google/uuid"
)

type Worker struct{}

func NewWorker() *Worker {
	return &Worker{}
}

func (w *Worker) Start(ctx context.Context, conf *Configuration) {
	logger := LoggerFromContext(ctx)

	coordinator, err := conf.CoordinatorProvider(conf)
	if err != nil {
		logger.Errorf("failed to initialize coordinator: %v", err)
		return
	}

	gwn, err := coordinator.AllocNextGWN()
	if err != nil {
		logger.Errorf("failed to allocate global worker number: %v", err)
		return
	}

	ctx, cancel := WorkerContext(ctx, gwn)
	logger = LoggerFromContext(ctx)
	logger.Info("started worker")

	resultCh := make(chan error, 3)
	results := make([]error, 0, 3)

	frontier, popCh, pushCh := w.startURLFrontier(ctx, conf, coordinator, resultCh)
	gatherer, acCh := w.startArtifactCollector(ctx, conf, resultCh)
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

	resOwners := []Finisher{crawler, frontier, gatherer, coordinator}
	for _, resOwner := range resOwners {
		if resOwner == nil {
			continue
		}

		if err := resOwner.Finish(); err != nil {
			logger.Errorf("failed to finish component: %v", err)
		}
	}
}

func (w *Worker) startArtifactCollector(ctx context.Context, conf *Configuration, resultCh chan<- error) (ArtifactGatherer, chan<- interface{}) {
	ctx = ComponentContext(ctx, "artifact-collector")
	inputCh := make(chan interface{}, 5)

	ac, err := conf.ArtifactGathererProvider(ctx, conf)
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

func (w *Worker) startURLFrontier(ctx context.Context, conf *Configuration, coordinator Coordinator, resultCh chan<- error) (URLFrontier, <-chan *www.SanitizedURL, chan<- *www.SanitizedURL) {
	ctx = ComponentContext(ctx, "url-frontier")
	popCh := make(chan *www.SanitizedURL, 5)
	pushCh := make(chan *www.SanitizedURL, 10)

	urlFrontier, err := conf.URLFrontierProvider(ctx, conf)
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
					case <-time.After(100 * time.Millisecond):
						// nop
					case <-ctx.Done():
						childErrCh <- nil
						return
					}
				} else {
					locked, err := coordinator.LockByIPAddrOf(url.Host())
					if err != nil {
						childErrCh <- err
						return
					}

					if !locked {
						continue // TODO: IPアドレスでロックできなかったURLはとりあえず捨てている
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

func (w *Worker) startCrawler(ctx context.Context, conf *Configuration, popCh <-chan *www.SanitizedURL, out OutputPipeline, resultCh chan<- error) Crawler {
	ctx = ComponentContext(ctx, "crawler")
	crawler, err := conf.CrawlerProvider(ctx, conf)
	if err != nil {
		resultCh <- err
		return nil
	}

	go func() {
		for {
			select {
			case url := <-popCh:
				// loggerにUUIDを付ける
				id, _ := uuid.NewRandom()
				logger := LoggerFromContext(ctx)
				ctx = ContextWithLogger(ctx, logger.WithField("id", id.String()))

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
