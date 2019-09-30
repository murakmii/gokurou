package gokurou

import (
	"context"
	"time"

	"github.com/murakmii/gokurou/pkg/gokurou/www"

	"github.com/google/uuid"
)

type Worker struct {
	resultCh chan error
}

const (
	// ArtifactGatherer, URLFrontier(Pop+Push), Crawlerからの計4つの結果を待つ
	expectedResults = 4
)

func NewWorker() *Worker {
	return &Worker{}
}

// Workerの処理を開始する。何がしかのエラーが発生するか、ContextがDoneするまで処理をブロックする
func (w *Worker) Start(ctx context.Context, conf *Configuration) {
	w.resultCh = make(chan error, expectedResults)
	logger := LoggerFromContext(ctx)

	// Coordinatorを生成してGWNを得る
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
	logger.Info("worker is started")

	// 各種SubSystemを生成し、全ての結果がChannelに書き込まれるまでブロックする
	frontier, popCh, pushCh := w.startURLFrontier(ctx, conf, coordinator)
	gatherer, acCh := w.startArtifactGatherer(ctx, conf)
	crawler := w.startCrawler(ctx, conf, popCh, NewOutputPipeline(acCh, pushCh))

	for received := 0; received < expectedResults; received++ {
		if err := <-w.resultCh; err != nil {
			logger.Errorf("sub-system returned error: %v", err)
			cancel()
		}
	}

	// 各種SubSystemを終了してWorker全体も終了
	finishers := []Finisher{crawler, frontier, gatherer, coordinator}
	for _, finisher := range finishers {
		if finisher == nil {
			continue
		}

		if err := finisher.Finish(); err != nil {
			logger.Errorf("failed to finish sub-system: %v", err)
		}
	}

	logger.Info("worker is finished")
}

// ArtifactGatherer用goroutineを起動する
func (w *Worker) startArtifactGatherer(ctx context.Context, conf *Configuration) (ArtifactGatherer, chan<- interface{}) {
	ctx = SubSystemContext(ctx, "artifact-gatherer")
	inputCh := make(chan interface{}, 5)

	ag, err := conf.ArtifactGathererProvider(ctx, conf)
	if err != nil {
		w.resultCh <- err
		return nil, inputCh
	}

	// Channel越しに与えられた結果をCollectし続けるだけ
	go func() {
		for {
			select {
			case artifact := <-inputCh:
				if err := ag.Collect(artifact); err != nil {
					w.resultCh <- err
					return
				}

			case <-ctx.Done():
				w.resultCh <- nil
				return
			}
		}
	}()

	return ag, inputCh
}

// URLFrontire用goroutineを起動する
func (w *Worker) startURLFrontier(ctx context.Context, conf *Configuration, coordinator Coordinator) (URLFrontier, <-chan *www.SanitizedURL, chan<- *www.SanitizedURL) {
	ctx = SubSystemContext(ctx, "url-frontier")
	popCh := make(chan *www.SanitizedURL, 5)
	pushCh := make(chan *www.SanitizedURL, 10)

	urlFrontier, err := conf.URLFrontierProvider(ctx, conf)
	if err != nil {
		w.resultCh <- err
		return nil, popCh, pushCh
	}

	// URLFrontierのPopを回し続けるgoroutineを立ち上げる
	go func() {
		for {
			url, err := urlFrontier.Pop(ctx)
			if err != nil {
				w.resultCh <- err
				return
			}

			if url == nil {
				// Pop出来ないならしばらく待つ
				select {
				case <-time.After(100 * time.Millisecond):
				case <-ctx.Done():
					w.resultCh <- nil
					return
				}
			} else {
				// Pop出来た場合はIPアドレスレベルでロックできるか確認し、それでも問題なければChannelに書き込む(最終的にCrawlerに渡される)
				locked, err := coordinator.LockByIPAddrOf(url.Host())
				if err != nil {
					w.resultCh <- err
					return
				}

				if !locked {
					continue // TODO: IPアドレスでロックできなかったURLはとりあえず捨てている
				}

				select {
				case popCh <- url:
				case <-ctx.Done():
					w.resultCh <- nil
					return
				}
			}
		}
	}()

	// URLFrontierのPushを回し続けるgoroutineを立ち上げる
	go func() {
		for {
			select {
			case url := <-pushCh:
				if err := urlFrontier.Push(ctx, url); err != nil {
					w.resultCh <- err
					return
				}
			case <-ctx.Done():
				w.resultCh <- nil
				return
			}
		}
	}()

	return urlFrontier, popCh, pushCh
}

// Crawler用goroutineを起動する
func (w *Worker) startCrawler(ctx context.Context, conf *Configuration, popCh <-chan *www.SanitizedURL, out OutputPipeline) Crawler {
	ctx = SubSystemContext(ctx, "crawler")
	crawler, err := conf.CrawlerProvider(ctx, conf)
	if err != nil {
		w.resultCh <- err
		return nil
	}

	// URLFrontierがPopしたURLをCrawlし続ける
	go func() {
		for {
			select {
			case url := <-popCh:
				// loggerにUUIDを付ける
				id, _ := uuid.NewRandom()
				logger := LoggerFromContext(ctx)
				ctx = ContextWithLogger(ctx, logger.WithField("id", id.String()))

				if err := crawler.Crawl(ctx, url, out); err != nil {
					w.resultCh <- err
					return
				}

			case <-ctx.Done():
				w.resultCh <- nil
				return
			}
		}
	}()

	return crawler
}
