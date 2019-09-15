package pkg

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// 指定の設定に基づいてクロールを開始する
func Start(conf *Configuration) {
	ctx := buildContext()
	wg := &sync.WaitGroup{}

	for i := uint16(0); i < conf.Workers; i++ {
		wg.Add(1)
		NewWorker().Start(ctx, wg, conf, i+1)
	}

	wg.Wait()
}

// クロール中に扱う全Contextの親となるContextを作る。このContextがDoneしたならクロールを終了する
func buildContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())

	// いくつかのシグナルを受信したらクロールを終了する
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGKILL, syscall.SIGINT)

		select {
		case <-sigCh:
			cancel()
		}
	}()

	return ctx
}
