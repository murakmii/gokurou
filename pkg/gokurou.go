package pkg

import (
	"sync"
)

// 指定の設定に基づいてクロールを開始する
func Start(conf *Configuration) {
	ctx := RootContext()
	wg := &sync.WaitGroup{}

	for i := uint16(0); i < conf.Workers; i++ {
		wg.Add(1)
		NewWorker().Start(ctx, wg, conf)
	}

	wg.Wait()
}
