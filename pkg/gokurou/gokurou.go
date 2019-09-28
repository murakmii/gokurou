package gokurou

import (
	"context"
	"sync"

	"github.com/murakmii/gokurou/pkg/gokurou/www"
)

// 何らかの終了処理表すFinishメソッドの実装を要求するinterface
type Finisher interface {
	Finish() error
}

// クロール中にどうしても必要な、他のworkerとの協調処理の実装を要求するinterface
type Coordinator interface {
	Finisher

	// 全worker中でユニークなworker番号(GWN = global worker number)を割り当てる
	// 割り当てられる番号は、呼び出し時点で存在している全worker数より1だけ大きい番号であること
	AllocNextGWN() (uint16, error)

	// 与えられたホスト名を解決して得られるIPアドレスについて、一定時間ロックする。ロックを獲得できた場合にtrueを返すこと
	// (同様のIPアドレスが得られるホスト名を引数とする他のLockByIPAddrOf呼び出しが、一定時間内はfalseを返すようにすること)
	LockByIPAddrOf(host string) (bool, error)
}

// クロール対象となるURLの集合を扱うための実装を要求するinterface
type URLFrontier interface {
	Finisher

	// URLの集合に対してURLを追加する
	Push(ctx context.Context, url *www.SanitizedURL) error

	// URLの集合からURLを1つ取り出す
	Pop(ctx context.Context) (*www.SanitizedURL, error)
}

// クロール中に得られた結果の収集処理の実装を要求するinterface
type ArtifactGatherer interface {
	Finisher

	// 結果を収集する。引数の解釈は実装依存で良い
	Collect(artifact interface{}) error
}

// クロールの実装を要求するinterface
type Crawler interface {
	Finisher

	// 与えられたURLについてクロールする
	// このURLで指定される対象と、関連するrobots.txtにはアクセスしないこと
	// 得られた結果はOutputPipelineを通じて外部に送信する
	Crawl(ctx context.Context, url *www.SanitizedURL, out OutputPipeline) error
}

// 指定の設定に基づいてクロールを開始する
func Start(conf *Configuration) {
	ctx := RootContext()
	wg := &sync.WaitGroup{}

	for i := uint(0); i < conf.Workers; i++ {
		wg.Add(1)
		NewWorker().Start(ctx, wg, conf)
	}

	wg.Wait()
}
