package gokurou

import (
	"context"
	"sync"

	"golang.org/x/xerrors"

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

	// クロール中に発生したデータをリセットし、次のクロール開始に備える。Finish相当の初期化処理も同時に行うこと
	Reset() error
}

// あるページから発生したURLを表す型
type SpawnedURL struct {
	From    *www.SanitizedURL
	Elapsed float64
	Spawned []*www.SanitizedURL
}

// クロール対象となるURLの集合を扱うための実装を要求するinterface
type URLFrontier interface {
	Finisher

	// 初期URLの設定をサポートする TODO: 配列で受け取る方が便利
	Seeding(ctx context.Context, url []string) error

	// URLの集合に対してURLを追加する
	Push(ctx context.Context, spawnedURL *SpawnedURL) error

	// URLの集合からURLを1つ取り出す
	Pop(ctx context.Context) (*www.SanitizedURL, error)

	// クロール中に発生したデータをリセットし、次のクロール開始に備える。Finish相当の初期化処理も同時に行うこと
	Reset() error
}

// クロール中に得られた結果の収集処理の実装を要求するinterface
type ArtifactGatherer interface {
	Finisher

	// 結果を収集する。引数の解釈は実装依存で良い
	Collect(ctx context.Context, artifact interface{}) error
}

// クロール中の動作状況をトレースするトレーサーの実装を要求するinterface
// トレーサーはWorker毎ではなく1プロセス中でただ1つのトレーサーを用いてトレースを行うため、 競合状態に注意すること
type Tracer interface {
	Finisher

	// 1クロール完了するごとに呼び出される
	TraceCrawled(ctx context.Context, err error)

	// 1 HTTP GET完了するごとに呼び出される
	TraceGetRequest(ctx context.Context, elaspsed float64)

	// 1 Pop完了するごとに呼び出される
	TracePop(ctx context.Context, elapsed float64)
}

// 何もしないデフォルトのトレーサーを実装しておく
type NullTracer struct{}

func NewNullTracer() Tracer                                       { return NullTracer{} }
func (t NullTracer) TraceCrawled(_ context.Context, _ error)      {}
func (t NullTracer) TraceGetRequest(_ context.Context, _ float64) {}
func (t NullTracer) TracePop(_ context.Context, _ float64)        {}
func (t NullTracer) Finish() error                                { return nil }

// クロールの実装を要求するinterface
type Crawler interface {
	Finisher

	// 与えられたURLについてクロールする
	// このURLで指定される対象と、関連するrobots.txt以外にはアクセスしないこと
	// 得られた結果はOutputPipelineを通じて外部に送信する
	Crawl(ctx context.Context, url *www.SanitizedURL, out OutputPipeline) error
}

// 初期URLを設定する
func Seeding(conf *Configuration, urls []string) error {
	ctx, err := contextGWN1(conf)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	frontier, err := conf.URLFrontierProvider(ctx, conf)
	if err != nil {
		return err
	}

	if err = frontier.Seeding(ctx, urls); err != nil {
		return err
	}

	cancel()
	return frontier.Finish()
}

// 指定の設定に基づいてクロールを開始する
func Start(conf *Configuration) error {
	ctx, err := RootContext(conf)
	if err != nil {
		return err
	}

	wg := &sync.WaitGroup{}

	for i := uint(0); i < conf.Workers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			NewWorker().Start(ctx, conf)
		}()
	}

	wg.Wait()
	return TracerFromContext(ctx).Finish()
}

// クロール中に生じたデータのリセットを実行する(成果物を除く)
func Reset(conf *Configuration) error {
	coordinator, err := conf.CoordinatorProvider(conf)
	if err != nil {
		return xerrors.Errorf("failed to setup coordinator: %v", err)
	}

	if err = coordinator.Reset(); err != nil {
		return xerrors.Errorf("failed to reset by coordinator: %v", err)
	}

	ctx, err := contextGWN1(conf)
	if err != nil {
		return err
	}

	frontier, err := conf.URLFrontierProvider(ctx, conf)
	if err != nil {
		return xerrors.Errorf("failed to setup url frontier: %v", err)
	}

	if err = frontier.Reset(); err != nil {
		return xerrors.Errorf("failed to reset by frontier: %v", err)
	}

	return nil
}

func contextGWN1(conf *Configuration) (context.Context, error) {
	ctx, err := RootContext(conf)
	if err != nil {
		return nil, err
	}

	ctx, _ = WorkerContext(ctx, 1)
	return ctx, nil
}
