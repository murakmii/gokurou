package pkg

// クロールにおける結果収集の実装の抽象
type ArtifactCollector interface {
	// 結果を収集する
	Collect(artifact interface{}) error

	// クロール終了時に1度だけ呼び出される
	Finish() error
}
