package gokurou

// クロールにおける結果収集の実装の抽象
type ArtifactGatherer interface {
	ResourceOwner

	// 結果を収集する
	Collect(artifact interface{}) error
}
