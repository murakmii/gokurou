package gokurou

import (
	"context"
)

// クロールして得られた結果の収集を行うためのパイプラインの実装を要求するinterface
type OutputPipeline interface {
	// 任意の結果の収集。ここで与えられた結果がArtifactGathererに渡される
	OutputArtifact(ctx context.Context, artifact interface{})

	// クロールにより発生したURLの収集。ここで与えられたURLがURLFrontierに渡される
	OutputCollectedURL(ctx context.Context, spawned *SpawnedURL)
}

// OutputPipelineの実装
type outputPipelineImpl struct {
	artifactCh chan<- interface{}
	pushCh     chan<- *SpawnedURL
}

func NewOutputPipeline(artifactCh chan<- interface{}, pushCh chan<- *SpawnedURL) OutputPipeline {
	return &outputPipelineImpl{
		artifactCh: artifactCh,
		pushCh:     pushCh,
	}
}

func (out *outputPipelineImpl) OutputArtifact(ctx context.Context, artifact interface{}) {
	select {
	case out.artifactCh <- artifact:
	case <-ctx.Done():
	}
}

func (out *outputPipelineImpl) OutputCollectedURL(ctx context.Context, spawned *SpawnedURL) {
	select {
	case out.pushCh <- spawned:
	case <-ctx.Done():
	}
}
