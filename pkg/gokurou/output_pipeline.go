package gokurou

import (
	"context"

	"github.com/murakmii/gokurou/pkg/gokurou/www"
)

// クロールして得られた結果の収集を行うためのパイプラインの実装を要求するinterface
type OutputPipeline interface {
	// 任意の結果の収集。ここで与えられた結果がArtifactGathererに渡される
	OutputArtifact(ctx context.Context, artifact interface{})

	// 次にクロールするべきURLの収集。ここで与えられたURLがURLFrontierに渡される
	OutputCollectedURL(ctx context.Context, url *www.SanitizedURL)
}

// OutputPipelineの実装
type outputPipelineImpl struct {
	artifactCh chan<- interface{}
	pushCh     chan<- *www.SanitizedURL
}

func NewOutputPipeline(artifactCh chan<- interface{}, pushCh chan<- *www.SanitizedURL) OutputPipeline {
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

func (out *outputPipelineImpl) OutputCollectedURL(ctx context.Context, url *www.SanitizedURL) {
	select {
	case out.pushCh <- url:
	case <-ctx.Done():
	}
}
