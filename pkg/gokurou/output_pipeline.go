package gokurou

import (
	"context"

	"github.com/murakmii/gokurou/pkg/gokurou/www"
)

type OutputPipeline interface {
	OutputArtifact(ctx context.Context, artifact interface{})
	OutputCollectedURL(ctx context.Context, url *www.SanitizedURL)
}

type outputPipeline struct {
	artifactCh chan<- interface{}
	pushCh     chan<- *www.SanitizedURL
}

func NewOutputPipeline(artifactCh chan<- interface{}, pushCh chan<- *www.SanitizedURL) OutputPipeline {
	return &outputPipeline{
		artifactCh: artifactCh,
		pushCh:     pushCh,
	}
}

func (out *outputPipeline) OutputArtifact(ctx context.Context, artifact interface{}) {
	select {
	case out.artifactCh <- artifact:
	case <-ctx.Done():
	}
}

func (out *outputPipeline) OutputCollectedURL(ctx context.Context, url *www.SanitizedURL) {
	select {
	case out.pushCh <- url:
	case <-ctx.Done():
	}
}
