package pkg

import (
	"context"

	"github.com/murakmii/gokurou/pkg/html"
)

type OutputPipeline interface {
	OutputArtifact(ctx context.Context, artifact interface{})
	OutputCollectedURL(ctx context.Context, url *html.SanitizedURL)
}

type outputPipeline struct {
	artifactCh chan<- interface{}
	pushCh     chan<- *html.SanitizedURL
}

func NewOutputPipeline(artifactCh chan<- interface{}, pushCh chan<- *html.SanitizedURL) OutputPipeline {
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

func (out *outputPipeline) OutputCollectedURL(ctx context.Context, url *html.SanitizedURL) {
	select {
	case out.pushCh <- url:
	case <-ctx.Done():
	}
}
