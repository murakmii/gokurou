package pkg

import (
	"context"

	"github.com/murakmii/gokurou/pkg/html"
)

type OutputPipeline struct {
	artifactCh chan<- interface{}
	pushCh     chan<- *html.SanitizedURL
}

func NewOutputPipeline(artifactCh chan<- interface{}, pushCh chan<- *html.SanitizedURL) *OutputPipeline {
	return &OutputPipeline{
		artifactCh: artifactCh,
		pushCh:     pushCh,
	}
}

func (out *OutputPipeline) OutputArtifact(ctx context.Context, artifact interface{}) {
	select {
	case out.artifactCh <- artifact:
	case <-ctx.Done():
	}
}

func (out *OutputPipeline) OutputCollectedURL(ctx context.Context, url *html.SanitizedURL) {
	select {
	case out.pushCh <- url:
	case <-ctx.Done():
	}
}
