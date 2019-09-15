package worker

import (
	"context"
	"fmt"

	"github.com/murakmii/gokurou/pkg/html"
)

// URLFrontierに対するURLのPushとPop、ArtifactCollectorに対する結果の送信を行うためのパイプライン
// 各種Channel操作がブロックされている最中にWorkerが終了する可能性があるので、
// ContextがDoneするかどうかも考慮しつつ各種Channelに対して読み書きする必要がある
type dataPipeline struct {
	artifactCh chan<- interface{}
	popCh      <-chan *html.SanitizedURL
	pushCh     chan<- *html.SanitizedURL
}

func NewDataPipeline(artifactCh chan<- interface{}, popCh <-chan *html.SanitizedURL, pushCh chan<- *html.SanitizedURL) *dataPipeline {
	return &dataPipeline{
		artifactCh: artifactCh,
		popCh:      popCh,
		pushCh:     pushCh,
	}
}

func (p *dataPipeline) putArtifact(ctx context.Context, artifact interface{}) error {
	select {
	case p.artifactCh <- artifact:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("can't put artifact. worker is shutting down")
	}
}

func (p *dataPipeline) popURL(ctx context.Context) (*html.SanitizedURL, error) {
	select {
	case url := <-p.popCh:
		return url, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("can't pop url. worker is shutting down")
	}
}

func (p *dataPipeline) pushURL(ctx context.Context, url *html.SanitizedURL) error {
	select {
	case p.pushCh <- url:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("can't push url. worker is shutting down")
	}
}
