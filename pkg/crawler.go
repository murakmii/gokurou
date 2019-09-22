package pkg

import (
	"context"

	"github.com/murakmii/gokurou/pkg/html"
)

type Crawler interface {
	ResourceOwner

	Crawl(ctx context.Context, url *html.SanitizedURL, out *OutputPipeline) error
}
