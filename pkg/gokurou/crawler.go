package gokurou

import (
	"context"

	"github.com/murakmii/gokurou/pkg/gokurou/www"
)

type Crawler interface {
	Finisher

	Crawl(ctx context.Context, url *www.SanitizedURL, out OutputPipeline) error
}
