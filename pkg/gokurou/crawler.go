package gokurou

import (
	"context"

	"github.com/murakmii/gokurou/pkg/gokurou/www"
)

type Crawler interface {
	ResourceOwner

	Crawl(ctx context.Context, url *www.SanitizedURL, out OutputPipeline) error
}
