package pkg

import (
	"context"

	"github.com/murakmii/gokurou/pkg/html"
)

type URLFrontier interface {
	ResourceOwner

	Push(ctx context.Context, url *html.SanitizedURL) error
	Pop(ctx context.Context) (*html.SanitizedURL, error)
	MarkAsCrawled(ctx context.Context, url *html.SanitizedURL) error
}
