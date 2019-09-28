package gokurou

import (
	"context"

	"github.com/murakmii/gokurou/pkg/gokurou/www"
)

type URLFrontier interface {
	Finisher

	Push(ctx context.Context, url *www.SanitizedURL) error
	Pop(ctx context.Context) (*www.SanitizedURL, error)
}
