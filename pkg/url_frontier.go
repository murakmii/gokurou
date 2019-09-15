package pkg

import (
	"github.com/murakmii/gokurou/pkg/html"
)

type URLFrontier interface {
	Init(conf *Configuration) error
	Push(url *html.SanitizedURL) error
	Pop() (*html.SanitizedURL, error)
	Finish() error
}
