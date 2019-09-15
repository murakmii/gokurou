package url_frontier

import (
	"github.com/gomodule/redigo/redis"
	"github.com/murakmii/gokurou/pkg"
	"github.com/murakmii/gokurou/pkg/html"
)

type defaultURLFrontier struct {
	pub *publisher
}

type publisher struct {
	redis  redis.Conn
	sent   map[int]int
	buffer map[int][]string
}

type packedURL struct {
	urls []string
}

func NewDefaultURLFrontier() pkg.URLFrontier {
	return &defaultURLFrontier{
		pub: &publisher{
			redis:  nil,
			sent:   make(map[int]int),
			buffer: make(map[int][]string),
		},
	}
}

func (frontier *defaultURLFrontier) Init(conf *pkg.Configuration) error {
	return nil
}

func (frontier *defaultURLFrontier) Push(url *html.SanitizedURL) error {
	return nil
}

func (frontier *defaultURLFrontier) Pop() (*html.SanitizedURL, error) {
	return nil, nil
}

func (frontier *defaultURLFrontier) Finish() error {
	return nil
}
