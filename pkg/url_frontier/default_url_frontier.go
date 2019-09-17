package url_frontier

import (
	"context"
	"fmt"
	"strings"

	"github.com/gomodule/redigo/redis"
	"github.com/murakmii/gokurou/pkg"
	"github.com/murakmii/gokurou/pkg/html"
)

type defaultURLFrontier struct {
	pub *publisher
	sub *subscriber
}

type publisher struct {
	conn         redis.Conn
	totalWorkers uint16
	published    map[uint16]int
	buffer       map[uint16][]string
	noBufUntil   int
	maxBuffer    int
}

// 1. BLPOP+DBへのURL書き込みをループし続けるgoroutineを立ち上げる
// 2. popの際は、DBに書き込まれたURLを1つ返す
type subscriber struct {
	conn  redis.Conn
	myGWN uint16
}

func NewDefaultURLFrontier(ctx context.Context, conf *pkg.Configuration) (pkg.URLFrontier, error) {
	pub, err := newPublisher(ctx, conf)
	if err != nil {
		return nil, err
	}

	return &defaultURLFrontier{
		pub: pub,
	}, nil
}

func (frontier *defaultURLFrontier) Push(url *html.SanitizedURL) error {
	return frontier.pub.publish(url)
}

func (frontier *defaultURLFrontier) Pop() (*html.SanitizedURL, error) {
	return nil, nil
}

func (frontier *defaultURLFrontier) Finish() error {
	// TODO: publisherのバッファに残ったデータを吐き出す
	return nil
}

func newPublisher(_ context.Context, conf *pkg.Configuration) (*publisher, error) {
	redisURL, err := conf.FetchAdvancedAsString("REDIS_URL")
	if err != nil {
		return nil, err
	}

	conn, err := redis.DialURL(redisURL)
	if err != nil {
		return nil, err
	}

	return &publisher{
		conn:         conn,
		totalWorkers: conf.TotalWorkers(),
		published:    make(map[uint16]int),
		buffer:       make(map[uint16][]string),
		noBufUntil:   100,
		maxBuffer:    100,
	}, nil
}

func (p *publisher) publish(url *html.SanitizedURL) error {
	hash, err := url.HashNumber()
	if err != nil {
		return err
	}

	shade := uint16(hash%uint32(p.totalWorkers)) + 1

	p.buffer[shade] = append(p.buffer[shade], url.String())
	p.published[shade]++

	if p.published[shade] < p.noBufUntil || len(p.buffer[shade]) >= p.maxBuffer {
		return p.publishShade(shade)
	}

	return nil
}

func (p *publisher) publishShade(shade uint16) error {
	data := strings.Join(p.buffer[shade], "\t")

	_, err := p.conn.Do("RPUSH", fmt.Sprintf("url_stream_%d", shade), data)
	if err != nil {
		return err
	}

	p.buffer[shade] = make([]string, 0, 100)

	return nil
}
