package url_frontier

import (
	"fmt"
	"testing"

	"github.com/murakmii/gokurou/pkg/html"

	"github.com/gomodule/redigo/redis"
)

func buildPublisher() *publisher {
	conn, err := redis.DialURL("redis://localhost:11111/2")
	if err != nil {
		panic(err)
	}

	_, err = conn.Do("FLUSHALL")
	if err != nil {
		panic(err)
	}

	return &publisher{
		conn:         conn,
		totalWorkers: 10,
		published:    make(map[uint16]int),
		buffer:       make(map[uint16][]string),
		noBufUntil:   0,
		maxBuffer:    0,
	}
}

func TestPublisher_publish(t *testing.T) {
	url, err := html.SanitizedURLFromString("http://example.com")
	if err != nil {
		panic(err)
	}

	hash, err := url.HashNumber()
	if err != nil {
		panic(err)
	}

	shade := uint16(hash%10) + 1
	stream := fmt.Sprintf("url_stream_%d", shade)

	fmt.Println(stream)

	t.Run("publish回数が規定回数に満たない場合、即座にpublishする", func(t *testing.T) {
		publisher := buildPublisher()
		publisher.noBufUntil = 3
		publisher.maxBuffer = 3

		err = publisher.publish(url)

		if err != nil {
			t.Errorf("publish(%s) = %v", url.String(), err)
		}

		publishedLen, err := redis.Uint64(publisher.conn.Do("LLEN", stream))
		if err != nil {
			panic(err)
		}

		if publishedLen != 1 {
			t.Errorf("publish(%s) does NOT publish url", url)
		}

		publishedURL, err := redis.String(publisher.conn.Do("LPOP", stream))
		if publishedURL != url.String() {
			t.Errorf("publish(%s) published %s, want = %s", url.String(), publishedURL, url.String())
		}
	})

	t.Run("publish回数が規定回数に達している場合、バッファしてからpublishする", func(t *testing.T) {
		publisher := buildPublisher()
		publisher.noBufUntil = 3
		publisher.maxBuffer = 3
		publisher.published[shade] = 3

		for i := 0; i < 3; i++ {
			if err := publisher.publish(url); err != nil {
				t.Errorf("publish(%s) = %v", url.String(), err)
				break
			}
		}

		publishedLen, err := redis.Uint64(publisher.conn.Do("LLEN", stream))
		if err != nil {
			panic(err)
		}

		if publishedLen != 1 {
			t.Errorf("publish(%s) does NOT publish url", url)
		}

		publishedURL, err := redis.String(publisher.conn.Do("LPOP", stream))
		want := fmt.Sprintf("%s\t%s\t%s", url.String(), url.String(), url.String())
		if publishedURL != want {
			t.Errorf("publish(%s)*3 published %s, want = %s", url.String(), publishedURL, want)
		}
	})
}
