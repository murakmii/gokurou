package url_frontier

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/murakmii/gokurou/pkg/gokurou/www"

	"github.com/gomodule/redigo/redis"

	"github.com/murakmii/gokurou/pkg/gokurou"
)

func buildRedisPubSubURLFrontier(ctx context.Context, url ...string) *redisPubSubURLFrontier {
	conf := gokurou.NewConfiguration(1, 1)
	conf.Options["redis_pub_sub_url_frontier.redis_url"] = "redis://localhost:11111/1"
	conf.Options["redis_pub_sub_url_frontier.local_db_path"] = nil

	r, err := redis.DialURL("redis://localhost:11111/1")
	if err != nil {
		panic(err)
	}
	if _, err := r.Do("FLUSHALL"); err != nil {
		panic(err)
	}
	for _, u := range url {
		if _, err := r.Do("RPUSH", "url_stream_1", u); err != nil {
			panic(err)
		}
	}
	_ = r.Close()

	f, err := NewRedisPubSubURLFrontierProvider(ctx, conf)
	if err != nil {
		panic(err)
	}

	return f.(*redisPubSubURLFrontier)
}

func TestRedisPubSubURLFrontier_subscribeLoop(t *testing.T) {
	ctx := buildContext()

	t.Run("対象ホストを初めてPOPする場合、ホストの情報と共にURLをDBに保存する", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		f := buildRedisPubSubURLFrontier(ctx, "http://www.example.com/path/to/page")
		time.Sleep(2500 * time.Millisecond)
		cancel()
		err := <-f.subCh
		if err != nil {
			t.Errorf("subscribeLoop() = %v", err)
		}
		f.subCh <- err

		var id int64
		if err := f.localDB.QueryRow("SELECT 1 FROM hosts WHERE host = 'www.example.com'").Scan(&id); err != nil {
			t.Errorf("subscribeLoop() = %v", err)
		}
		if id != 1 {
			t.Errorf("subscribeLoop() does NOT save host")
		}

		var urlCount int64
		if err := f.localDB.QueryRow("SELECT COUNT(*) FROM urls WHERE host_id = ?", id).Scan(&urlCount); err != nil {
			t.Errorf("subscribeLoop() = %v", err)
		}
		if urlCount != 1 {
			t.Errorf("subscribeLoop() does NOT save host")
		}

		var url string
		if err := f.localDB.QueryRow("SELECT url FROM urls WHERE host_id = ?", id).Scan(&url); err != nil {
			t.Errorf("subscribeLoop() = %v", err)
		}

		if url != "http://www.example.com/path/to/page" {
			t.Errorf("subscribeLoop() does NOT save url")
		}

		if err := f.Finish(); err != nil {
			panic(err)
		}
	})

	t.Run("対象ホストを既にPOP済みの場合、URLのみをDBに保存する", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		f := buildRedisPubSubURLFrontier(ctx, "http://www.example.com/path/to/page")
		if _, err := f.localDB.Exec("INSERT INTO hosts(host, crawlable_at) VALUES('www.example.com', 123)"); err != nil {
			panic(err)
		}
		time.Sleep(2500 * time.Millisecond)
		cancel()
		err := <-f.subCh
		if err != nil {
			t.Errorf("subscribeLoop() = %v", err)
		}
		f.subCh <- err

		var urlCount int64
		if err := f.localDB.QueryRow("SELECT COUNT(*) FROM urls WHERE host_id = 1").Scan(&urlCount); err != nil {
			t.Errorf("subscribeLoop() = %v", err)
		}
		if urlCount != 1 {
			t.Errorf("subscribeLoop() does NOT save host")
		}

		var url string
		if err := f.localDB.QueryRow("SELECT url FROM urls WHERE host_id = 1").Scan(&url); err != nil {
			t.Errorf("subscribeLoop() = %v", err)
		}

		if url != "http://www.example.com/path/to/page" {
			t.Errorf("subscribeLoop() does NOT save url")
		}

		if err := f.Finish(); err != nil {
			panic(err)
		}
	})

	t.Run("対象ホストについてURLを十分保存済みの場合、それ以上保存しない", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		f := buildRedisPubSubURLFrontier(ctx, "http://www.example.com/path/to/page")
		if _, err := f.localDB.Exec("INSERT INTO hosts(host, crawlable_at) VALUES('www.example.com', 123)"); err != nil {
			panic(err)
		}
		for i := 1; i <= maxURLPerHost; i++ {
			url := fmt.Sprintf("http://www.example.com/path/%d", i)
			if _, err := f.localDB.Exec("INSERT INTO urls(host_id, url, crawled) VALUES(1, ?, 0)", url); err != nil {
				panic(err)
			}
		}

		time.Sleep(2500 * time.Millisecond)
		cancel()
		err := <-f.subCh
		if err != nil {
			t.Errorf("subscribeLoop() = %v", err)
		}
		f.subCh <- err

		var urlCount int64
		if err := f.localDB.QueryRow("SELECT COUNT(*) FROM urls WHERE host_id = 1").Scan(&urlCount); err != nil {
			t.Errorf("subscribeLoop() = %v", err)
		}
		if urlCount != maxURLPerHost {
			t.Errorf("subscribeLoop() saved urls = %d, want = %d", urlCount, maxURLPerHost)
		}

		if err := f.Finish(); err != nil {
			panic(err)
		}
	})

	t.Run("対象ホストについて同じパスのURLを保存済みの場合、それ以上保存しない", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		f := buildRedisPubSubURLFrontier(ctx, "http://www.example.com/same")
		if _, err := f.localDB.Exec("INSERT INTO hosts(host, crawlable_at) VALUES('www.example.com', 123)"); err != nil {
			panic(err)
		}
		url := "http://www.example.com/same"
		if _, err := f.localDB.Exec("INSERT INTO urls(host_id, url, crawled) VALUES(1, ?, 0)", url); err != nil {
			panic(err)
		}

		time.Sleep(2500 * time.Millisecond)
		cancel()
		err := <-f.subCh
		if err != nil {
			t.Errorf("subscribeLoop() = %v", err)
		}
		f.subCh <- err

		var urlCount int64
		if err := f.localDB.QueryRow("SELECT COUNT(*) FROM urls WHERE host_id = 1").Scan(&urlCount); err != nil {
			t.Errorf("subscribeLoop() = %v", err)
		}
		if urlCount != 1 {
			t.Errorf("subscribeLoop() saved urls = %d, want = %d", urlCount, 1)
		}

		if err := f.Finish(); err != nil {
			panic(err)
		}
	})
}

func TestRedisPubSubURLFrontier_Push(t *testing.T) {
	ctx := buildContext()

	t.Run("取り扱い可能なURLの場合、Publishする", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		cancel()
		f := buildRedisPubSubURLFrontier(ctx)

		err := f.Push(ctx, &gokurou.SpawnedURL{
			From:    mustURL("http://example.com"),
			Elapsed: 0.100,
			Spawned: []*www.SanitizedURL{
				mustURL("http://www.example.com"),
			},
		})
		if err != nil {
			t.Errorf("Push() = %v", err)
		}

		l, err := redis.Int(f.pub.Do("LLEN", "url_stream_1"))
		if err != nil {
			panic(err)
		}

		if l != 1 {
			t.Errorf("Push() pushes url = %d, want = 1", l)
		}

		if err := f.Finish(); err != nil {
			panic(err)
		}
	})

	t.Run("取り扱い不可なURLの場合、Publishしない", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		cancel()
		f := buildRedisPubSubURLFrontier(ctx)
		f.tldFilter = []string{"com"}

		err := f.Push(ctx, &gokurou.SpawnedURL{
			From:    mustURL("http://example.com"),
			Elapsed: 0.100,
			Spawned: []*www.SanitizedURL{
				mustURL("http://www.example.io"),
			},
		})
		if err != nil {
			t.Errorf("Push() = %v", err)
		}

		l, err := redis.Int(f.pub.Do("LLEN", "url_stream_1"))
		if err != nil {
			panic(err)
		}

		if l != 0 {
			t.Errorf("Push() pushes url = %d, want = 0", l)
		}

		if err := f.Finish(); err != nil {
			panic(err)
		}
	})
}

func TestRedisPubSubURLFrontier_Pop(t *testing.T) {
	ctx := buildContext()

	t.Run("何もPushされていない場合、nilを返す", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		f := buildRedisPubSubURLFrontier(ctx)

		url, err := f.Pop(ctx)
		if err != nil {
			t.Errorf("Pop() = %v", err)
		}

		if url != nil {
			t.Errorf("Pop() = %s, want = nil", url)
		}

		cancel()
		if err := f.Finish(); err != nil {
			panic(err)
		}
	})

	t.Run("Pushされている場合、そのURLを返す", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		f := buildRedisPubSubURLFrontier(ctx)

		spawned := &gokurou.SpawnedURL{
			From:    mustURL("http://example.com"),
			Elapsed: 0.100,
			Spawned: []*www.SanitizedURL{mustURL("http://www.example.com/index.html")},
		}

		if err := f.Push(ctx, spawned); err != nil {
			panic(err)
		}
		time.Sleep(100 * time.Millisecond)

		url, err := f.Pop(ctx)
		if err != nil {
			t.Errorf("Pop() = %v", err)
		}

		if url.String() != "http://www.example.com/index.html" {
			t.Errorf("Pop() = %s, want = 'http://www.example.com/index.html'", url)
		}

		var crawled int64
		if err := f.localDB.QueryRow("SELECT crawled FROM urls LIMIT 1").Scan(&crawled); err != nil {
			panic(err)
		}

		if crawled != 1 {
			t.Errorf("Pop() does NOT mark url as crawled")
		}

		var crawlableAt int64
		if err := f.localDB.QueryRow("SELECT crawlable_at FROM hosts WHERE id = 1").Scan(&crawlableAt); err != nil {
			panic(err)
		}

		if crawlableAt < time.Now().Unix()+110 {
			t.Errorf("Pop() does NOT update next crawlable time")
		}

		cancel()
		if err := f.Finish(); err != nil {
			panic(err)
		}
	})

	t.Run("1ホストあたりの上限回数までPopした場合、2度とPopされないような時刻を次回Pop時刻に設定する", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		f := buildRedisPubSubURLFrontier(ctx)

		for i := 1; i <= maxURLPerHost-1; i++ {
			url := fmt.Sprintf("http://www.example.com/page%d", i)
			if _, err := f.localDB.Exec("INSERT INTO urls(host_id, url, crawled) VALUES(1, ?, 1)", url); err != nil {
				panic(err)
			}
		}

		spawned := &gokurou.SpawnedURL{
			From:    mustURL("http://example.com"),
			Elapsed: 0.100,
			Spawned: []*www.SanitizedURL{mustURL("http://www.example.com/index.html")},
		}

		if err := f.Push(ctx, spawned); err != nil {
			panic(err)
		}
		time.Sleep(100 * time.Millisecond)

		url, err := f.Pop(ctx)
		if err != nil {
			t.Errorf("Pop() = %v", err)
		}

		if url.String() != "http://www.example.com/index.html" {
			t.Errorf("Pop() = %s, want = 'http://www.example.com/index.html'", url)
		}

		var crawlableAt int64
		if err := f.localDB.QueryRow("SELECT crawlable_at FROM hosts WHERE id = 1").Scan(&crawlableAt); err != nil {
			panic(err)
		}

		if crawlableAt < time.Now().Unix()+(24*3600*364) {
			t.Errorf("Pop() does NOT update next crawlable time")
		}

		cancel()
		if err := f.Finish(); err != nil {
			panic(err)
		}
	})
}
