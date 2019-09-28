package url_frontier

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/murakmii/gokurou/pkg/gokurou"

	"github.com/murakmii/gokurou/pkg/gokurou/www"

	"github.com/google/uuid"
)

func buildContext() context.Context {
	return gokurou.ContextWithGWN(context.Background(), uint16(1))
}

func buildURLFrontier(ctx context.Context) *builtInURLFrontier {
	conf := gokurou.NewConfiguration(10)
	conf.Workers = 1
	conf.Machines = 1
	conf.Advanced["URL_FRONTIER_SHARED_DB_SOURCE"] = "root:gokurou1234@tcp(127.0.0.1:11112)/gokurou_test?charset=utf8mb4,utf&interpolateParams=true"
	conf.Advanced["URL_FRONTIER_LOCAL_DB_PATH_PROVIDER"] = func(_ uint16) string { return ":memory:" }

	f, err := BuiltInURLFrontierProvider(ctx, conf)
	if err != nil {
		panic(err)
	}

	frontier := f.(*builtInURLFrontier)
	if _, err = frontier.sharedDB.Exec("TRUNCATE urls"); err != nil {
		panic(err)
	}

	return frontier
}

func buildRandomHostURL() *www.SanitizedURL {
	u, err := uuid.NewRandom()
	if err != nil {
		panic(err)
	}

	url, err := www.SanitizedURLFromString(fmt.Sprintf("http://example-%s.com", u.String()))
	if err != nil {
		panic(err)
	}

	return url
}

func TestDefaultURLFrontier_Push(t *testing.T) {
	ctx := buildContext()
	frontier := buildURLFrontier(ctx)

	defer frontier.Finish()

	t.Run("十分にPushしたことがない場合、即座にPushする", func(t *testing.T) {
		url := buildRandomHostURL()
		if err := frontier.Push(ctx, url); err != nil {
			t.Errorf("Push(%s) = %v", url, err)
		}

		var pushed string
		err := frontier.sharedDB.QueryRow("SELECT tab_joined_url FROM urls WHERE id = (SELECT MAX(id) FROM urls)").Scan(&pushed)
		if err != nil {
			panic(err)
		}

		if pushed != url.String() {
			t.Errorf("Push(%s) does NOT push valid url(%s)", url, pushed)
		}
	})

	t.Run("十分にPushしている場合、バッファしてからPushする", func(t *testing.T) {
		frontier.pushedCount[1] = 99
		want := make([]string, 50)
		for i := 1; i <= 50; i++ {
			url := buildRandomHostURL()
			want[i-1] = url.String()

			if err := frontier.Push(ctx, url); err != nil {
				t.Errorf("Push(%s) = %v", url, err)
				break
			}
		}

		var pushed string
		err := frontier.sharedDB.QueryRow("SELECT tab_joined_url FROM urls WHERE id = (SELECT MAX(id) FROM urls)").Scan(&pushed)
		if err != nil {
			panic(err)
		}

		if pushed != strings.Join(want, "\t") {
			t.Errorf("Push([URL]) does NOT push tab joined url(%s)", pushed)
		}
	})
}

func TestDefaultURLFrontier_Pop(t *testing.T) {
	ctx := buildContext()
	frontier := buildURLFrontier(ctx)

	defer frontier.Finish()

	t.Run("PopするURLがない場合、nilを返す", func(t *testing.T) {
		url, err := frontier.Pop(ctx)
		if err != nil {
			t.Errorf("Pop() = %v", err)
		}

		if url != nil {
			t.Errorf("Pop() = %v, want = nil", url)
		}
	})

	t.Run("PopするURLがある場合、それを返す", func(t *testing.T) {
		url := buildRandomHostURL()
		if err := frontier.Push(ctx, url); err != nil {
			panic(err)
		}

		poppedURL, err := frontier.Pop(ctx)
		if err != nil {
			t.Errorf("Pop() = %v", err)
		}

		if poppedURL == nil || poppedURL.String() != url.String() {
			t.Errorf("Pop() = %s, want = %s", poppedURL, url)
		}
	})

	t.Run("PopしたURLがクロール済みのものだった場合、次のURLを返す", func(t *testing.T) {
		urls := []*www.SanitizedURL{buildRandomHostURL(), buildRandomHostURL()}
		for _, url := range urls {
			if err := frontier.Push(ctx, url); err != nil {
				panic(err)
			}
		}

		if _, err := frontier.localDB.Exec("INSERT INTO crawled_hosts VALUES(?)", urls[0].Host()); err != nil {
			panic(err)
		}

		poppedURL, err := frontier.Pop(ctx)
		if err != nil {
			t.Errorf("Pop() = %v", err)
		}

		if poppedURL == nil || poppedURL.String() != urls[1].String() {
			t.Errorf("Pop() = %s, want = %s", poppedURL, urls[1].String())
		}
	})
}

func TestDefaultURLFrontier_Finish(t *testing.T) {
	ctx := buildContext()
	frontier := buildURLFrontier(ctx)

	if err := frontier.Finish(); err != nil {
		t.Errorf("Finish() = %v", err)
	}
}
