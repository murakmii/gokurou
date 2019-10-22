package crawler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/murakmii/gokurou/pkg/gokurou"

	"github.com/murakmii/gokurou/pkg/gokurou/www"
)

type mockPipeline struct {
	pushed    []*gokurou.SpawnedURL
	collected []*artifact
}

func buildMockPipeline() *mockPipeline {
	return &mockPipeline{
		pushed:    make([]*gokurou.SpawnedURL, 0),
		collected: make([]*artifact, 0),
	}
}

func (p *mockPipeline) OutputArtifact(ctx context.Context, a interface{}) {
	p.collected = append(p.collected, a.(*artifact))
}

func (p *mockPipeline) OutputCollectedURL(ctx context.Context, spawned *gokurou.SpawnedURL) {
	p.pushed = append(p.pushed, spawned)
}

func buildConfiguration() *gokurou.Configuration {
	conf := gokurou.NewConfiguration(1, 1)
	conf.Options["built_in.crawler.header_ua"] = "test"
	conf.Options["built_in.crawler.primary_ua"] = "gokurou"
	conf.Options["built_in.crawler.secondary_ua"] = "google"
	return conf
}

func buildTestServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/robots.txt":
			w.Header().Set("Server", "test-server")
			_, _ = w.Write([]byte("User-Agent: gokurou\n"))
			_, _ = w.Write([]byte("Disallow: /admin"))

		case "/index.html", "/admin":
			time.Sleep(300 * time.Millisecond)
			w.Header().Set("Server", "test-server")
			_, _ = w.Write([]byte("<title>Hello, crawler</title>"))
			_, _ = w.Write([]byte("<a href='/'>"))
			_, _ = w.Write([]byte("<a href='http://www.example.com/foobar.html'>"))
			_, _ = w.Write([]byte("<a href='http://www.example.com/hogefuga.html'>"))

		case "/noindex.html":
			w.Header().Set("Server", "test-server")
			_, _ = w.Write([]byte("<meta name='robots' content='noindex' />"))

		case "/redirect":
			w.Header().Set("Server", "test-server")
			w.Header().Set("Location", "/redirect")
			w.WriteHeader(http.StatusMovedPermanently)

		case "/slowloop":
			w.Header().Set("Server", "test-server")
			w.Header().Set("Location", "/slowloop")
			time.Sleep(4 * time.Second)
			w.WriteHeader(http.StatusMovedPermanently)
		}
	}))
}

func buildTestServer2() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/robots.txt":
			w.Header().Set("Server", "test-server")
			w.Header().Set("Location", "/robots.txt")
			w.WriteHeader(http.StatusMovedPermanently)

		case "/index.html":
			w.Header().Set("Server", "test-server")
			_, _ = w.Write([]byte("<a href='/'>"))
		}
	}))
}

func TestDefaultCrawler_Crawl(t *testing.T) {
	conf := buildConfiguration()
	ctx, _ := gokurou.WorkerContext(gokurou.MustRootContext(conf), 1)
	crawler, err := BuiltInCrawlerProvider(ctx, conf)
	if err != nil {
		panic(err)
	}

	ts := buildTestServer()
	defer ts.Close()

	ts2 := buildTestServer2()
	defer ts2.Close()

	t.Run("問題なくクロールできる場合、結果を収集する", func(t *testing.T) {
		out := buildMockPipeline()
		url, _ := www.SanitizedURLFromString(ts.URL + "/index.html")

		err := crawler.Crawl(ctx, url, out)
		if err != nil {
			t.Errorf("Crawl() = %v", err)
		}

		if len(out.collected) != 1 {
			t.Errorf("Crawl() does NOT collect artifact")
		}

		art := out.collected[0]
		if art.URL != url.String() || art.StatusCode != 200 || art.Title != "Hello, crawler" || art.Server != "test-server" {
			t.Errorf("Crawl() collected invalid artifact")
		}

		if len(out.pushed) != 1 ||
			out.pushed[0].Elapsed < 0.1 || out.pushed[0].Elapsed > 0.5 ||
			len(out.pushed[0].Spawned) != 3 ||
			out.pushed[0].Spawned[0].String() != ts.URL+"/" ||
			out.pushed[0].Spawned[1].String() != "http://www.example.com/foobar.html" ||
			out.pushed[0].Spawned[2].String() != "http://www.example.com/hogefuga.html" {
			t.Errorf("Crawl() collected invalid urls")
		}
	})

	t.Run("noindexなページの場合、結果を収集しないがURLは収集する", func(t *testing.T) {
		out := buildMockPipeline()
		url, _ := www.SanitizedURLFromString(ts.URL + "/noindex.html")

		err := crawler.Crawl(ctx, url, out)
		if err != nil {
			t.Errorf("Crawl() = %v", err)
		}

		if len(out.collected) != 0 || len(out.pushed) != 1 {
			t.Errorf("Crawl() collects data from noindex page")
		}
	})

	t.Run("robots.txtでインデックスを禁止されているページの場合、結果を収集しない", func(t *testing.T) {
		out := buildMockPipeline()
		url, _ := www.SanitizedURLFromString(ts.URL + "/admin.html")

		err := crawler.Crawl(ctx, url, out)
		if err != nil {
			t.Errorf("Crawl() = %v", err)
		}

		if len(out.collected) != 0 || len(out.pushed) != 0 {
			t.Errorf("Crawl() collects data from disallowed page")
		}
	})

	t.Run("robots.txtで無限にリダイレクトする場合、中断してページを取得する", func(t *testing.T) {
		out := buildMockPipeline()
		url, _ := www.SanitizedURLFromString(ts2.URL + "/index.html")

		err := crawler.Crawl(ctx, url, out)
		if err != nil {
			t.Errorf("Crawl() = %v", err)
		}

		if len(out.collected) != 1 {
			t.Errorf("Crawl() does NOT collect artifact")
		}

		art := out.collected[0]
		if art.URL != url.String() || art.StatusCode != 200 || art.Server != "test-server" {
			t.Errorf("Crawl() collected invalid artifact")
		}

		if len(out.pushed) != 1 || len(out.pushed[0].Spawned) != 1 || out.pushed[0].Spawned[0].String() != ts2.URL+"/" {
			t.Errorf("Crawl() collected invalid urls")
		}
	})

	t.Run("ページ取得で無限にリダイレクトする場合、途中で諦める", func(t *testing.T) {
		out := buildMockPipeline()
		url, _ := www.SanitizedURLFromString(ts.URL + "/redirect")

		err := crawler.Crawl(ctx, url, out)
		if err != nil {
			t.Errorf("Crawl() = %v", err)
		}

		if len(out.collected) != 1 {
			t.Errorf("Crawl() does NOT collect artifact")
		}

		art := out.collected[0]
		if art.URL != url.String() || art.StatusCode != 301 || art.Server != "test-server" {
			t.Errorf("Crawl() collected invalid artifact")
		}
	})

	t.Run("リダイレクト込みで時間を浪費するようなフローを辿った場合、途中で諦める", func(t *testing.T) {
		out := buildMockPipeline()
		url, _ := www.SanitizedURLFromString(ts.URL + "/slowloop")

		err := crawler.Crawl(ctx, url, out)
		if err != nil {
			t.Errorf("Crawl() = %v", err)
		}

		if len(out.collected) != 0 || len(out.pushed) != 0 {
			t.Errorf("Crawl() collects data")
		}
	})
}
