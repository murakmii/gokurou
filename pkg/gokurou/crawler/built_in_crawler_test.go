package crawler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/murakmii/gokurou/pkg/gokurou"

	"github.com/murakmii/gokurou/pkg/gokurou/www"
)

type mockPipeline struct {
	pushed    []*www.SanitizedURL
	collected [][]byte
}

func buildMockPipeline() *mockPipeline {
	return &mockPipeline{
		pushed:    make([]*www.SanitizedURL, 0),
		collected: make([][]byte, 0),
	}
}

func (p *mockPipeline) OutputArtifact(ctx context.Context, artifact interface{}) {
	p.collected = append(p.collected, artifact.([]byte))
}

func (p *mockPipeline) OutputCollectedURL(ctx context.Context, url *www.SanitizedURL) {
	p.pushed = append(p.pushed, url)
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
		w.Header().Set("Server", "test-server")

		switch r.URL.Path {
		case "/robots.txt":
			_, _ = w.Write([]byte("Disallow: /admin"))

		case "/index.html", "/admin":
			_, _ = w.Write([]byte("<title>Hello, crawler</title>"))
			_, _ = w.Write([]byte("<a href='/'>"))
			_, _ = w.Write([]byte("<a href='http://www.example.com/foobar.html'>"))
			_, _ = w.Write([]byte("<a href='http://www.example.com/hogefuga.html'>"))

		case "/noindex.html":
			_, _ = w.Write([]byte("<meta name='robots' content='noindex' />"))
		}
	}))
}

func TestDefaultCrawler_Crawl(t *testing.T) {
	conf := buildConfiguration()
	ctx := gokurou.RootContext(conf)
	crawler, err := BuiltInCrawlerProvider(ctx, conf)
	if err != nil {
		panic(err)
	}

	ts := buildTestServer()
	defer ts.Close()

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

		art := &artifact{}
		if err := json.Unmarshal(out.collected[0], art); err != nil {
			t.Errorf("Crawl() collected invalid artifact")
		}

		if art.URL != url.String() || art.StatusCode != 200 || art.Title != "Hello, crawler" || art.Server != "test-server" {
			t.Errorf("Crawl() collected invalid artifact")
		}

		if len(out.pushed) != 1 || out.pushed[0].String() != "http://www.example.com/foobar.html" {
			t.Errorf("Crawl() collected invalid urls")
		}
	})

	t.Run("noindexなページの場合、結果を収集しない", func(t *testing.T) {
		out := buildMockPipeline()
		url, _ := www.SanitizedURLFromString(ts.URL + "/noindex.html")

		err := crawler.Crawl(ctx, url, out)
		if err != nil {
			t.Errorf("Crawl() = %v", err)
		}

		if len(out.collected) != 0 || len(out.pushed) != 0 {
			t.Errorf("Crawl() collects data from noindex page")
		}
	})

	t.Run("robots.txtでインデックスを禁止されているページの場合、結果を収集しない", func(t *testing.T) {
		out := buildMockPipeline()
		url, _ := www.SanitizedURLFromString(ts.URL + "/noindex.html")

		err := crawler.Crawl(ctx, url, out)
		if err != nil {
			t.Errorf("Crawl() = %v", err)
		}

		if len(out.collected) != 0 || len(out.pushed) != 0 {
			t.Errorf("Crawl() collects data from disallowed page")
		}
	})
}
