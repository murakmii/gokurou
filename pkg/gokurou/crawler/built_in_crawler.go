package crawler

import (
	"context"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/murakmii/gokurou/pkg/gokurou"

	"github.com/murakmii/gokurou/pkg/gokurou/www"

	"golang.org/x/net/html/charset"

	"golang.org/x/xerrors"

	"github.com/murakmii/gokurou/pkg/gokurou/robots"
)

const (
	headerUAConfKey    = "built_in.crawler.header_ua"
	primaryUAConfKey   = "built_in.crawler.primary_ua"
	secondaryUAConfKey = "built_in.crawler.secondary_ua"
)

type builtInCrawler struct {
	headerUA         string
	primaryUA        string
	secondaryUA      string
	defaultRobotsTxt *robots.Txt
	httpClient       *http.Client
}

type responseWrapper struct {
	resp    *http.Response
	elapsed float64
}

type artifact struct {
	Host       string  `json:"host"`
	URL        string  `json:"url"`
	StatusCode int     `json:"status"`
	Title      string  `json:"title"`
	Server     string  `json:"server"`
	Elapsed    float64 `json:"elapsed"`
}

var (
	// robots.txtを取得する際のリダイレクトのルール
	// ホスト名ののSDLとTDLが等しい限り、3回までリダイレクトする
	robotsTxtRedirectPolicy = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 3 {
			return http.ErrUseLastResponse
		}

		first, err := www.SanitizedURLFromURL(via[0].URL)
		if err != nil {
			return http.ErrUseLastResponse
		}

		next, err := www.SanitizedURLFromURL(req.URL)
		if err != nil {
			return http.ErrUseLastResponse
		}

		if !strings.HasSuffix(next.Host(), first.Host()) {
			return http.ErrUseLastResponse
		}

		gokurou.LoggerFromContext(req.Context()).Debugf("redirecting: %s", req.URL)
		return nil
	}

	// ページを取得する際のリダイレクトのルール
	// ホスト名が等しい限り3回までリダイレクトする
	pageRedirectPolicy = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 3 {
			return http.ErrUseLastResponse
		}

		before, err := www.SanitizedURLFromURL(via[len(via)-1].URL)
		if err != nil {
			return http.ErrUseLastResponse
		}

		next, err := www.SanitizedURLFromURL(req.URL)
		if err != nil {
			return http.ErrUseLastResponse
		}

		if before.Host() != next.Host() {
			return http.ErrUseLastResponse
		}

		gokurou.LoggerFromContext(req.Context()).Debugf("redirecting: %s", req.URL)
		return nil
	}
)

// Crawlerを生成して返す
func BuiltInCrawlerProvider(_ context.Context, conf *gokurou.Configuration) (gokurou.Crawler, error) {
	return &builtInCrawler{
		headerUA:    conf.MustOptionAsString(headerUAConfKey),
		primaryUA:   conf.MustOptionAsString(primaryUAConfKey),
		secondaryUA: conf.MustOptionAsString(secondaryUAConfKey),
		httpClient: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        1,
				MaxIdleConnsPerHost: 1,
				MaxConnsPerHost:     2,
				DisableCompression:  false,

				// ESTABLISHEDなsocketの数に如実に影響するので短めに設定する
				// robots.txt取得後のページ取得まで生きていれば良い
				IdleConnTimeout: 1 * time.Second,
			},
			CheckRedirect: nil,
			Timeout:       10 * time.Second,
		},
	}, nil
}

func (crawler *builtInCrawler) Crawl(ctx context.Context, url *www.SanitizedURL, out gokurou.OutputPipeline) error {
	logger := gokurou.LoggerFromContext(ctx)
	defer func() {
		logger.Debug("finished")
	}()

	robotsTxt := crawler.getRobotsTxt(ctx, url)
	if robotsTxt != nil && !robotsTxt.Allows(url.Path()) {
		logger.Debugf("crawling disallowed by robots.txt: %s", url)
		return nil
	}

	resp, err := crawler.request(ctx, url, pageRedirectPolicy)
	gokurou.TracerFromContext(ctx).TraceCrawled(ctx, err)
	defer func() {
		if err != nil && !xerrors.Is(err, context.Canceled) {
			logger.Warnf("failed to crawl: %v", err)
		}
	}()

	if err != nil {
		return nil
	}

	defer func() {
		err = resp.resp.Body.Close()
	}()

	baseArtifact := &artifact{
		Host:       url.Host(),
		URL:        url.String(),
		StatusCode: resp.resp.StatusCode,
		Server:     resp.resp.Header.Get("Server"),
		Elapsed:    resp.elapsed,
	}

	defer func() {
		if baseArtifact != nil {
			out.OutputArtifact(ctx, baseArtifact) // 成果物があるならクロール終了時に保存する
		}
	}()

	if !resp.parsableText() {
		return nil
	}

	reader, err := resp.bodyReader()
	if err != nil {
		return nil
	}

	page, err := www.ParseHTML(reader, url)
	if err != nil {
		return nil
	}

	if page.NoIndex() {
		baseArtifact = nil
	} else {
		baseArtifact.Title = page.Title()
	}

	out.OutputCollectedURL(ctx, &gokurou.SpawnedURL{
		From:    url,
		Elapsed: resp.elapsed,
		Spawned: page.AllURL(),
	})

	return nil
}

func (crawler *builtInCrawler) Finish() error {
	crawler.httpClient.CloseIdleConnections()
	return nil
}

// robots.txtを取得する
// このメソッドはエラーを返さず、意図したrobots.txtが取得できないならデフォルトのそれを返す
func (crawler *builtInCrawler) getRobotsTxt(ctx context.Context, url *www.SanitizedURL) *robots.Txt {
	resp, err := crawler.request(ctx, url.RobotsTxtURL(), robotsTxtRedirectPolicy)
	defer func() {
		if err != nil && !xerrors.Is(err, context.Canceled) {
			gokurou.LoggerFromContext(ctx).Warnf("failed to get robots.txt: %v", err)
		}
	}()

	if err != nil {
		return nil
	}

	defer func() {
		err = resp.resp.Body.Close()
	}()

	if !resp.parsableText() {
		return nil
	}

	reader, err := resp.bodyReader()
	if err != nil {
		return nil
	}

	txt, err := robots.ParserRobotsTxt(reader, crawler.primaryUA, crawler.secondaryUA)
	if err != nil {
		return nil
	}

	return txt
}

func (crawler *builtInCrawler) request(ctx context.Context, url *www.SanitizedURL, redirectPolicy func(req *http.Request, via []*http.Request) error) (*responseWrapper, error) {
	gokurou.LoggerFromContext(ctx).Debugf("preparing: %s", url)

	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return nil, xerrors.Errorf("failed to build request for: %w", err)
	}

	req = req.WithContext(ctx)
	req.Header.Set("User-Agent", crawler.headerUA)

	crawler.httpClient.CheckRedirect = redirectPolicy
	start := time.Now()
	resp, err := crawler.httpClient.Do(req)
	elapsed := time.Since(start).Seconds()
	gokurou.TracerFromContext(ctx).TraceGetRequest(ctx, elapsed)

	if err != nil {
		return nil, err
	}

	return &responseWrapper{resp: resp, elapsed: elapsed}, nil
}

func (rw *responseWrapper) bodyReader() (io.Reader, error) {
	return charset.NewReader(rw.resp.Body, rw.resp.Header.Get("Content-Type"))
}

func (rw *responseWrapper) parsableText() bool {
	ct := rw.resp.Header.Get("Content-Type")

	return rw.resp.StatusCode >= 200 &&
		rw.resp.StatusCode < 300 &&
		(len(ct) == 0 ||
			strings.Contains(ct, "text") ||
			strings.Contains(ct, "html") ||
			strings.Contains(ct, "xml"))
}
