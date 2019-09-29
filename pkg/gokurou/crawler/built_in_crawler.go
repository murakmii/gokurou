package crawler

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
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
	resp *http.Response
}

type artifact struct {
	URL        string `json:"url"`
	StatusCode int    `json:"status"`
	Title      string `json:"title"`
	Server     string `json:"server"`
}

// robots.txtを取得する際のリダイレクトのルール
// ホスト名ののSDLとTDLが等しい限り、3回までリダイレクトする
var robotsTxtRedirectPolicy = func(req *http.Request, via []*http.Request) error {
	if len(via) >= 3 {
		return http.ErrUseLastResponse
	}

	before, err := www.SanitizedURLFromURL(via[len(via)-1].URL)
	if err != nil {
		return http.ErrUseLastResponse
	}

	next, err := before.Join(req.URL.String())
	if err != nil {
		return http.ErrUseLastResponse
	}

	if before.SLDAndTLDOfHost() != next.SLDAndTLDOfHost() {
		return http.ErrUseLastResponse
	}

	gokurou.LoggerFromContext(req.Context()).Debugf("redirecting: %s", req.URL)
	return nil
}

// ページを取得する際のリダイレクトのルール
// ホスト名が等しい限り3回までリダイレクトする
var pageRedirectPolicy = func(req *http.Request, via []*http.Request) error {
	if len(via) >= 3 {
		return http.ErrUseLastResponse
	}

	before, err := www.SanitizedURLFromURL(via[len(via)-1].URL)
	if err != nil {
		return http.ErrUseLastResponse
	}

	next, err := before.Join(req.URL.String())
	if err != nil {
		return http.ErrUseLastResponse
	}

	if before.Host() != next.Host() {
		return http.ErrUseLastResponse
	}

	gokurou.LoggerFromContext(req.Context()).Debugf("redirecting: %s", req.URL)
	return nil
}

// Crawlerを生成して返す
func BuiltInCrawlerProvider(ctx context.Context, conf *gokurou.Configuration) (gokurou.Crawler, error) {
	primaryUA := conf.MustOptionAsString(primaryUAConfKey)
	secondaryUA := conf.MustOptionAsString(secondaryUAConfKey)

	defaultRobotsTxt, err := robots.ParserRobotsTxt(bytes.NewBuffer(nil), primaryUA, secondaryUA)
	if err != nil {
		return nil, xerrors.Errorf("failed to build default robots.txt: %w", err)
	}

	return &builtInCrawler{
		headerUA:         conf.MustOptionAsString(headerUAConfKey),
		primaryUA:        primaryUA,
		secondaryUA:      secondaryUA,
		defaultRobotsTxt: defaultRobotsTxt,
		httpClient: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        1,
				MaxIdleConnsPerHost: 1,
				MaxConnsPerHost:     1,
				DisableCompression:  false,
			},
			CheckRedirect: nil,
			Timeout:       5 * time.Second,
		},
	}, nil
}

func (crawler *builtInCrawler) Crawl(ctx context.Context, url *www.SanitizedURL, out gokurou.OutputPipeline) error {
	logger := gokurou.LoggerFromContext(ctx)
	defer func() {
		logger.Debug("finished")
	}()

	robotsTxt := crawler.requestToGetRobotsTxtOf(ctx, url)
	if !robotsTxt.Allows(url.Path()) {
		logger.Debugf("crawling disallowed by robots.txt: %s", url.String())
		return nil
	}

	resp, err := crawler.buildRequestToGet(ctx, url, pageRedirectPolicy)
	defer func() {
		if err != nil {
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
		URL:        url.String(),
		StatusCode: resp.resp.StatusCode,
		Server:     resp.resp.Header.Get("Server"),
	}

	defer func() {
		// 成果物があるならクロール終了時に保存する
		if baseArtifact != nil {
			j, err := json.Marshal(baseArtifact)
			if err == nil {
				out.OutputArtifact(ctx, j)
			}
		}
	}()

	if !resp.succeeded() {
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

	// どうせ1ホストあたり1回しかクロールしないので、この時点で1ホスト1URLになるようにフィルタしてから結果を送信する
	urlPerHost := make(map[string]*www.SanitizedURL)
	for _, collectedURL := range page.AllURL() {
		if collectedURL.Host() == url.Host() {
			continue
		}

		u, ok := urlPerHost[collectedURL.Host()]
		if !ok || len(u.Path()) > len(collectedURL.Path()) {
			urlPerHost[collectedURL.Host()] = collectedURL
		}
	}

	for _, u := range urlPerHost {
		out.OutputCollectedURL(ctx, u)
	}

	return nil
}

func (crawler *builtInCrawler) Finish() error {
	crawler.httpClient.CloseIdleConnections()
	return nil
}

// robots.txtを取得する
// このメソッドはエラーを返さず、意図したrobots.txtが取得できないならデフォルトのそれを返す
func (crawler *builtInCrawler) requestToGetRobotsTxtOf(ctx context.Context, url *www.SanitizedURL) (robotsTxt *robots.Txt) {
	robotsTxt = crawler.defaultRobotsTxt

	resp, err := crawler.buildRequestToGet(ctx, url.RobotsTxtURL(), robotsTxtRedirectPolicy)
	defer func() {
		if err != nil {
			gokurou.LoggerFromContext(ctx).Warnf("failed to get robots.txt: %v", err)
		}
	}()

	if err != nil {
		return
	}

	defer func() {
		err = resp.resp.Body.Close()
	}()

	if !resp.succeeded() {
		return
	}

	reader, err := resp.bodyReader()
	if err != nil {
		return
	}

	robotsTxt, err = robots.ParserRobotsTxt(reader, crawler.primaryUA, crawler.secondaryUA)
	if err != nil {
		return
	}

	return
}

func (crawler *builtInCrawler) buildRequestToGet(ctx context.Context, url *www.SanitizedURL, redirectPolicy func(req *http.Request, via []*http.Request) error) (*responseWrapper, error) {
	gokurou.LoggerFromContext(ctx).Debugf("preparing: %s", url)

	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return nil, xerrors.Errorf("failed to build request for: %w", err)
	}

	req = req.WithContext(ctx)
	req.Header.Set("User-Agent", crawler.headerUA)

	crawler.httpClient.CheckRedirect = redirectPolicy
	resp, err := crawler.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	return &responseWrapper{resp: resp}, nil
}

func (rw *responseWrapper) bodyReader() (io.Reader, error) {
	return charset.NewReader(rw.resp.Body, rw.resp.Header.Get("Content-Type"))
}

func (rw *responseWrapper) succeeded() bool {
	return rw.resp.StatusCode >= 200 && rw.resp.StatusCode < 300
}
