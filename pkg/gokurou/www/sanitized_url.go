package www

import (
	"fmt"
	"net/url"
	"path"
	"strings"

	"golang.org/x/net/idna"
)

// クローラー中で扱うことが安全なURLを表す型
type SanitizedURL struct {
	url *url.URL
}

// URLをサニタイズしてSanitizedURLを返す
func SanitizedURLFromURL(u *url.URL) (*SanitizedURL, error) {
	if !u.IsAbs() {
		return nil, fmt.Errorf("url is NOT absolute url")
	}

	if u.User != nil {
		return nil, fmt.Errorf("url has userinfo")
	}

	sScheme := strings.ToLower(u.Scheme)
	if sScheme != "http" && sScheme != "https" {
		return nil, fmt.Errorf("url's scheme is invalid: %s", sScheme)
	}

	// これは微妙にテストも考慮しており、localhostならportの指定があっても許可する
	if !strings.HasPrefix(u.Host, "127.0.0.1:") && len(u.Port()) > 0 {
		return nil, fmt.Errorf("url's has port")
	}

	sHost, err := idna.ToASCII(u.Host)
	if err != nil {
		return nil, fmt.Errorf("url has invalid host: %s", sHost)
	}

	if len(sHost) > 255 {
		return nil, fmt.Errorf("url's host is too long")
	}

	sPath := u.Path
	sQuery := u.Query().Encode()

	if len(sPath)+len(sQuery) > 1000 {
		return nil, fmt.Errorf("url's path and query is too long")
	}

	return &SanitizedURL{
		url: &url.URL{
			Scheme:   sScheme,
			Host:     sHost,
			Path:     sPath,
			RawQuery: sQuery,
		},
	}, nil
}

// 文字列で表されるURLをサニタイズしてSanitizedURLを返す
func SanitizedURLFromString(s string) (*SanitizedURL, error) {
	if len(s) > 2000 {
		return nil, fmt.Errorf("url is too long")
	}

	u, err := url.Parse(s)
	if err != nil {
		return nil, fmt.Errorf("can't parse url: %s", u)
	}

	return SanitizedURLFromURL(u)
}

// URLのホスト部を返す
func (sanitized *SanitizedURL) Host() string {
	return sanitized.url.Host
}

// ホスト部が表すドメインのTLDを返す
func (sanitized *SanitizedURL) TLD() string {
	labels := strings.Split(sanitized.Host(), ".")
	return labels[len(labels)-1]
}

// URLのパス部を返す
func (sanitized *SanitizedURL) Path() string {
	return sanitized.url.Path
}

// このURLに対して有効なrobots.txtのURLを返す
func (sanitized *SanitizedURL) RobotsTxtURL() *SanitizedURL {
	robotsTxt, _ := url.Parse(sanitized.String())
	robotsTxt.Path = "/robots.txt"
	return &SanitizedURL{url: robotsTxt}
}

// サニタイズ済みURLの文字列表現を返す
func (sanitized *SanitizedURL) String() string {
	return sanitized.url.String()
}

// このサニタイズ済みURLを元に、相対パスを与えて新しいサニタイズ済みURLを生成する
func (sanitized *SanitizedURL) Join(str string) (*SanitizedURL, error) {
	u, err := url.Parse(str)
	if err != nil {
		return nil, fmt.Errorf("can't parse url: %s", str)
	}

	if !u.IsAbs() {
		var newPath string
		if strings.HasPrefix(u.Path, "/") {
			newPath = u.Path
		} else {
			newPath = path.Join(sanitized.url.Path, u.Path)
		}

		u = &url.URL{
			Scheme:   sanitized.url.Scheme,
			Host:     sanitized.url.Host,
			Path:     newPath,
			RawQuery: u.Query().Encode(),
		}
	}

	return SanitizedURLFromURL(u)
}
