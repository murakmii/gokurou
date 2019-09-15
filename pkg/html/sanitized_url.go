package html

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
		u = &url.URL{
			Scheme:   sanitized.url.Scheme,
			Host:     sanitized.url.Host,
			Path:     path.Join(sanitized.url.Path, u.Path),
			RawQuery: u.Query().Encode(),
		}
	}

	return SanitizedURLFromURL(u)
}
