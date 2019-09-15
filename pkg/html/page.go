package html

import (
	"fmt"
	"io"
	"strings"

	"golang.org/x/net/html"
)

type Page struct {
	title    string
	allURL   []*SanitizedURL
	noIndex  bool
	noFollow bool
}

func ParseHTML(baseURL *SanitizedURL, r io.Reader) (*Page, error) {
	page := &Page{allURL: make([]*SanitizedURL, 0, 100)}
	tokenizer := html.NewTokenizer(r)
	waitTitle := false

	var err error

TOKENIZE:
	for {
		tt := tokenizer.Next()

		switch tt {
		case html.ErrorToken:
			err = tokenizer.Err()
			break TOKENIZE

		case html.StartTagToken, html.SelfClosingTagToken:
			tagBytes, _ := tokenizer.TagName()

			switch strings.ToLower(string(tagBytes)) {
			case "title":
				waitTitle = true

			case "meta":
				attrs := readAttrs(tokenizer)
				if attrs["name"] != strings.ToLower(strings.Trim("robots", " ")) {
					continue
				}

				page.noIndex = strings.Contains(strings.ToLower(attrs["content"]), "noindex")
				page.noFollow = strings.Contains(strings.ToLower(attrs["content"]), "nofollow")

			case "a":
				normalized, err := generateSanitizedURL(baseURL, readAttrs(tokenizer))
				if err != nil {
					continue
				}
				page.allURL = append(page.allURL, normalized)
			}

		case html.TextToken:
			if !waitTitle {
				continue
			}
			page.title = string(tokenizer.Text())
			waitTitle = false
		}
	}

	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to parse html: %v", err)
	}

	return page, nil
}

func readAttrs(t *html.Tokenizer) map[string]string {
	attrs := make(map[string]string, 10)

	for {
		k, v, more := t.TagAttr()
		attrs[strings.ToLower(string(k))] = strings.Trim(string(v), " ")

		if !more || len(attrs) > 100 {
			break
		}
	}

	return attrs
}

func generateSanitizedURL(baseURL *SanitizedURL, attrs map[string]string) (*SanitizedURL, error) {
	if strings.ToLower(attrs["rel"]) == "nofollow" {
		return nil, fmt.Errorf("'a' tag has rel='nofollow' attribute")
	}

	href, ok := attrs["href"]
	if !ok {
		return nil, fmt.Errorf("'a' tag does NOT have 'href' attribute")
	}

	fetched, err := baseURL.Join(href)
	if err != nil {
		return nil, fmt.Errorf("can't fetch url: %v", err)
	}

	return fetched, nil
}

func (p *Page) Title() string {
	return p.title
}

func (p *Page) AllURL() []*SanitizedURL {
	if p.noFollow {
		return nil
	}

	return p.allURL
}

func (p *Page) NoIndex() bool {
	return p.noIndex
}
