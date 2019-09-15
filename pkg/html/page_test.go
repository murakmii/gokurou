package html

import (
	"fmt"
	"io"
	"os"
	"testing"
)

func openTestData(path string) io.Reader {
	f, err := os.Open(path)
	if err != nil {
		panic(fmt.Sprintf("can't open test data: %v", err))
	}

	return f
}

func TestParseHTML(t *testing.T) {
	baseURL, err := SanitizedURLFromString("http://www.example.com/a/b/c")
	if err != nil {
		panic(fmt.Sprintf("can't build sanitized url"))
	}

	t.Run("一般的なHTMLの場合", func(t *testing.T) {
		html, err := ParseHTML(baseURL, openTestData("testdata/test.html"))
		if err != nil {
			t.Errorf("ParseHTML(testdata/test.html) = error, want = no error")
			return
		}

		if html.Title() != "テスト用HTML" {
			t.Errorf("ParseHTML(testdata/test.html).Title() = %s, want = \"テスト用HTML\"", html.Title())
		}

		if !html.NoIndex() {
			t.Errorf("ParseHTML(testdata/test.html).NoIndex() = false, want = true")
		}

		if len(html.AllURL()) != 3 {
			t.Errorf("len(ParseHTML(testdata/test.html).AllURL()) = %d, want = 3", len(html.AllURL()))
		}

		wantURL := []string{"http://example1.com", "https://example2.com", "http://www.example.com/a/b/rel.html"}
		for i, want := range wantURL {
			if html.AllURL()[i].String() != want {
				t.Errorf("ParseHTML(testdata/test.html).AllURL()[%d] = %s, want = %s", i, html.AllURL()[i].String(), want)
			}
		}
	})

	t.Run("nofollowが全面的に指定されているHTMLの場合", func(t *testing.T) {
		html, err := ParseHTML(baseURL, openTestData("testdata/nofollow.html"))
		if err != nil {
			t.Errorf("ParseHTML(testdata/nofollow.html) = error, want = no error")
			return
		}

		if html.Title() != "テスト用HTML" {
			t.Errorf("ParseHTML(testdata/nofollow.html).Title() = %s, want = \"テスト用HTML\"", html.Title())
		}

		if html.NoIndex() {
			t.Errorf("ParseHTML(testdata/nofollow.html).NoIndex() = true, want = false")
		}

		if len(html.AllURL()) != 0 {
			t.Errorf("len(ParseHTML(testdata/nofollow.html).AllURL()) = %d, want = 0", len(html.AllURL()))
		}
	})
}
