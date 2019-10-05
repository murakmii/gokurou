package www

import (
	"fmt"
	"strings"
	"testing"
)

func TestSanitizedURLFromString(t *testing.T) {
	t.Run("安全なURLの場合", func(t *testing.T) {
		tests := []struct {
			in  string
			out string
		}{
			{
				in:  "https://example.com/foo/bar",
				out: "https://example.com/foo/bar",
			},
			{
				in:  "HTTP://あいうえお.com/かき/くけ/%E3%81%93?p=さ%E3%81%97",
				out: "http://xn--l8jegik.com/%E3%81%8B%E3%81%8D/%E3%81%8F%E3%81%91/%E3%81%93?p=%E3%81%95%E3%81%97",
			},
		}

		for _, test := range tests {
			sanitized, err := SanitizedURLFromString(test.in)
			if err != nil {
				t.Errorf("SanitizedFromString(%s) = %v, want = no error", test.in, err)
				return
			}

			if sanitized.String() != test.out {
				t.Errorf("SanitizedFromString(%s) = %v, want = %s", test.in, sanitized.String(), test.out)
			}
		}

	})

	t.Run("安全ではないURLの場合", func(t *testing.T) {
		tests := []struct {
			in  string
			out string
		}{
			{
				in:  "/foo",
				out: "url is NOT absolute url",
			},
			{
				in:  "http://user:pass@example.com",
				out: "url has userinfo",
			},
			{
				in:  fmt.Sprintf("http://%s.com", strings.Repeat("a", 255)),
				out: "url's host is too long",
			},
			{
				in:  fmt.Sprintf("http://example.com/%s/%s", strings.Repeat("a", 500), strings.Repeat("a", 500)),
				out: "url's path and query is too long",
			},
		}

		for _, test := range tests {
			sanitized, err := SanitizedURLFromString(test.in)
			if sanitized != nil {
				t.Errorf("SanitizedFromString(%s) = %v, want = nil", test.in, sanitized)
				return
			}

			if err == nil {
				t.Errorf("SanitizedFromString(%s) = nil, want = %s", test.in, test.out)
				return
			}

			if err.Error() != test.out {
				t.Errorf("SanitizedFromString(%s) = %v, want = %s", test.in, err.Error(), test.out)
			}
		}
	})
}

func TestSanitizedURL_TLD(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{in: "http://example.com", want: "com"},
		{in: "http://example.co.jp", want: "jp"},
		{in: "http://com", want: "com"},
	}

	for _, tt := range tests {
		url, err := SanitizedURLFromString(tt.in)
		if err != nil {
			panic(err)
		}

		got := url.TLD()
		if got != tt.want {
			t.Errorf("TLD() = %s, want = %s", got, tt.want)
		}
	}
}

func TestSanitizedURL_RobotsTxtURL(t *testing.T) {
	url, err := SanitizedURLFromString("http://example.com/path/to/page")
	if err != nil {
		t.Error(err)
	}

	want := "http://example.com/robots.txt"

	if url.RobotsTxtURL().String() != want {
		t.Errorf("RobotsTxtURL() = %s, want = %s", url.RobotsTxtURL().String(), want)
	}
}

func TestSanitizedURL_Join(t *testing.T) {
	tests := []struct {
		in  string
		out string
	}{
		{
			in:  "../../foo",
			out: "http://example.com/%E3%81%93/foo",
		},
		{
			in:  "http://www.example.com/foo",
			out: "http://www.example.com/foo",
		},
		{
			in:  "/",
			out: "http://example.com/",
		},
	}

	sanitized, _ := SanitizedURLFromString("http://example.com/%E3%81%93/b/c")

	for _, test := range tests {
		joined, err := sanitized.Join(test.in)
		if err != nil {
			t.Errorf("Join(%s) = %s, want = no error", test.in, err)
			continue
		}

		if joined.String() != test.out {
			t.Errorf("Join(%s) = %s, want = %s", test.in, joined.String(), test.out)
		}
	}
}
