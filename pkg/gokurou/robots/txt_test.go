package robots

import (
	"io"
	"os"
	"testing"
)

func testDataReader(path string) io.ReadCloser {
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}

	return f
}

func TestTxt_Allows(t *testing.T) {
	type inArgs struct {
		pUA  string
		sUA  string
		path string
	}

	tests := []struct {
		in   inArgs
		want bool
	}{
		{in: inArgs{pUA: "gokurou", sUA: "googlebot", path: "/path/to/contents"}, want: true},
		{in: inArgs{pUA: "gokurou", sUA: "googlebot", path: "/admin/index.html"}, want: false},
		{in: inArgs{pUA: "gokurou", sUA: "googlebot", path: "/admin/banner2"}, want: false},
		{in: inArgs{pUA: "gokurou", sUA: "googlebot", path: "/admin/banner1"}, want: true},
		{in: inArgs{pUA: "gokurou", sUA: "googlebot", path: "/admin/banner1/text"}, want: true},

		{in: inArgs{pUA: "foo", sUA: "googlebot", path: "/path/to/contents"}, want: true},
		{in: inArgs{pUA: "foo", sUA: "googlebot", path: "/admin/index.html"}, want: false},
		{in: inArgs{pUA: "foo", sUA: "googlebot", path: "/admin/banner2"}, want: true},
		{in: inArgs{pUA: "foo", sUA: "googlebot", path: "/admin/banner1"}, want: false},
		{in: inArgs{pUA: "foo", sUA: "googlebot", path: "/admin/banner2/text"}, want: true},

		{in: inArgs{pUA: "foo", sUA: "bar", path: "/path/to/contents"}, want: true},
		{in: inArgs{pUA: "foo", sUA: "bar", path: "/admin/index.html"}, want: false},
		{in: inArgs{pUA: "foo", sUA: "bar", path: "/admin/banner3"}, want: true},
		{in: inArgs{pUA: "foo", sUA: "bar", path: "/admin/banner1"}, want: false},
		{in: inArgs{pUA: "foo", sUA: "bar", path: "/admin/banner3/text"}, want: true},
	}

	for _, tt := range tests {
		testData := testDataReader("testdata/robots.txt")

		txt, err := ParserRobotsTxt(testData, tt.in.pUA, tt.in.sUA)
		if err != nil {
			t.Errorf("failed to parse robots.txt: %q", err)
		}

		got := txt.Allows(tt.in.path)
		if got != tt.want {
			t.Errorf("Txt(%s, %s).Allows(%s) = %v, want = %v", tt.in.pUA, tt.in.sUA, tt.in.path, got, tt.want)
		}

		_ = testData.Close()
	}
}

func TestTxt_Delay(t *testing.T) {
	type inArgs struct {
		pUA string
		sUA string
	}

	tests := []struct {
		in   inArgs
		want uint
	}{
		{in: inArgs{pUA: "gokurou", sUA: "googlebot"}, want: 100},
		{in: inArgs{pUA: "foo", sUA: "googlebot"}, want: 200},
		{in: inArgs{pUA: "foo", sUA: "bar"}, want: 300},
	}

	for _, tt := range tests {
		testData := testDataReader("testdata/robots.txt")

		txt, err := ParserRobotsTxt(testData, tt.in.pUA, tt.in.sUA)
		if err != nil {
			t.Errorf("failed to parse robots.txt: %q", err)
		}

		got := txt.Delay()
		if got != tt.want {
			t.Errorf("Txt(%s, %s).Delay() = %d, want = %d", tt.in.pUA, tt.in.sUA, got, tt.want)
		}

		_ = testData.Close()
	}
}
