package robots

import (
	"fmt"
	"io"
	"os"
	"testing"
)

func loadTestData(path string) io.ReadCloser {
	f, err := os.Open(path)
	if err != nil {
		panic(fmt.Sprintf("can't load test data: '%s'", path))
	}

	return f
}

func Test_FromString(t *testing.T) {
	data := loadTestData("testdata/robots.txt")
	defer data.Close()

	txt, err := NewRobotsTxt(data, "gokurou", "googlebot")
	if err != nil {
		t.Errorf("FromString returns error!")
	}

	t.Run("クロール間隔が正しいこと", func(t *testing.T) {
		if txt.anonymous.delay != 90 {
			t.Errorf("anonymous.delay = %v, want = 90", txt.anonymous.delay)
		}

		tests := []struct {
			UA    string
			delay uint
		}{
			{"gokurou", 100},
			{"googlebot", 200},
			{"*", 300},
		}

		for _, test := range tests {
			if txt.named[test.UA].delay != test.delay {
				t.Errorf("named[%s].delay = %v, want = %v", test.UA, txt.named[test.UA].delay, test.delay)
			}
		}
	})

	t.Run("パスの設定が正しいこと", func(t *testing.T) {
		tests := []struct {
			UA         string
			disallowed string
			allowed    string
		}{
			{"gokurou", "/admin/a", "/admin/banner1"},
			{"googlebot", "/admin/b", "/admin/banner2"},
			{"*", "/admin/c", "/admin/banner3"},
		}

		for _, test := range tests {
			if !txt.named[test.UA].allows(test.allowed) {
				t.Errorf("test.named[%s].allows(%s) = false, want = true", test.UA, test.allowed)
			}

			if txt.named[test.UA].allows(test.disallowed) {
				t.Errorf("test.named[%s].allows(%s) = true, want = false", test.UA, test.disallowed)
			}
		}
	})
}

func TestTxt_Allows(t *testing.T) {
	data := loadTestData("testdata/robots.txt")
	defer data.Close()

	txt, err := NewRobotsTxt(data, "gokurou", "googlebot")
	if err != nil {
		t.Errorf("FromString returns error!")
	}

	if !txt.Allows("/admin/banner1") {
		t.Errorf("Allows(%v) = false, want = true", "/admin/banner1")
	}
}

func TestTxt_Delay(t *testing.T) {
	data := loadTestData("testdata/robots.txt")
	defer data.Close()

	txt, err := NewRobotsTxt(data, "gokurou", "googlebot")
	if err != nil {
		t.Errorf("FromString returns error!")
	}

	if txt.Delay() != 100 {
		t.Errorf("Delay() = %v, want = 100", txt.Delay())
	}
}
