package robots

import "testing"

func TestPattern_matches(t *testing.T) {
	type inArgs struct {
		pattern string
		path    string
	}

	tests := []struct {
		in   inArgs
		want bool
	}{
		{in: inArgs{pattern: "/fi*****sh*****dayo", path: "/fishdayo"}, want: true},
		{in: inArgs{pattern: "/fi*****sh*****dayo", path: "/fixshxdayo"}, want: true},

		// https://developers.google.com/search/reference/robots_txt?hl=ja
		{in: inArgs{pattern: "/fish", path: "/fish"}, want: true},
		{in: inArgs{pattern: "/fish", path: "/fish.html"}, want: true},
		{in: inArgs{pattern: "/fish", path: "/fish/salmon.html"}, want: true},
		{in: inArgs{pattern: "/fish", path: "/fishheads"}, want: true},
		{in: inArgs{pattern: "/fish", path: "/fishheads/yummy.html"}, want: true},
		{in: inArgs{pattern: "/fish", path: "/fish.php?id=anything"}, want: true},
		{in: inArgs{pattern: "/fish", path: "/Fish.asp"}, want: false},
		{in: inArgs{pattern: "/fish", path: "/catfish"}, want: false},
		{in: inArgs{pattern: "/fish", path: "/?id=fish"}, want: false},

		{in: inArgs{pattern: "/fish*", path: "/fish"}, want: true},
		{in: inArgs{pattern: "/fish*", path: "/fish.html"}, want: true},
		{in: inArgs{pattern: "/fish*", path: "/fish/salmon.html"}, want: true},
		{in: inArgs{pattern: "/fish*", path: "/fishheads"}, want: true},
		{in: inArgs{pattern: "/fish*", path: "/fishheads/yummy.html"}, want: true},
		{in: inArgs{pattern: "/fish*", path: "/fish.php?id=anything"}, want: true},
		{in: inArgs{pattern: "/fish*", path: "/Fish.asp"}, want: false},
		{in: inArgs{pattern: "/fish*", path: "/catfish"}, want: false},
		{in: inArgs{pattern: "/fish*", path: "/?id=fish"}, want: false},

		{in: inArgs{pattern: "/fish/", path: "/fish/"}, want: true},
		{in: inArgs{pattern: "/fish/", path: "/fish/?id=anything"}, want: true},
		{in: inArgs{pattern: "/fish/", path: "/fish/salmon.htm"}, want: true},
		{in: inArgs{pattern: "/fish/", path: "/fish"}, want: false},
		{in: inArgs{pattern: "/fish/", path: "/fish.html"}, want: false},
		{in: inArgs{pattern: "/fish/", path: "/Fish/Salmon.asp"}, want: false},

		{in: inArgs{pattern: "/*.php", path: "/filename.php"}, want: true},
		{in: inArgs{pattern: "/*.php", path: "/folder/filename.php"}, want: true},
		{in: inArgs{pattern: "/*.php", path: "/folder/filename.php?parameters"}, want: true},
		{in: inArgs{pattern: "/*.php", path: "/folder/any.php.file.html"}, want: true},
		{in: inArgs{pattern: "/*.php", path: "/filename.php/"}, want: true},
		{in: inArgs{pattern: "/*.php", path: "/"}, want: false},
		{in: inArgs{pattern: "/*.php", path: "/windows.PHP"}, want: false},

		{in: inArgs{pattern: "/*.php$", path: "/filename.php"}, want: true},
		{in: inArgs{pattern: "/*.php$", path: "/folder/filename.php"}, want: true},
		{in: inArgs{pattern: "/*.php$", path: "/filename.php?parameters"}, want: false},
		{in: inArgs{pattern: "/*.php$", path: "/filename.php/"}, want: false},
		{in: inArgs{pattern: "/*.php$", path: "/filename.php5"}, want: false},
		{in: inArgs{pattern: "/*.php$", path: "/windows.PHP"}, want: false},

		{in: inArgs{pattern: "/fish*.php", path: "/fish.php"}, want: true},
		{in: inArgs{pattern: "/fish*.php", path: "/fishheads/catfish.php?parameters"}, want: true},
		{in: inArgs{pattern: "/fish*.php", path: "/Fish.PHP"}, want: false},
	}

	for _, tt := range tests {
		ptn, err := newPathPattern([]byte(tt.in.pattern))
		if err != nil {
			t.Errorf("failed to build pattern: %v", err)
		}

		got := ptn.matches([]byte(tt.in.path))
		if got != tt.want {
			t.Errorf("matches(%s) = %v, want = %v", tt.in.path, got, tt.want)
		}
	}
}
