package robots

import (
	"testing"
)

func TestTrie_add(t *testing.T) {
	trie := newTrie()
	err := trie.add("x")

	if err == nil {
		t.Errorf("add(%s) = nil, want = error", "x")
	}
}

func TestTrie_matches(t *testing.T) {
	trie := newTrie()

	pathes := []string{
		"/foo/bar",
		"/foo/ba",
		"/baz/*",
		"/hoge/*fuga",
		"/hoge/**piyo*",
		"/piyo/*.go$",
	}

	for _, path := range pathes {
		if err := trie.add(path); err != nil {
			t.Errorf("can't add path '%v'", err)
		}
	}

	tests := []struct {
		in  string
		out bool
	}{
		{"/foo", false},
		{"/foo/bar", true},
		{"/foo/bas", true},
		{"/foo/bb", false},
		{"/baz", false},
		{"/baz/xxx/yyy", true},
		{"/hoge/fuga", true},
		{"/hoge/fubfuga", true},
		{"/hoge/fub", false},
		{"/hoge/xpiyoy", true},
		{"/piyo/foo/baz/test.go", true},
		{"/piyo/foo/baz/test.gox", false},
	}

	for _, test := range tests {
		actual := trie.matches(test.in)
		if actual != test.out {
			t.Errorf("matches(%s) = %v, want = %v", test.in, actual, test.out)
		}
	}
}
