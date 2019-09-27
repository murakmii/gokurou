package robots

import (
	"fmt"
	"regexp"
)

// Trieっぽいデータ構造でパスのマッチングを行うための型
type trie struct {
	root *node
}

type node struct {
	char   byte
	exists bool
	next   map[byte]*node
}

var multiWild = regexp.MustCompile(`\*{2,}`)

// 新しくtrieを作成して返す
func newTrie() *trie {
	root := newNode(byte(0), false, 1)
	root.next['/'] = newNode('/', false, 10)

	return &trie{root: root}
}

// trieにパスを追加する
func (p *trie) add(path string) error {
	bytes := []byte(multiWild.ReplaceAllString(path, "*"))
	lenBytes := len(bytes)

	if bytes[0] != '/' {
		return fmt.Errorf("path is not started with '/'")
	}

	cur := p.root

	for i, b := range bytes {
		isLast := i+1 == lenBytes

		if b == '$' && !isLast {
			return fmt.Errorf("path includes invalid '$'")
		}

		if !cur.hasNext(b) {
			cur.addNext(newNode(b, false, 10))
		}

		cur = cur.next[b]
		if cur.exists == false && isLast {
			cur.exists = true
		}
	}

	return nil
}

// trieが指定のパスを保持しているかどうかを返す
func (p *trie) matches(path string) bool {
	bytes := []byte(path)
	lenBytes := len(bytes)

	cur := p.root
	var lastWild *node
	var reachedIdx int

	for i, b := range bytes {
		reachedIdx = i

		// 次のノードへ遷移する。この際ノードが'*'であるようなワイルドカードもサポートする
		n, ok := cur.next[b]
		if !ok {
			if cur.hasNext('*') {
				cur = cur.next['*']
				if cur.hasNext(b) {
					lastWild = cur
					cur = cur.next[b]
				}
			} else if cur.isWild() {
				continue
			} else if lastWild != nil {
				if lastWild.hasNext(b) {
					cur = lastWild.next[b]
				} else {
					cur = lastWild
				}
			} else {
				break
			}
		} else {
			cur = n
		}

		if cur.isWild() {
			lastWild = cur
		}
	}

	return cur.exists || (cur.hasNext('$') && reachedIdx+1 == lenBytes)
}

// 遷移先ノードを持つかどうかを返す
func (n *node) hasNext(b byte) bool {
	_, ok := n.next[b]
	return ok
}

// 遷移先ノードを追加する
func (n *node) addNext(next *node) {
	n.next[next.char] = next
}

func (n *node) isWild() bool {
	return n.char == '*'
}

// 新しくノードを作成する
func newNode(c byte, e bool, n int) *node {
	return &node{
		char:   c,
		exists: e,
		next:   make(map[byte]*node, n),
	}
}
