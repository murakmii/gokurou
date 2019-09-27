package robots

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// 1つのrobots.txtを表す型
type Txt struct {
	primaryGroup   string
	secondaryGroup string
	anonymous      *group
	named          map[string]*group
}

// robots.txt中の1エントリーを表す型
type entry struct {
	field string
	value string
}

// robots.txt中の1グループを表す型
type group struct {
	allowed    *trie
	disallowed *trie
	delay      uint
}

// 文字列からTxtを生成して返す
func NewRobotsTxt(reader io.Reader, primaryGroup string, secondaryGroup string) (*Txt, error) {
	txt := &Txt{
		primaryGroup:   primaryGroup,
		secondaryGroup: secondaryGroup,
		anonymous:      newGroup(),
		named:          make(map[string]*group, 10),
	}

	currentGrp := txt.anonymous

	r := bufio.NewScanner(reader)
	for r.Scan() {
		entry, err := parseEntry(r.Text())
		if err != nil || entry.isComment() {
			continue
		}

		switch entry.field {
		case "user-agent":
			currentGrp = newGroup()
			txt.named[strings.ToLower(entry.value)] = currentGrp

		case "allow":
			if err := currentGrp.allowed.add(entry.value); err != nil {
				continue
			}

		case "disallow":
			if err := currentGrp.disallowed.add(entry.value); err != nil {
				continue
			}

		case "crawl-delay":
			d, err := strconv.Atoi(entry.value)
			if err == nil && d > 0 && uint(d) > currentGrp.delay {
				currentGrp.delay = uint(d)
			}

		default:
			continue
		}
	}

	if err := r.Err(); err != nil {
		return nil, fmt.Errorf("can't read string: %q", err)
	}

	return txt, nil
}

// robots.txtが指定のパスのクロールを許可しているかどうかを返す
func (txt *Txt) Allows(path string) bool {
	return txt.selectGroup().allows(path)
}

// robots.txtが表すクロール間隔を返す
func (txt *Txt) Delay() uint {
	return txt.selectGroup().delay
}

// robots.txt中から、パスやCrawl-Delayの選定元となる適切なグループを選んで返す
func (txt *Txt) selectGroup() *group {
	g, ok := txt.named[txt.primaryGroup]
	if ok {
		return g
	}

	g, ok = txt.named[txt.secondaryGroup]
	if ok {
		return g
	}

	g, ok = txt.named["*"]
	if ok {
		return g
	}

	return txt.anonymous
}

func parseEntry(s string) (*entry, error) {
	if len(s) > 2000 {
		return nil, fmt.Errorf("entry is too long")
	}

	s = strings.Trim(s, " \t")
	if strings.HasPrefix(s, "#") {
		return &entry{field: "#", value: ""}, nil
	}

	tokens := strings.Split(s, ":")
	if len(tokens) != 2 {
		return nil, fmt.Errorf("invalid entry")
	}

	return &entry{field: strings.ToLower(strings.Trim(tokens[0], " \t")), value: strings.Trim(tokens[1], " \t")}, nil
}

func (e *entry) isComment() bool {
	return e.field == "#"
}

func newGroup() *group {
	return &group{
		allowed:    newTrie(),
		disallowed: newTrie(),
		delay:      60,
	}
}

func (g *group) allows(path string) bool {
	return !g.disallowed.matches(path) || g.allowed.matches(path)
}
