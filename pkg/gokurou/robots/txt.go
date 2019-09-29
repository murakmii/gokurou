package robots

import (
	"bufio"
	"io"
	"strconv"
	"strings"

	"golang.org/x/xerrors"
)

// 1つのrobots.txtを表す型
type Txt struct {
	pUA       string // robots.txtのうち、どのUserAgentの設定を参照するか("gokurou"など)
	pGrp      *group
	sUA       string // pUAで指定されるUserAgentが存在しない場合の第2UserAgent指定("googlebot"など)
	sGrp      *group
	anonymous *group
}

// robots.txt中の1エントリーを表す型
type entry struct {
	field string
	value string
}

// robots.txt中の1グループを表す型
type group struct {
	allowed    pathPattenSet
	disallowed pathPattenSet
	delay      uint
}

var (
	errCommentEntry = xerrors.New("entry is comment")
)

// 文字列からTxtを生成して返す
func ParserRobotsTxt(reader io.Reader, primaryUA string, secondaryUA string) (*Txt, error) {
	txt := &Txt{
		pUA:       primaryUA,
		sUA:       secondaryUA,
		anonymous: newGroup(),
	}

	var currentGrp *group

	r := bufio.NewScanner(reader)
	for r.Scan() {
		entry, err := parseEntry(r.Text())
		if err != nil {
			continue
		}

		// 興味があるUA以外の設定は無視するようにしておく
		if entry.field == "user-agent" {
			switch strings.ToLower(entry.value) {
			case primaryUA:
				txt.pGrp = newGroup()
				currentGrp = txt.pGrp

			case secondaryUA:
				txt.sGrp = newGroup()
				currentGrp = txt.sGrp

			case "*":
				currentGrp = txt.anonymous
			}
		}

		if currentGrp == nil {
			continue
		}

		switch entry.field {
		case "allow":
			if pp, err := newPathPattern([]byte(entry.value)); err == nil {
				currentGrp.allowed = append(currentGrp.allowed, pp)
			}

		case "disallow":
			if pp, err := newPathPattern([]byte(entry.value)); err == nil {
				currentGrp.disallowed = append(currentGrp.disallowed, pp)
			}

		case "crawl-delay":
			d, err := strconv.Atoi(entry.value)
			if err == nil && uint(d) > currentGrp.delay {
				currentGrp.delay = uint(d)
			}
		}
	}

	if err := r.Err(); err != nil {
		return nil, xerrors.Errorf("failed to read: %w", err)
	}

	return txt, nil
}

// robots.txtが指定のパスのクロールを許可しているかどうかを返す
func (txt *Txt) Allows(path string) bool {
	return txt.group().allows(path)
}

// robots.txtが表すクロール間隔を返す
func (txt *Txt) Delay() uint {
	return txt.group().delay
}

// robots.txt中から、パスやCrawl-Delayの選定元となる適切なグループを選んで返す
func (txt *Txt) group() *group {
	if txt.pGrp != nil {
		return txt.pGrp
	} else if txt.sGrp != nil {
		return txt.sGrp
	}

	return txt.anonymous
}

func parseEntry(s string) (*entry, error) {
	if len(s) > 2000 {
		return nil, xerrors.New("entry is too long")
	}

	s = strings.Trim(s, " \t")
	if strings.HasPrefix(s, "#") {
		return nil, errCommentEntry
	}

	tokens := strings.Split(s, ":")
	if len(tokens) != 2 {
		return nil, xerrors.New("invalid entry")
	}

	return &entry{
		field: strings.ToLower(strings.Trim(tokens[0], " \t")),
		value: strings.Trim(tokens[1], " \t")}, nil
}

func newGroup() *group {
	return &group{
		allowed:    newPathPatternSet(),
		disallowed: newPathPatternSet(),
		delay:      60,
	}
}

func (g *group) allows(path string) bool {
	return !g.disallowed.matches(path) || g.allowed.matches(path)
}
