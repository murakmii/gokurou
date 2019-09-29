package robots

import (
	"golang.org/x/xerrors"
)

// '/path/to/*/contents' のようなパターンに対するマッチングを行う型
type pathPattern []byte

// pathPatternの集合
type pathPattenSet []pathPattern

func newPathPattern(ptn []byte) (pathPattern, error) {
	if len(ptn) == 0 {
		return nil, xerrors.New("invalid path pattern")
	}

	if ptn[0] != '/' {
		return nil, xerrors.New("invalid path pattern")
	}

	return ptn, nil
}

func newPathPatternSet() pathPattenSet {
	return make([]pathPattern, 0, 50)
}

func (p pathPattern) matches(path []byte) bool {
	bytes := append(path, 0) // '$'とのマッチングを判定しやすいようヌル文字を足しておく
	ptnIdx := 0
	pthIdx := 0

	for {
		if p[ptnIdx] == '*' {
			// '*'が見つかったらそれ以外が見つかるまでptnIdxを進める。
			// もしそのままパターンの末尾に到達した場合はどんなパスにもマッチするのでtrueを返す
			for ptnIdx < len(p) && p[ptnIdx] == '*' {
				ptnIdx++
			}

			if ptnIdx == len(p) {
				return true
			}

			// '*'の直後にある文字に一致する文字をパス側から見つける。
			// もしそのままパターンの末尾に到達しなかった場合はマッチしないことになる
			for pthIdx < len(bytes) && bytes[pthIdx] != p[ptnIdx] {
				pthIdx++
			}

			if pthIdx == len(bytes) {
				return false
			}
		} else if p[ptnIdx] == '$' && bytes[pthIdx] == 0 {
			return true
		} else {
			if p[ptnIdx] == bytes[pthIdx] {
				ptnIdx++
				pthIdx++

				if ptnIdx == len(p) {
					return true
				} else if pthIdx == len(bytes) {
					return false
				}
			} else {
				return false
			}
		}
	}
}

func (ps pathPattenSet) matches(path string) bool {
	bytes := []byte(path)
	for _, pp := range ps {
		if pp.matches(bytes) {
			return true
		}
	}

	return false
}
