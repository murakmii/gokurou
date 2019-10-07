package url_frontier

import (
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/xerrors"

	"github.com/murakmii/gokurou/pkg/gokurou"

	"github.com/murakmii/gokurou/pkg/gokurou/www"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
)

const (
	tldFilterConfKey      = "built_in.url_frontier.tld_filter"
	sharedDBSourceConfKey = "built_in.url_frontier.shared_db_source"
	localDBPathConfKey    = "built_in.url_frontier.local_db_path"
)

type builtInURLFrontier struct {
	sharedDB     *sql.DB
	totalWorkers uint
	tldFilter    []string
	pushBuffer   map[uint][]string
	pushedCount  map[uint]uint64

	localDB     *sql.DB
	localDBPath string
	nextPopID   int64
	popBuffer   []string
}

func BuiltInURLFrontierProvider(ctx context.Context, conf *gokurou.Configuration) (gokurou.URLFrontier, error) {
	var tldFilter []string
	tldFilterValue, exists := conf.Options[tldFilterConfKey]
	if exists {
		var ok bool
		tldFilter, ok = tldFilterValue.([]string)
		if !ok {
			return nil, xerrors.Errorf("'%s' config expects value as []string", tldFilterConfKey)
		}
	}

	var err error

	sharedDB, err := sql.Open("mysql", conf.MustOptionAsString(sharedDBSourceConfKey))
	if err != nil {
		return nil, xerrors.Errorf("failed to connect shared db: %v", err)
	}

	defer func() {
		if err != nil {
			_ = sharedDB.Close()
		}
	}()

	sharedDB.SetMaxOpenConns(1)
	sharedDB.SetMaxIdleConns(2)
	sharedDB.SetConnMaxLifetime(10 * time.Second)

	localDBPathPtr := conf.OptionAsString(localDBPathConfKey)
	var localDBPath string
	if localDBPathPtr == nil {
		localDBPath = ":memory:"
		gokurou.LoggerFromContext(ctx).Warn("local db setted on memory")
	} else {
		localDBPath = fmt.Sprintf(*localDBPathPtr, gokurou.GWNFromContext(ctx))
	}

	localDB, err := sql.Open("sqlite3", localDBPath)
	if err != nil {
		return nil, xerrors.Errorf("failed to connect local db: %v", err)
	}

	defer func() {
		if err != nil {
			_ = localDB.Close()
		}
	}()

	localDB.SetMaxOpenConns(1)
	localDB.SetMaxIdleConns(1)
	localDB.SetConnMaxLifetime(0)
	if _, err = localDB.Exec("CREATE TABLE IF NOT EXISTS crawled_hosts(host TEXT PRIMARY KEY)"); err != nil {
		return nil, xerrors.Errorf("failed to setup local db: %v", err)
	}

	return &builtInURLFrontier{
		sharedDB:     sharedDB,
		totalWorkers: conf.TotalWorkers(),
		tldFilter:    tldFilter,
		pushBuffer:   make(map[uint][]string),
		pushedCount:  make(map[uint]uint64),
		localDB:      localDB,
		localDBPath:  localDBPath,
		nextPopID:    0,
		popBuffer:    make([]string, 0),
	}, nil
}

func (frontier *builtInURLFrontier) Seeding(url *www.SanitizedURL) error {
	_, err := frontier.sharedDB.Exec("INSERT INTO urls(gwn, tab_joined_url) VALUES (1, ?)", url.String())
	if err != nil {
		return err
	}

	return nil
}

func (frontier *builtInURLFrontier) Push(ctx context.Context, spawned *gokurou.SpawnedURL) error {
	filtered := frontier.filterURL(spawned)
	for _, url := range filtered {
		destGWN := frontier.computeDestinationGWN(url)

		if _, ok := frontier.pushBuffer[destGWN]; !ok {
			frontier.pushBuffer[destGWN] = make([]string, 0, 51)
		}

		frontier.pushBuffer[destGWN] = append(frontier.pushBuffer[destGWN], url.String())
		frontier.pushedCount[destGWN]++

		var threshold int
		if frontier.pushedCount[destGWN] < 1000 {
			threshold = 1
		} else {
			threshold = 50
		}

		if len(frontier.pushBuffer[destGWN]) >= threshold {
			tabJoinedURL := strings.Join(frontier.pushBuffer[destGWN], "\t")
			_, err := frontier.sharedDB.Exec("INSERT INTO urls(gwn, tab_joined_url) VALUES (?, ?)", destGWN, tabJoinedURL)
			if err != nil {
				return err
			}

			frontier.pushBuffer[destGWN] = make([]string, 0, 51)
		}
	}

	return nil
}

func (frontier *builtInURLFrontier) Pop(ctx context.Context) (*www.SanitizedURL, error) {
	for {
		if len(frontier.popBuffer) == 0 {
			var id int64
			var tabJoinedURL string
			query := "SELECT id, tab_joined_url FROM urls WHERE gwn = ? AND id > ? ORDER BY id LIMIT 1"
			err := frontier.sharedDB.QueryRow(query, gokurou.GWNFromContext(ctx), frontier.nextPopID).Scan(&id, &tabJoinedURL)

			if err == sql.ErrNoRows {
				return nil, nil
			} else if err != nil {
				return nil, err
			}

			frontier.nextPopID = id
			frontier.popBuffer = strings.Split(tabJoinedURL, "\t")
		}

		url, err := www.SanitizedURLFromString(frontier.popBuffer[0])
		if err != nil {
			return nil, err
		}

		frontier.popBuffer = frontier.popBuffer[1:]

		var tmp sql.NullInt64
		if err = frontier.localDB.QueryRow("SELECT 1 FROM crawled_hosts WHERE host = ?", url.Host()).Scan(&tmp); err != nil {
			if err != sql.ErrNoRows {
				return nil, err
			}
		} else {
			continue
		}

		if _, err := frontier.localDB.Exec("INSERT INTO crawled_hosts VALUES(?)", url.Host()); err != nil {
			return nil, err
		}

		return url, nil
	}
}

func (frontier *builtInURLFrontier) Finish() error {
	sharedDBErr := frontier.sharedDB.Close()
	localDBErr := frontier.localDB.Close()

	if sharedDBErr != nil {
		return sharedDBErr
	}

	if localDBErr != nil {
		return localDBErr
	}

	return nil
}

func (frontier *builtInURLFrontier) Reset() error {
	_, err := frontier.sharedDB.Exec("TRUNCATE urls")
	if err != nil {
		return err
	}

	if err = frontier.Finish(); err != nil {
		return err
	}

	files, err := filepath.Glob(filepath.Join(filepath.Dir(frontier.localDBPath), "*.sqlite"))
	for _, file := range files {
		if err = os.Remove(file); err != nil {
			return err
		}
	}

	return nil
}

// URLから、それを処理するべきworkerのGWNを求める
// ホスト名のSLDとTLDのハッシュ値から計算する
func (frontier *builtInURLFrontier) computeDestinationGWN(url *www.SanitizedURL) uint {
	sldAndTLD := strings.Split(url.Host(), ".")
	if len(sldAndTLD) > 2 {
		sldAndTLD = sldAndTLD[len(sldAndTLD)-2:]
	}

	// hash.Hash32のWriteの実装を読めば分かるが、これは絶対にエラーを返さない
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(strings.Join(sldAndTLD, ".")))

	return (uint(hash.Sum32()) % frontier.totalWorkers) + 1
}

// 収集されたURLを必要なものだけにフィルタする
func (frontier *builtInURLFrontier) filterURL(spawned *gokurou.SpawnedURL) []*www.SanitizedURL {
	urlPerHost := make(map[string]*www.SanitizedURL)

	// * 1ホストあたり1つのURLで良い
	// * TLDによるフィルタ
	// * 生成元と同じホスト部を持つURLは不要
	for _, url := range spawned.Spawned {
		if !frontier.isAvailableURL(url) || spawned.From.Host() == url.Host() {
			continue
		}

		exists, ok := urlPerHost[url.Host()]
		if !ok || len(exists.Path()) > len(url.Path()) {
			urlPerHost[url.Host()] = url
		}
	}

	filtered := make([]*www.SanitizedURL, len(urlPerHost))
	idx := 0
	for _, url := range urlPerHost {
		filtered[idx] = url
		idx++
	}

	return filtered
}

// URLが有効なものかどうか。今のところ判定の条件はTLDのフィルタに引っかかるかどうかのみ
func (frontier *builtInURLFrontier) isAvailableURL(url *www.SanitizedURL) bool {
	if len(frontier.tldFilter) == 0 {
		return true
	}

	tld := url.TLD()
	for _, fTLD := range frontier.tldFilter {
		if fTLD == tld {
			return true
		}
	}
	return false
}
