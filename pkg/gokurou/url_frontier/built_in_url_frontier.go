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
	sharedDBSourceConfKey = "built_in.url_frontier.shared_db_source"
	localDBPathConfKey    = "built_in.url_frontier.local_db_path"
)

type builtInURLFrontier struct {
	sharedDB     *sql.DB
	totalWorkers uint
	pushBuffer   map[uint][]string
	pushedCount  map[uint]uint64

	localDB     *sql.DB
	localDBPath string
	nextPopID   int64
	popBuffer   []string
}

func BuiltInURLFrontierProvider(ctx context.Context, conf *gokurou.Configuration) (gokurou.URLFrontier, error) {
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

func (frontier *builtInURLFrontier) Push(ctx context.Context, url *www.SanitizedURL) error {
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

	return nil
}

func (frontier *builtInURLFrontier) Pop(ctx context.Context) (*www.SanitizedURL, error) {
	for {
		if len(frontier.popBuffer) == 0 {
			var id int64
			var tabJoinedURL string
			query := "SELECT id, tab_joined_url FROM urls WHERE gwn = ? AND id > ? ORDER BY id"
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
