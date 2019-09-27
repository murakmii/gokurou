package url_frontier

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/murakmii/gokurou/pkg/gokurou"

	"github.com/murakmii/gokurou/pkg/gokurou/www"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
)

const (
	sharedDBSourceConfName      = "URL_FRONTIER_SHARED_DB_SOURCE"
	localDBPathProviderConfName = "URL_FRONTIER_LOCAL_DB_PATH_PROVIDER"
)

type defaultURLFrontier struct {
	sharedDB     *sql.DB
	totalWorkers uint32
	pushBuffer   map[uint32][]string
	pushedCount  map[uint32]uint64

	localDB   *sql.DB
	nextPopID int64
	popBuffer []string
}

func NewDefaultURLFrontier(ctx context.Context, conf *gokurou.Configuration) (gokurou.URLFrontier, error) {
	var err error

	sharedDB, err := sql.Open("mysql", conf.MustFetchAdvancedAsString(sharedDBSourceConfName))
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			_ = sharedDB.Close()
		}
	}()

	pathProvider, ok := conf.Advanced[localDBPathProviderConfName].(func(uint16) string)
	if !ok {
		return nil, fmt.Errorf("can't get local db path provider from configuration")
	}

	localDB, err := sql.Open("sqlite3", pathProvider(gokurou.GWNFromContext(ctx)))
	if err != nil {
		_ = sharedDB.Close()
		return nil, err
	}

	defer func() {
		if err != nil {
			_ = localDB.Close()
		}
	}()

	localDB.SetMaxOpenConns(1)
	localDB.SetMaxIdleConns(1)
	if _, err = localDB.Exec("CREATE TABLE IF NOT EXISTS crawled_hosts(host TEXT PRIMARY KEY)"); err != nil {
		return nil, err
	}

	return &defaultURLFrontier{
		sharedDB:     sharedDB,
		totalWorkers: uint32(conf.TotalWorkers()),
		pushBuffer:   make(map[uint32][]string),
		pushedCount:  make(map[uint32]uint64),
		localDB:      localDB,
		nextPopID:    0,
		popBuffer:    make([]string, 0),
	}, nil
}

func (frontier *defaultURLFrontier) Push(ctx context.Context, url *www.SanitizedURL) error {
	hash, err := url.HashNumber()
	if err != nil {
		return err
	}

	gwn := hash%frontier.totalWorkers + 1
	if _, ok := frontier.pushBuffer[gwn]; !ok {
		frontier.pushBuffer[gwn] = make([]string, 0, 51)
	}

	frontier.pushBuffer[gwn] = append(frontier.pushBuffer[gwn], url.String())
	frontier.pushedCount[gwn]++

	var threshold int
	if frontier.pushedCount[gwn] < 100 {
		threshold = 1
	} else {
		threshold = 50
	}

	if len(frontier.pushBuffer[gwn]) >= threshold {
		tabJoinedURL := strings.Join(frontier.pushBuffer[gwn], "\t")
		_, err := frontier.sharedDB.Exec("INSERT INTO urls(gwn, tab_joined_url) VALUES (?, ?)", gwn, tabJoinedURL)
		if err != nil {
			return err
		}

		frontier.pushBuffer[gwn] = make([]string, 0, 51)
	}

	return nil
}

func (frontier *defaultURLFrontier) Pop(ctx context.Context) (*www.SanitizedURL, error) {
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

		rows, err := frontier.localDB.Query("SELECT 1 FROM crawled_hosts WHERE host = ?", url.Host())
		if err != nil {
			return nil, err
		}

		needLoop := rows.Next()
		if err = rows.Close(); err != nil {
			return nil, err
		}

		if needLoop {
			continue
		}

		if _, err := frontier.localDB.Exec("INSERT INTO crawled_hosts VALUES(?)", url.Host()); err != nil {
			return nil, err
		}

		return url, nil
	}
}

func (frontier *defaultURLFrontier) Finish() error {
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
