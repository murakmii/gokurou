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

	"github.com/murakmii/gokurou/pkg/gokurou/www"

	"golang.org/x/xerrors"

	"github.com/gomodule/redigo/redis"
	"github.com/murakmii/gokurou/pkg/gokurou"
)

const (
	pubSubtldFilterConfKey   = "redis_pub_sub_url_frontier.tld_filter"
	pubSubRedisURLConfKey    = "redis_pub_sub_url_frontier.redis_url"
	pubSubLocalDBPathConfKey = "redis_pub_sub_url_frontier.local_db_path"

	maxURLPerHost = 5
)

type redisPubSubURLFrontier struct {
	totalWorkers uint
	pub          redis.Conn
	sub          redis.Conn
	subCh        chan error

	localDB     *sql.DB
	localDBPath string

	tldFilter []string
}

func RedisPubSubURLFrontierProvider(ctx context.Context, conf *gokurou.Configuration) (gokurou.URLFrontier, error) {
	var tldFilter []string
	tldFilterValue, exists := conf.Options[pubSubtldFilterConfKey]
	if exists {
		var ok bool
		tldFilter, ok = tldFilterValue.([]string)
		if !ok {
			return nil, xerrors.Errorf("'%s' config expects value as []string", tldFilterConfKey)
		}
	}

	pub, err := redis.DialURL(conf.MustOptionAsString(pubSubRedisURLConfKey))
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			_ = pub.Close()
		}
	}()

	sub, err := redis.DialURL(conf.MustOptionAsString(pubSubRedisURLConfKey))
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			_ = sub.Close()
		}
	}()

	localDBPathPtr := conf.OptionAsString(pubSubLocalDBPathConfKey)
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

	initialQueries := []string{
		"PRAGMA journal_mode=memory", // ガッツ
		"PRAGMA synchronous=OFF",

		"CREATE        TABLE IF NOT EXISTS hosts(id INTEGER PRIMARY KEY, host TEXT, crawlable_at INTEGER)",
		"CREATE UNIQUE INDEX IF NOT EXISTS hosts_host ON hosts(host)",
		"CREATE        INDEX IF NOT EXISTS hosts_crawlable_at ON hosts(crawlable_at)",

		"CREATE TABLE IF NOT EXISTS urls(id INTEGER PRIMARY KEY, host_id INTEGER, url TEXT, crawled INTEGER)",
		"CREATE INDEX IF NOT EXISTS urls_host_id ON urls(host_id)",
	}

	for _, query := range initialQueries {
		if _, err = localDB.Exec(query); err != nil {
			return nil, xerrors.Errorf("failed to setup local db: %v", err)
		}
	}

	frontier := &redisPubSubURLFrontier{
		totalWorkers: conf.TotalWorkers(),
		pub:          pub,
		sub:          sub,
		subCh:        make(chan error, 1),
		localDB:      localDB,
		localDBPath:  localDBPath,
		tldFilter:    tldFilter,
	}

	frontier.subscribeLoop(ctx, frontier.subCh)

	return frontier, nil
}

func beginTx(db *sql.DB, f func(tx *sql.Tx) error) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	if err := f(tx); err != nil {
		return tx.Rollback()
	}

	return tx.Commit()
}

func (f *redisPubSubURLFrontier) subscribeLoop(ctx context.Context, ch chan<- error) {
	stream := f.streamName(gokurou.GWNFromContext(ctx))

	go func() {
		var err error
		defer func() { ch <- err }()

	SUBSCRIBE:
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			sanitized, err := f.subscribe(stream)
			if err != nil {
				return
			}

			if sanitized == nil {
				continue
			}

			hostID, noHost, err := f.queryRowAsInt64("SELECT id FROM hosts WHERE host = ?", sanitized.Host())
			if err != nil {
				return
			}

			if !noHost {
				savedURLs, err := f.queryRowsAsStrings("SELECT url FROM urls WHERE host_id = ?", maxURLPerHost, hostID)
				if err != nil {
					return
				}

				if len(savedURLs) >= maxURLPerHost {
					continue
				}

				for _, u := range savedURLs {
					s, err := www.SanitizedURLFromString(u)
					if err != nil {
						return
					}

					if s.Path() == sanitized.Path() {
						continue SUBSCRIBE
					}
				}
			}

			err = beginTx(f.localDB, func(tx *sql.Tx) error {
				if noHost {
					inserted, errInTx := tx.Exec("INSERT INTO hosts(host, crawlable_at) VALUES(?, ?)", sanitized.Host(), time.Now().Unix())
					if errInTx != nil {
						return errInTx
					}

					if hostID, errInTx = inserted.LastInsertId(); errInTx != nil {
						return errInTx
					}
				}

				query := "INSERT INTO urls(host_id, url, crawled) VALUES(?, ?, 0)"
				if _, errInTx := tx.Exec(query, hostID, sanitized.String()); errInTx != nil {
					return errInTx
				}

				return nil
			})
			if err != nil {
				return
			}
		}
	}()
}

func (f *redisPubSubURLFrontier) Seeding(url *www.SanitizedURL) error {
	stream := f.streamName(uint16(f.computeDestinationGWN(url)))
	if _, err := f.pub.Do("RPUSH", stream, url.String()); err != nil {
		return err
	}
	return nil
}

// TODO: 対象のGWNが同じURLは1つのRPUSHで送信されるようにする
func (f *redisPubSubURLFrontier) Push(ctx context.Context, spawned *gokurou.SpawnedURL) error {
	if _, err := f.pub.Do("MULTI"); err != nil {
		return err
	}

	defer func() {
		if _, err := f.pub.Do("EXEC"); err != nil {
			gokurou.LoggerFromContext(ctx).Warnf("failed to redis exec: %v", err)
		}
	}()

	for _, url := range spawned.Spawned {
		if !f.isAvailableURL(url) {
			continue
		}

		stream := f.streamName(uint16(f.computeDestinationGWN(url)))
		if _, err := f.pub.Do("RPUSH", stream, url.String()); err != nil {
			return err
		}
	}

	return nil
}

func (f *redisPubSubURLFrontier) Pop(ctx context.Context) (*www.SanitizedURL, error) {
	now := time.Now().Unix()
	hostID, noHost, err := f.queryRowAsInt64("SELECT id FROM hosts WHERE crawlable_at <= ? LIMIT 1", now)
	if err != nil {
		return nil, err
	}
	if noHost {
		return nil, nil
	}

	var urlID int64
	var url string
	if err := f.localDB.QueryRow("SELECT id, url FROM urls WHERE host_id = ? AND crawled = 0", hostID).Scan(&urlID, &url); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	sanitized, err := www.SanitizedURLFromString(url)
	if err != nil {
		return nil, err
	}

	crawledCount, _, err := f.queryRowAsInt64("SELECT COUNT(*) FROM urls WHERE host_id = ? AND crawled = 1", hostID)
	if err != nil {
		return nil, err
	}

	var nextInterval int64
	if crawledCount+1 >= maxURLPerHost {
		nextInterval = 3600 * 24 * 365
	} else {
		nextInterval = 120
	}

	err = beginTx(f.localDB, func(tx *sql.Tx) error {
		if _, err := tx.Exec("UPDATE urls SET crawled = 1 WHERE id = ?", urlID); err != nil {
			return err
		}
		if _, err := tx.Exec("UPDATE hosts SET crawlable_at = ? WHERE id = ?", now+nextInterval, hostID); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return sanitized, nil
}

func (f *redisPubSubURLFrontier) Finish() error {
	errors := []error{
		f.pub.Close(),
		<-f.subCh,
		f.sub.Close(),
		f.localDB.Close(),
	}

	for _, err := range errors {
		if err != nil {
			return err
		}
	}

	return nil
}

func (f *redisPubSubURLFrontier) Reset() error {
	if _, err := f.pub.Do("FLUSHALL"); err != nil {
		return err
	}

	if err := f.Finish(); err != nil {
		return err
	}

	files, err := filepath.Glob(filepath.Join(filepath.Dir(f.localDBPath), "*.sqlite"))
	for _, file := range files {
		if err = os.Remove(file); err != nil {
			return err
		}
	}

	return nil
}

func (f *redisPubSubURLFrontier) streamName(gwn uint16) string {
	return fmt.Sprintf("url_stream_%d", gwn)
}

func (f *redisPubSubURLFrontier) computeDestinationGWN(url *www.SanitizedURL) uint {
	sldAndTLD := strings.Split(url.Host(), ".")
	if len(sldAndTLD) > 2 {
		sldAndTLD = sldAndTLD[len(sldAndTLD)-2:]
	}

	// hash.Hash32のWriteの実装を読めば分かるが、これは絶対にエラーを返さない
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(strings.Join(sldAndTLD, ".")))

	return (uint(hash.Sum32()) % f.totalWorkers) + 1
}

func (f *redisPubSubURLFrontier) isAvailableURL(url *www.SanitizedURL) bool {
	if len(f.tldFilter) == 0 {
		return true
	}

	tld := url.TLD()
	for _, fTLD := range f.tldFilter {
		if fTLD == tld {
			return true
		}
	}
	return false
}

func (f *redisPubSubURLFrontier) subscribe(streamName string) (*www.SanitizedURL, error) {
	values, err := redis.Values(f.sub.Do("BLPOP", streamName, 1))
	if err != nil {
		return nil, err
	}
	if values[0] == nil {
		return nil, nil
	}

	url, err := redis.String(values[1], nil)
	if err != nil {
		return nil, err
	}

	sanitized, err := www.SanitizedURLFromString(url)
	if err != nil {
		return nil, err
	}

	return sanitized, nil
}

func (f *redisPubSubURLFrontier) queryRowAsInt64(query string, param ...interface{}) (int64, bool, error) {
	var value int64
	if err := f.localDB.QueryRow(query, param...).Scan(&value); err != nil {
		if err == sql.ErrNoRows {
			return 0, true, nil
		} else {
			return 0, false, err
		}
	}

	return value, false, nil
}

func (f *redisPubSubURLFrontier) queryRowsAsStrings(query string, countEstimation int, param ...interface{}) ([]string, error) {
	rows, err := f.localDB.Query(query, param...)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	results := make([]string, 0, countEstimation)
	var s string
	for rows.Next() {
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		results = append(results, s)
	}

	return results, nil
}
