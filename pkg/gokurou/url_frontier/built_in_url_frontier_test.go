package url_frontier

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"

	"github.com/murakmii/gokurou/pkg/gokurou"

	"github.com/murakmii/gokurou/pkg/gokurou/www"

	"github.com/google/uuid"
)

func buildContext() context.Context {
	logger := logrus.New()
	logger.SetOutput(ioutil.Discard)

	ctx := gokurou.ContextWithLogger(context.Background(), logrus.NewEntry(logger))
	return gokurou.ContextWithGWN(ctx, uint16(1))
}

func buildURLFrontier(ctx context.Context) *builtInURLFrontier {
	conf := gokurou.NewConfiguration(1, 1)
	conf.Options["built_in.url_frontier.shared_db_source"] = "root:gokurou1234@tcp(127.0.0.1:11112)/gokurou_test?charset=utf8mb4,utf&interpolateParams=true"
	conf.Options["built_in.url_frontier.local_db_path"] = nil

	f, err := BuiltInURLFrontierProvider(ctx, conf)
	if err != nil {
		panic(err)
	}

	frontier := f.(*builtInURLFrontier)
	if _, err = frontier.sharedDB.Exec("TRUNCATE urls"); err != nil {
		panic(err)
	}

	return frontier
}

func buildRandomHostURL() *www.SanitizedURL {
	u, err := uuid.NewRandom()
	if err != nil {
		panic(err)
	}

	url, err := www.SanitizedURLFromString(fmt.Sprintf("http://example-%s.com", u.String()))
	if err != nil {
		panic(err)
	}

	return url
}

func TestDefaultURLFrontier_Push(t *testing.T) {
	ctx := buildContext()
	frontier := buildURLFrontier(ctx)

	defer frontier.Finish()

	t.Run("十分にPushしたことがない場合、即座にPushする", func(t *testing.T) {
		url := buildRandomHostURL()
		spawned := &gokurou.SpawnedURL{
			From:    buildRandomHostURL(),
			Spawned: []*www.SanitizedURL{url},
		}

		if err := frontier.Push(ctx, spawned); err != nil {
			t.Errorf("Push(%s) = %v", url, err)
		}

		var pushed string
		err := frontier.sharedDB.QueryRow("SELECT tab_joined_url FROM urls WHERE id = (SELECT MAX(id) FROM urls)").Scan(&pushed)
		if err != nil {
			panic(err)
		}

		if pushed != url.String() {
			t.Errorf("Push(%s) does NOT push valid url(%s)", url, pushed)
		}
	})

	t.Run("十分にPushしている場合、バッファしてからPushする", func(t *testing.T) {
		frontier.pushedCount[1] = 999
		want := make([]string, 50)
		for i := 1; i <= 50; i++ {
			url := buildRandomHostURL()
			want[i-1] = url.String()

			spawned := &gokurou.SpawnedURL{
				From:    buildRandomHostURL(),
				Spawned: []*www.SanitizedURL{url},
			}

			if err := frontier.Push(ctx, spawned); err != nil {
				t.Errorf("Push(%s) = %v", url, err)
				break
			}
		}

		var pushed string
		err := frontier.sharedDB.QueryRow("SELECT tab_joined_url FROM urls WHERE id = (SELECT MAX(id) FROM urls)").Scan(&pushed)
		if err != nil {
			panic(err)
		}

		if pushed != strings.Join(want, "\t") {
			t.Errorf("Push([URL]) does NOT push tab joined url(%s)", pushed)
		}
	})
}

func TestDefaultURLFrontier_Pop(t *testing.T) {
	tests := []struct {
		name  string
		setup func(frontier *builtInURLFrontier)
		want  []sql.NullString
	}{
		{
			name:  "PopするURLがない場合、nilを返す",
			setup: func(_ *builtInURLFrontier) {},
			want:  []sql.NullString{{}},
		},
		{
			name: "PopするURLがある場合、それを返す",
			setup: func(frontier *builtInURLFrontier) {
				if _, err := frontier.sharedDB.Exec("INSERT INTO urls(gwn, tab_joined_url) VALUES(1, 'http://example.com')"); err != nil {
					panic(err)
				}
			},
			want: []sql.NullString{{String: "http://example.com", Valid: true}, {}},
		},
		{
			name: "PopしたURLがクロール済みのものだった場合、次のURLを返す",
			setup: func(frontier *builtInURLFrontier) {
				if _, err := frontier.sharedDB.Exec("INSERT INTO urls(gwn, tab_joined_url) VALUES(1, 'http://example.com')"); err != nil {
					panic(err)
				}
				if _, err := frontier.sharedDB.Exec("INSERT INTO urls(gwn, tab_joined_url) VALUES(1, 'http://www.example.com')"); err != nil {
					panic(err)
				}
				if _, err := frontier.localDB.Exec("INSERT INTO crawled_hosts VALUES('example.com')"); err != nil {
					panic(err)
				}
			},
			want: []sql.NullString{{String: "http://www.example.com", Valid: true}, {}},
		},
		{
			name: "複数URLをバッファしつつ返す",
			setup: func(frontier *builtInURLFrontier) {
				if _, err := frontier.sharedDB.Exec("INSERT INTO urls(gwn, tab_joined_url) VALUES(1, 'http://example.com\thttp://www.example.com\thttp://foo.com')"); err != nil {
					panic(err)
				}
			},
			want: []sql.NullString{
				{String: "http://example.com", Valid: true},
				{String: "http://www.example.com", Valid: true},
				{String: "http://foo.com", Valid: true},
				{},
			},
		},
	}

	for _, tt := range tests {
		ctx := buildContext()
		frontier := buildURLFrontier(ctx)

		t.Run(tt.name, func(t *testing.T) {
			tt.setup(frontier)

			for _, want := range tt.want {
				got, err := frontier.Pop(ctx)
				if err != nil {
					t.Errorf("Pop() = %v", err)
				}

				if want.Valid {
					if got == nil || got.String() != want.String {
						t.Errorf("Pop() = %s, want = %s", got, want.String)
					}
				} else {
					if got != nil {
						t.Errorf("Pop() = %s, want = nil", got)
					}
				}
			}
		})

		_ = frontier.Finish()
	}
}

func TestDefaultURLFrontier_Finish(t *testing.T) {
	ctx := buildContext()
	frontier := buildURLFrontier(ctx)

	if err := frontier.Finish(); err != nil {
		t.Errorf("Finish() = %v", err)
	}
}
