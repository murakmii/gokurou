# gokurou

3日で1億ページクロールしたい。  
つまり500crawl/secを目指すぞ。

# 進捗

2019/09/29時点ではMySQL, Redis, S3が必要。  
`docker-compose.yml`でバチっとすれば全部立ち上がる。後は以下のような`main.go`を書いて`go run main.go`する。
(パスワードやらは全部`docker-compose.yml`に書いているローカル動作確認用のもの)

```go
package main

import (
	"github.com/murakmii/gokurou/pkg/gokurou/crawler"
	"github.com/murakmii/gokurou/pkg/gokurou/url_frontier"

	"github.com/murakmii/gokurou/pkg/gokurou"

	"github.com/murakmii/gokurou/pkg/gokurou/artifact_gatherer"
	"github.com/murakmii/gokurou/pkg/gokurou/coordinator"
)

func main() {
	conf := gokurou.NewConfiguration(1, 1)
	conf.CoordinatorProvider = coordinator.BuiltInCoordinatorProvider
	conf.ArtifactGathererProvider = artifact_gatherer.BuiltInArtifactGathererProvider
	conf.URLFrontierProvider = url_frontier.BuiltInURLFrontierProvider
	conf.CrawlerProvider = crawler.BuiltInCrawlerProvider
	conf.DebugLevelLogging = true

	conf.Options["built_in.aws.region"] = "ap-northeast-1"
	conf.Options["built_in.aws.access_key_id"] = "gokurou-s3-access-key"
	conf.Options["built_in.aws.secret_access_key"] = "gokurou-s3-secret-key"
	conf.Options["built_in.aws.s3_endpoint"] = "http://localhost:11113"
	conf.Options["built_in.artifact_gatherer.bucket"] = "gokurou-dev"
	conf.Options["built_in.artifact_gatherer.gathered_item_prefix"] = "crawled"

	conf.Options["built_in.redis_url"] = "redis://localhost:11111"

	conf.Options["built_in.crawler.header_ua"] = "gokurou (+https://github.com/murakmii/gokurou)"
	conf.Options["built_in.crawler.primary_ua"] = "gokurou"
	conf.Options["built_in.crawler.secondary_ua"] = "googlebot"

	conf.Options["built_in.url_frontier.shared_db_source"] = "root:gokurou1234@tcp(127.0.0.1:11112)/gokurou_dev?charset=utf8mb4,utf&interpolateParams=true"
	conf.Options["built_in.url_frontier.local_db_path"] = "tmp/localdb-%d.sqlite"

	gokurou.Start(conf)
}
```
