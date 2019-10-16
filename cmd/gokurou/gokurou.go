package main

import (
	"bufio"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/xwb1989/sqlparser"

	"github.com/murakmii/gokurou/pkg/gokurou/tracer"

	"golang.org/x/xerrors"

	"github.com/murakmii/gokurou/pkg/gokurou/artifact_gatherer"
	"github.com/murakmii/gokurou/pkg/gokurou/coordinator"
	"github.com/murakmii/gokurou/pkg/gokurou/crawler"
	"github.com/murakmii/gokurou/pkg/gokurou/url_frontier"

	"github.com/murakmii/gokurou/pkg/gokurou"

	"github.com/urfave/cli"
)

type config struct {
	Workers           uint `json:"workers"`
	Machines          uint `json:"machines"`
	DebugLevelLogging bool `json:"debug_level_logging"`
	JSONLogging       bool `json:"json_logging"`

	Aws         awsConfig         `json:"aws"`
	Artifact    artifactConfig    `json:"artifact"`
	Coordinator coordinatorConfig `json:"coordinator"`
	Crawling    crawlingConfig    `json:"crawling"`
	URLFrontier urlFrontierConfig `json:"url_frontier"`
	Tracer      tracerConfig      `json:"tracer"`
}

type awsConfig struct {
	Region          string `json:"region"`
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	S3EndPoint      string `json:"s3_endpoint"`
}

type artifactConfig struct {
	Bucket    string `json:"bucket"`
	KeyPrefix string `json:"key_prefix"`
}

type coordinatorConfig struct {
	RedisURL string `json:"redis_url"`
}

type crawlingConfig struct {
	HeaderUA    string `json:"header_ua"`
	PrimaryUA   string `json:"primary_ua"`
	SecondaryUA string `json:"secondary_ua"`
}

type urlFrontierConfig struct {
	SharedDBSource string   `json:"shared_db_source"`
	LocalDBPath    string   `json:"local_db_path"`
	TLDFilter      []string `json:"tld_filter"`
}

type tracerConfig struct {
	Namespace string `json:"namespace"`
	DimName   string `json:"dim_name"`
	DimValue  string `json:"dim_value"`
}

func main() {
	app := cli.NewApp()
	app.Name = "gokurou"
	app.Usage = "Let's crawl web!"
	app.UsageText = "gokurou [global options] command [arguments...]"
	app.Version = "0.0.1"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:     "config,c",
			Usage:    "configuration file `PATH`",
			Required: true,
		},
	}

	app.Commands = []cli.Command{
		{
			Name:      "genseed-rss",
			Usage:     "Generate seed file from links in RSS1.0(XML)",
			UsageText: "gokurou genseed-rss [URLs for RSS]",
			Action:    genSeedRssCommand,
		},
		{
			Name:      "genseed-wiki",
			Usage:     "Generate seed file from externallinks.sql(Wikipedia dumps)",
			UsageText: "gokurou genseed-wiki [command options]",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:     "file,f",
					Usage:    "path to `FILE` contains externallinks.sql of Wikipedia",
					Required: true,
				},
			},
			Action: genSeedWikiCommand,
		},
		{
			Name:      "seeding",
			Usage:     "Seeding initial URL",
			UsageText: "gokurou seeding [command options]",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "url,u",
					Usage: "seed `URL`",
				},
				cli.StringFlag{
					Name:  "file,f",
					Usage: "specify `FILE` contains seed URLs",
				},
			},
			Action: seedingCommand,
		},
		{
			Name:      "crawl",
			Usage:     "Start to crawl",
			UsageText: "gokurou -c PATH crawl",
			Action:    crawlCommand,
		},
		{
			Name:      "reset",
			Usage:     "Reset all data(exclude 'artifact')",
			UsageText: "gokurou -c PATH reset",
			Action:    resetCommand,
		},
	}

	if err := app.Run(os.Args); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "\nERROR DETECTED:\n   %v\n", err)
	}
}

// データ初期化コマンド
func resetCommand(c *cli.Context) error {
	conf, err := buildConfiguration(c.GlobalString("config"))
	if err != nil {
		return xerrors.Errorf("failed to load configuration: %v", err)
	}

	return gokurou.Reset(conf)
}

// クロール開始コマンド
func crawlCommand(c *cli.Context) error {
	conf, err := buildConfiguration(c.GlobalString("config"))
	if err != nil {
		return xerrors.Errorf("failed to load configuration: %v", err)
	}

	if err = gokurou.Start(conf); err != nil {
		return xerrors.Errorf("failed to crawl: %w", err)
	}

	return nil
}

// 初期URL設定コマンド
func seedingCommand(c *cli.Context) error {
	conf, err := buildConfiguration(c.GlobalString("config"))
	if err != nil {
		return xerrors.Errorf("failed to load configuration: %v", err)
	}

	seedURLs := make([]string, 0)

	if len(c.String("url")) > 0 {
		seedURLs = append(seedURLs, c.String("url"))
	}

	if len(c.String("file")) > 0 {
		f, err := os.Open(c.String("file"))
		if err != nil {
			return err
		}
		defer f.Close()

		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			seedURLs = append(seedURLs, scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			return xerrors.Errorf("failed to load seed file: %v", err)
		}
	}

	if err := gokurou.Seeding(conf, seedURLs); err != nil {
		return xerrors.Errorf("failed to seeding: %v", err)
	}

	return nil
}

func genSeedRssCommand(c *cli.Context) error {
	conf, err := buildConfiguration(c.GlobalString("config"))
	if err != nil {
		return xerrors.Errorf("failed to load configuration: %v", err)
	}

	type result struct {
		urls []string
		err  error
	}

	ua := conf.MustOptionAsString("built_in.crawler.header_ua")
	resultCh := make(chan *result)
	client := &http.Client{
		Transport: http.DefaultTransport,
		Timeout:   10 * time.Second,
	}
	defer client.CloseIdleConnections()

	getter := func(url string) *result {
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return &result{err: err}
		}
		req.Header.Set("User-Agent", ua)

		resp, err := client.Do(req)
		if err != nil {
			return &result{err: err}
		}

		defer resp.Body.Close()

		decoder := xml.NewDecoder(resp.Body)
		waitLinkText := false
		urls := make([]string, 0, 100)

		for {
			token, err := decoder.Token()
			if err == io.EOF {
				break
			} else if err != nil {
				return &result{err: err}
			}

			switch t := token.(type) {
			case xml.StartElement:
				waitLinkText = t.Name.Local == "link"
			case xml.CharData:
				if waitLinkText {
					waitLinkText = false
					urls = append(urls, string(t))
				}
			}
		}

		return &result{urls: urls}
	}

	rssURLs := c.Args()
	for _, url := range rssURLs {
		go func() {
			resultCh <- getter(url)
		}()
	}

	var latestErr error
	for i := 0; i < len(rssURLs); i++ {
		received := <-resultCh
		if received.err != nil {
			latestErr = xerrors.Errorf("failed to get rss: %v", received.err)
		} else {
			for _, url := range received.urls {
				fmt.Println(url)
			}
		}
	}

	return latestErr
}

// TODO: 各種バッファの値を調整できるようにする
// TODO: 並列化
func genSeedWikiCommand(c *cli.Context) error {
	f, err := os.Open(c.String("file"))
	if err != nil {
		return err
	}
	defer f.Close()

	bufSize := 2000000
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, bufSize), bufSize)

	rand.Seed(time.Now().Unix())
	log.SetOutput(ioutil.Discard)

	for scanner.Scan() {
		stmt, err := sqlparser.Parse(scanner.Text())
		if err != nil {
			continue
		}

		switch insert := stmt.(type) {
		case *sqlparser.Insert:
			values := insert.Rows.(sqlparser.Values)
			choicedTuple := values[rand.Intn(len(values))]
			url := string(choicedTuple[2].(*sqlparser.SQLVal).Val)

			if strings.Contains(url, "wikimedia") ||
				strings.Contains(url, ".pdf") ||
				strings.HasPrefix(url, "//") {
				continue
			}
			fmt.Println(url)

		default:
			continue
		}
	}
	if err := scanner.Err(); err != nil {
		return xerrors.Errorf("failed to parse file: %v", err)
	}

	return nil
}

// Configuration生成
func buildConfiguration(path string) (*gokurou.Configuration, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	content, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}

	configContent := config{}
	if err = json.Unmarshal(content, &configContent); err != nil {
		return nil, err
	}

	conf := gokurou.NewConfiguration(configContent.Workers, configContent.Machines)
	conf.DebugLevelLogging = configContent.DebugLevelLogging
	conf.JSONLogging = configContent.JSONLogging

	conf.AwsRegion = configContent.Aws.Region
	conf.AwsAccessKeyID = configContent.Aws.AccessKeyID
	conf.AwsSecretAccessKey = configContent.Aws.SecretAccessKey
	if len(configContent.Aws.S3EndPoint) > 0 {
		conf.AwsS3EndPoint = configContent.Aws.S3EndPoint
	}

	conf.CoordinatorProvider = coordinator.BuiltInCoordinatorProvider
	conf.ArtifactGathererProvider = artifact_gatherer.BuiltInArtifactGathererProvider
	conf.URLFrontierProvider = url_frontier.BuiltInURLFrontierProvider
	conf.CrawlerProvider = crawler.BuiltInCrawlerProvider

	conf.Options["built_in.artifact_gatherer.bucket"] = configContent.Artifact.Bucket
	conf.Options["built_in.artifact_gatherer.gathered_item_prefix"] = configContent.Artifact.KeyPrefix

	conf.Options["built_in.redis_url"] = configContent.Coordinator.RedisURL

	conf.Options["built_in.crawler.header_ua"] = configContent.Crawling.HeaderUA
	conf.Options["built_in.crawler.primary_ua"] = configContent.Crawling.PrimaryUA
	conf.Options["built_in.crawler.secondary_ua"] = configContent.Crawling.SecondaryUA

	conf.Options["built_in.url_frontier.tld_filter"] = configContent.URLFrontier.TLDFilter
	conf.Options["built_in.url_frontier.shared_db_source"] = configContent.URLFrontier.SharedDBSource
	conf.Options["built_in.url_frontier.local_db_path"] = configContent.URLFrontier.LocalDBPath

	if !conf.AwsConfigurationMayBeDummy() {
		conf.TracerProvider = tracer.NewMetricsTracer
		conf.Options["built_in.tracer.namespace"] = configContent.Tracer.Namespace
		conf.Options["built_in.tracer.dimention_name"] = configContent.Tracer.DimName
		conf.Options["built_in.tracer.dimention_value"] = configContent.Tracer.DimValue
	}

	return conf, nil
}
