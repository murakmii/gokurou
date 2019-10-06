package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

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
	Namespace            string `json:"namespace"`
	CrawledCountDimName  string `json:"crawled_count_dim_name"`
	CrawledCountDimValue string `json:"crawled_count_dim_value"`
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
			Name:      "seeding",
			Usage:     "Seeding initial URL",
			UsageText: "gokurou seeding [command options]",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:     "url,u",
					Usage:    "seed `URL`",
					Required: true,
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

	if err = gokurou.Seeding(conf, c.String("url")); err != nil {
		return xerrors.Errorf("failed to seeding: %w", err)
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
		conf.Options["built_in.tracer.crawled_count_dimention_name"] = configContent.Tracer.CrawledCountDimName
		conf.Options["built_in.tracer.crawed_count_dimention_value"] = configContent.Tracer.CrawledCountDimValue
	}

	return conf, nil
}
