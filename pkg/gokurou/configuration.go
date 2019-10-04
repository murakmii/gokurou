package gokurou

import (
	"context"

	"golang.org/x/xerrors"
)

type (
	ArtifactGathererProviderFunc func(ctx context.Context, conf *Configuration) (ArtifactGatherer, error)
	URLFrontierProviderFunc      func(ctx context.Context, conf *Configuration) (URLFrontier, error)
	CrawlerProviderFunc          func(ctx context.Context, conf *Configuration) (Crawler, error)
	CoordinatorProviderFunc      func(conf *Configuration) (Coordinator, error)
	TracerProviderFunc           func(conf *Configuration) (Tracer, error)
)

type Configuration struct {
	Workers           uint
	Machines          uint
	DebugLevelLogging bool
	JSONLogging       bool

	AwsRegion          string
	AwsAccessKeyID     string
	AwsSecretAccessKey string

	ArtifactGathererProvider ArtifactGathererProviderFunc
	URLFrontierProvider      URLFrontierProviderFunc
	CrawlerProvider          CrawlerProviderFunc
	CoordinatorProvider      CoordinatorProviderFunc
	TracerProvider           TracerProviderFunc

	Options map[string]interface{}
}

func NewConfiguration(workers, machines uint) *Configuration {
	return &Configuration{
		Workers:  workers,
		Machines: machines,
		Options:  make(map[string]interface{}),
	}
}

func (c *Configuration) TotalWorkers() uint {
	return c.Workers * c.Machines
}

func (c *Configuration) OptionAsString(key string) *string {
	option, exists := c.Options[key]
	if !exists {
		return nil
	}

	str, ok := option.(string)
	if !ok {
		return nil
	}

	return &str
}

func (c *Configuration) MustOptionAsString(key string) string {
	str := c.OptionAsString(key)
	if str == nil {
		panic(xerrors.Errorf("required option: '%s' was NOT set", key))
	}

	return *str
}
