package pkg

import (
	"context"

	"golang.org/x/xerrors"
)

type Configuration struct {
	Workers              uint16
	Machines             uint8
	NewArtifactCollector func(ctx context.Context, conf *Configuration) (ArtifactCollector, error)
	NewURLFrontier       func(ctx context.Context, conf *Configuration) (URLFrontier, error)
	NewCrawler           func(ctx context.Context, conf *Configuration) (Crawler, error)
	NewSynchronizer      func(conf *Configuration) (Synchronizer, error)
	Advanced             map[string]interface{}
}

func NewConfiguration(workers uint16) *Configuration {
	return &Configuration{
		Workers:  workers,
		Advanced: make(map[string]interface{}),
	}
}

func (c *Configuration) TotalWorkers() uint16 {
	return c.Workers * uint16(c.Machines)
}

func (c *Configuration) FetchAdvancedAsString(key string) (string, error) {
	value, ok := c.Advanced[key]
	if !ok {
		return "", xerrors.Errorf("configuration key '%s' not found", key)
	}

	asStr, ok := value.(string)
	if !ok {
		return "", xerrors.Errorf("configuration key '%s' has not value as string", key)
	}

	if len(asStr) == 0 {
		return "", xerrors.Errorf("configuration key '%s' not found", key)
	}

	return asStr, nil
}

func (c *Configuration) MustFetchAdvancedAsString(key string) string {
	str, err := c.FetchAdvancedAsString(key)
	if err != nil {
		panic(err)
	}

	return str
}
