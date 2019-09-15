package pkg

import (
	"fmt"
)

type Configuration struct {
	Workers              uint16
	NewArtifactCollector func() ArtifactCollector
	NewURLFrontier       func() URLFrontier
	NewSynchronizer      func() Synchronizer
	Advanced             map[string]interface{}
}

func NewConfiguration(workers uint16) *Configuration {
	return &Configuration{
		Workers:  workers,
		Advanced: make(map[string]interface{}),
	}
}

func (c *Configuration) FetchAdvancedAsString(key string) (string, error) {
	value, ok := c.Advanced[key]
	if !ok {
		return "", fmt.Errorf("key '%s' not found", key)
	}

	asStr, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("key '%s' has not value as string", key)
	}

	if len(asStr) == 0 {
		return "", fmt.Errorf("key '%s' not found", key)
	}

	return asStr, nil
}
