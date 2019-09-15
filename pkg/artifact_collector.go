package pkg

type ArtifactCollector interface {
	Collect(artifact interface{}) error
	Finish() error
}
