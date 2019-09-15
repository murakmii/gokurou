package pkg

type ArtifactCollector interface {
	DeclareBufferSize() uint8
	Init(conf *Configuration) error
	Collect(artifact interface{}) error
	Finish() error
}
