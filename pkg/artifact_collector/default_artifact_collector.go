package artifact_collector

import (
	"bytes"
	"fmt"
	"time"

	"github.com/murakmii/gokurou/pkg"

	"github.com/google/uuid"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/aws/aws-sdk-go/aws/credentials"

	"github.com/aws/aws-sdk-go/aws/session"
)

type defaultArtifactCollector struct {
	storage  distributedStorage
	prefix   string
	buffer   *bytes.Buffer
	bufCount uint8
	errCount uint8
}

type distributedStorage interface {
	put(key string, data []byte) error
}

type s3Storage struct {
	s3     *s3.S3
	bucket string
}

func (s *s3Storage) put(key string, data []byte) error {
	obj := &s3.PutObjectInput{
		ACL:    aws.String("private"),
		Body:   bytes.NewReader(data),
		Key:    aws.String(key),
		Bucket: aws.String(s.bucket),
	}

	_, err := s.s3.PutObject(obj)
	return err
}

func NewDefaultArtifactCollector() pkg.ArtifactCollector {
	return &defaultArtifactCollector{}
}

func (c *defaultArtifactCollector) DeclareBufferSize() uint8 {
	return 10
}

func (c *defaultArtifactCollector) Init(conf *pkg.Configuration) error {
	awsID, err := conf.FetchAdvancedAsString("AWS_ACCESS_ID")
	if err != nil {
		return err
	}

	awsSecret, err := conf.FetchAdvancedAsString("AWS_ACCESS_SECRET")
	if err != nil {
		return err
	}

	bucket, err := conf.FetchAdvancedAsString("DEFAULT_ARTIFACT_COLLECTOR_BUCKET")
	if err != nil {
		return err
	}

	sess, err := session.NewSession()
	if err != nil {
		return fmt.Errorf("can't create aws session: %v", err)
	}

	cred := credentials.NewStaticCredentials(awsID, awsSecret, "")
	c.storage = &s3Storage{
		s3:     s3.New(sess, aws.NewConfig().WithCredentials(cred).WithRegion("ap-northeast-1")),
		bucket: bucket,
	}

	c.prefix, err = conf.FetchAdvancedAsString("DEFAULT_ARTIFACT_COLLECTOR_PREFIX")
	if err != nil {
		return err
	}

	c.buffer = bytes.NewBuffer(nil)
	c.bufCount = 0
	c.errCount = 0

	return nil
}

func (c *defaultArtifactCollector) Collect(artifact interface{}) error {
	b, ok := artifact.([]byte)
	if !ok {
		return fmt.Errorf("can't cast artifact to []byte")
	}

	c.buffer.Write(b)
	c.buffer.WriteByte('\n')
	c.bufCount++

	if c.bufCount < 100 {
		return nil
	}

	return c.upload()
}

func (c *defaultArtifactCollector) Finish() error {
	if c.bufCount == 0 {
		return nil
	}

	return c.upload()
}

func (c *defaultArtifactCollector) buildNewKey() (string, error) {
	u, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s/%s/%s.log", c.prefix, time.Now().Format("2006-01-02-15-04"), u.String()), nil
}

func (c *defaultArtifactCollector) upload() error {
	key, err := c.buildNewKey()
	if err != nil {
		return err
	}

	if err = c.storage.put(key, c.buffer.Bytes()); err != nil {
		c.errCount++
		if c.errCount >= 5 {
			return fmt.Errorf("can't upload artifact: %v", err)
		}

		return nil
	}

	c.buffer.Reset()
	c.bufCount = 0
	c.errCount = 0

	return nil
}
