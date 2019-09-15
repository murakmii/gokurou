package artifact_collector

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/murakmii/gokurou/pkg"

	"github.com/google/uuid"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/aws/aws-sdk-go/aws/credentials"

	"github.com/aws/aws-sdk-go/aws/session"
)

// 保存先ストレージの詳細を抽象化しておく
type artifactStorage interface {
	put(key string, data []byte) error
}

// デフォルトのArtifactCollector
// 受け取ったバイト列を改行区切りでストレージに保存する
type defaultArtifactCollector struct {
	storage     artifactStorage
	prefix      string
	buffer      *bytes.Buffer
	bufCount    uint8
	maxBuffered uint8
	errCount    uint8
}

// 新しいNewDefaultArtifactCollectorを生成する
func NewDefaultArtifactCollector(_ context.Context, conf *pkg.Configuration) (pkg.ArtifactCollector, error) {
	store, err := newS3StoreFromConfiguration(conf)
	if err != nil {
		return nil, err
	}

	prefix, err := conf.FetchAdvancedAsString("DEFAULT_ARTIFACT_COLLECTOR_PREFIX")
	if err != nil {
		return nil, err
	}

	return &defaultArtifactCollector{
		storage:     store,
		prefix:      prefix,
		buffer:      bytes.NewBuffer(nil),
		bufCount:    0,
		maxBuffered: 100,
		errCount:    0,
	}, nil
}

// 結果収集。定期的にストレージにアップロードする
func (c *defaultArtifactCollector) Collect(artifact interface{}) error {
	b, ok := artifact.([]byte)
	if !ok {
		return fmt.Errorf("can't cast artifact to []byte")
	}

	c.buffer.Write(b)
	c.buffer.WriteByte('\n')
	c.bufCount++

	if c.bufCount < c.maxBuffered {
		return nil
	}

	return c.upload()
}

// 終了時はバッファに残った結果をアップロード
func (c *defaultArtifactCollector) Finish() error {
	if c.bufCount == 0 {
		return nil
	}

	return c.upload()
}

// アップロード時のキーを生成する
func (c *defaultArtifactCollector) buildNewKey() (string, error) {
	u, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s/%s/%s.log", c.prefix, time.Now().Format("2006-01-02-15-04"), u.String()), nil
}

// ストレージへのアップロード処理
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

// artifactStoreを実装したS3を対象にしたストレージ
type s3ArtifactStorage struct {
	s3     *s3.S3
	bucket string
}

// 新しくs3ArtifactStoreを生成する
func newS3StoreFromConfiguration(conf *pkg.Configuration) (*s3ArtifactStorage, error) {
	awsRegion, err := conf.FetchAdvancedAsString("AWS_REGION")
	if err != nil {
		return nil, err
	}

	awsID, err := conf.FetchAdvancedAsString("AWS_ACCESS_ID")
	if err != nil {
		return nil, err
	}

	awsSecret, err := conf.FetchAdvancedAsString("AWS_ACCESS_SECRET")
	if err != nil {
		return nil, err
	}

	sess, err := session.NewSession()
	if err != nil {
		return nil, fmt.Errorf("can't create aws session: %v", err)
	}

	cred := credentials.NewStaticCredentials(awsID, awsSecret, "")

	bucket, err := conf.FetchAdvancedAsString("DEFAULT_ARTIFACT_COLLECTOR_BUCKET")
	if err != nil {
		return nil, err
	}

	return &s3ArtifactStorage{
		s3:     s3.New(sess, aws.NewConfig().WithCredentials(cred).WithRegion(awsRegion)),
		bucket: bucket,
	}, nil
}

// 結果をS3のオブジェクトとして保存する
func (s *s3ArtifactStorage) put(key string, data []byte) error {
	obj := &s3.PutObjectInput{
		ACL:    aws.String("private"),
		Body:   bytes.NewReader(data),
		Key:    aws.String(key),
		Bucket: aws.String(s.bucket),
	}

	_, err := s.s3.PutObject(obj)
	return err
}
