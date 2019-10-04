package artifact_gatherer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/murakmii/gokurou/pkg/gokurou"

	"golang.org/x/xerrors"

	"github.com/google/uuid"

	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/aws/aws-sdk-go/aws/credentials"

	"github.com/aws/aws-sdk-go/aws/session"
)

// 保存先ストレージの詳細を抽象化しておく
type artifactStorage interface {
	put(key string, data []byte) error
}

// デフォルトのbuiltInArtifactGatherer
// 受け取ったバイト列を改行区切りでストレージに保存する
type builtInArtifactGatherer struct {
	storage     artifactStorage
	keyPrefix   string
	buffer      *bytes.Buffer
	maxBuffered int
}

const (
	awsS3EndpointConfKey = "built_in.aws.s3_endpoint"
	keyPrefixConfKey     = "built_in.artifact_gatherer.gathered_item_prefix"
	bucketConfKey        = "built_in.artifact_gatherer.bucket"
)

// 新しいArtifactGathererを生成する
func BuiltInArtifactGathererProvider(_ context.Context, conf *gokurou.Configuration) (gokurou.ArtifactGatherer, error) {
	store, err := newS3StoreFromConfiguration(conf)
	if err != nil {
		return nil, err
	}

	return &builtInArtifactGatherer{
		storage:     store,
		keyPrefix:   conf.MustOptionAsString(keyPrefixConfKey),
		buffer:      bytes.NewBuffer(nil),
		maxBuffered: 100000,
	}, nil
}

// 結果収集。定期的にストレージにアップロードする
func (ag *builtInArtifactGatherer) Collect(ctx context.Context, artifact interface{}) error {
	marshaled, err := json.Marshal(artifact)
	if err != nil {
		gokurou.LoggerFromContext(ctx).Warnf("failed to marshal artifact: %v", err)
		return nil
	}

	ag.buffer.Write(marshaled)
	ag.buffer.WriteByte('\n')

	if ag.buffer.Len() < ag.maxBuffered {
		return nil
	}

	return ag.upload()
}

// 終了時はバッファに残った結果をアップロード
func (ag *builtInArtifactGatherer) Finish() error {
	if ag.buffer.Len() == 0 {
		return nil
	}

	return ag.upload()
}

// アップロード時のキーを生成する
func (ag *builtInArtifactGatherer) buildNewKey() (string, error) {
	u, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s/%s/%s.log", ag.keyPrefix, time.Now().Format("2006-01-02-15-04"), u.String()), nil
}

// ストレージへのアップロード処理
func (ag *builtInArtifactGatherer) upload() error {
	key, err := ag.buildNewKey()
	if err != nil {
		return xerrors.Errorf("failed to build artifact key: %v", err)
	}

	if err = ag.storage.put(key, ag.buffer.Bytes()); err != nil {
		return xerrors.Errorf("failed to upload artifact: %v", err)
	}

	ag.buffer.Reset()
	return nil
}

// artifactStoreを実装したS3を対象にしたストレージ
type s3ArtifactStorage struct {
	s3     *s3.S3
	bucket string
}

// 新しくs3ArtifactStoreを生成する
func newS3StoreFromConfiguration(conf *gokurou.Configuration) (artifactStorage, error) {
	sess, err := session.NewSession()
	if err != nil {
		return nil, xerrors.Errorf("can't create aws session: %v", err)
	}

	cred := credentials.NewStaticCredentials(conf.AwsAccessKeyID, conf.AwsSecretAccessKey, "")
	s3config := aws.NewConfig().
		WithCredentials(cred).
		WithRegion(conf.AwsRegion).
		WithMaxRetries(5)

	endpoint := conf.OptionAsString(awsS3EndpointConfKey)
	if endpoint != nil {
		s3config = s3config.WithEndpoint(*endpoint).WithS3ForcePathStyle(true)
	}

	return &s3ArtifactStorage{
		s3:     s3.New(sess, s3config),
		bucket: conf.MustOptionAsString(bucketConfKey),
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
