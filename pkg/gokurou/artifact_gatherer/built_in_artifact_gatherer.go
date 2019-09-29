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
	prefix      string
	buffer      *bytes.Buffer
	bufCount    uint8
	maxBuffered uint8
	errCount    uint8
}

const (
	awsRegionConfKey          = "built_in.aws.region"
	awsAccessKeyIDConfKey     = "built_in.aws.access_key_id"
	awsSecretAccessKeyConfKey = "built_in.aws.secret_access_key"
	awsS3EndpointConfKey      = "built_in.aws.s3_endpoint"
	keyPrefixConfKey          = "built_in.artifact_gatherer.gathered_item_prefix"
	bucketConfKey             = "built_in.artifact_gatherer.bucket"
)

// 新しいArtifactGathererを生成する
func BuiltInArtifactGathererProvider(_ context.Context, conf *gokurou.Configuration) (gokurou.ArtifactGatherer, error) {
	store, err := newS3StoreFromConfiguration(conf)
	if err != nil {
		return nil, err
	}

	return &builtInArtifactGatherer{
		storage:     store,
		prefix:      conf.MustOptionAsString(keyPrefixConfKey),
		buffer:      bytes.NewBuffer(nil),
		bufCount:    0,
		maxBuffered: 100,
		errCount:    0,
	}, nil
}

// 結果収集。定期的にストレージにアップロードする
func (ag *builtInArtifactGatherer) Collect(artifact interface{}) error {
	marshaled, err := json.Marshal(artifact)
	if err != nil {
		return xerrors.Errorf("failed to marshal artifact: %w", err)
	}

	ag.buffer.Write(marshaled)
	ag.buffer.WriteByte('\n')
	ag.bufCount++

	if ag.bufCount < ag.maxBuffered {
		return nil
	}

	return ag.upload()
}

// 終了時はバッファに残った結果をアップロード
func (ag *builtInArtifactGatherer) Finish() error {
	if ag.bufCount == 0 {
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

	return fmt.Sprintf("%s/%s/%s.log", ag.prefix, time.Now().Format("2006-01-02-15-04"), u.String()), nil
}

// ストレージへのアップロード処理
func (ag *builtInArtifactGatherer) upload() error {
	key, err := ag.buildNewKey()
	if err != nil {
		return err
	}

	if err = ag.storage.put(key, ag.buffer.Bytes()); err != nil {
		ag.errCount++
		if ag.errCount >= 5 {
			return xerrors.Errorf("can't upload artifact: %w", err)
		}

		return nil
	}

	ag.buffer.Reset()
	ag.bufCount = 0
	ag.errCount = 0

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

	cred := credentials.NewStaticCredentials(
		conf.MustOptionAsString(awsAccessKeyIDConfKey),
		conf.MustOptionAsString(awsSecretAccessKeyConfKey),
		"",
	)

	s3config := aws.NewConfig().WithCredentials(cred).WithRegion(conf.MustOptionAsString(awsRegionConfKey))
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
