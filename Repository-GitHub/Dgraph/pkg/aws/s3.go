package aws

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	awstypes "github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type s3Client struct {
	cfg    *S3Config
	client *s3.Client
}

func NewS3Connection(cfg *S3Config) (S3Adapter, error) {
	if cfg == nil {
		return nil, fmt.Errorf("s3 config is nil")
	}
	if cfg.Region == "" {
		return nil, fmt.Errorf("aws s3 region is required")
	}
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("aws s3 bucket is required")
	}

	loadOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.Region),
	}
	// Optional static credentials
	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		creds := credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken)
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(creds))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load aws config: %w", err)
	}

	clientOpts := []func(*s3.Options){
		func(o *s3.Options) {
			if cfg.Endpoint != "" {
				o.BaseEndpoint = awstypes.String(cfg.Endpoint)
			}
			o.UsePathStyle = cfg.ForcePathStyle
		},
	}

	s3c := s3.NewFromConfig(awsCfg, clientOpts...)

	return &s3Client{cfg: cfg, client: s3c}, nil
}

func (s *s3Client) Upload(ctx context.Context, key string, contentType string, body io.Reader) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      awstypes.String(s.cfg.Bucket),
		Key:         awstypes.String(key),
		ContentType: awstypes.String(contentType),
		Body:        body,
		ACL:         types.ObjectCannedACLPrivate,
	})
	if err != nil {
		return fmt.Errorf("put object: %w", err)
	}
	return nil
}

func (s *s3Client) GetBytes(ctx context.Context, key string) ([]byte, error) {
	resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: awstypes.String(s.cfg.Bucket),
		Key:    awstypes.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("get object: %w", err)
	}
	defer resp.Body.Close()
	buf := new(bytes.Buffer)
	if _, err := io.Copy(buf, resp.Body); err != nil {
		return nil, fmt.Errorf("read object body: %w", err)
	}
	return buf.Bytes(), nil
}

func (s *s3Client) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: awstypes.String(s.cfg.Bucket),
		Key:    awstypes.String(key),
	})
	if err != nil {
		return fmt.Errorf("delete object: %w", err)
	}
	return nil
}

func (s *s3Client) HealthCheck(ctx context.Context) error {
	// simple HeadBucket via ListObjectsV2 with max keys 0 or GetBucketLocation
	_, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: awstypes.String(s.cfg.Bucket)})
	if err != nil {
		var notFound *types.NotFound
		if errors.As(err, &notFound) {
			return fmt.Errorf("bucket not found: %w", err)
		}
		return err
	}
	return nil
}

func (s *s3Client) Bucket() string { return s.cfg.Bucket }
