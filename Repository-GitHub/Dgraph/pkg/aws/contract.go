package aws

import (
	"context"
	"io"
)

type S3Config struct {
	Region          string
	Bucket          string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	Endpoint        string
	ForcePathStyle  bool
}

type S3Adapter interface {
	Upload(ctx context.Context, key string, contentType string, body io.Reader) error
	GetBytes(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error
	HealthCheck(ctx context.Context) error
	Bucket() string
}
