// Package recordings sobe as gravações para o S3 (ou compatível).
package recordings

import (
	"bytes"
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Uploader struct {
	client *s3.Client
	bucket string
}

// New monta o client. `endpoint` vazio = AWS; preenchido = provedor
// S3-compatível (MinIO/Spaces/R2), que exige path-style.
func New(accessKey, secret, region, endpoint, bucket string) *Uploader {
	opts := []func(*s3.Options){
		func(o *s3.Options) {
			o.Region = region
			o.Credentials = credentials.NewStaticCredentialsProvider(accessKey, secret, "")
		},
	}
	if endpoint != "" {
		opts = append(opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		})
	}
	return &Uploader{client: s3.New(s3.Options{}, opts...), bucket: bucket}
}

func (u *Uploader) BucketName() string { return u.bucket }

func (u *Uploader) Upload(ctx context.Context, key string, body []byte, contentType string) error {
	_, err := u.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(u.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(body),
		ContentType: aws.String(contentType),
	})
	return err
}
