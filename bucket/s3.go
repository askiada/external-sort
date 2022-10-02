// Package bucket implements the io.ReadWriter for communication with the S3
// API.
package bucket

import (
	"context"
	"io"

	"github.com/askiada/external-sort/internal/progress"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pkg/errors"
)

// S3 can read and write from/to S3 buckets using io.Reader and io.Writer
// inputs.
type S3 struct {
	s3Client           S3ClientAPI
	progress           progress.Progress
	region             string
	maxRetries         int
	bufferLen          int
	partBodyMaxRetries int
}

// New returns an instance of the S3 struct.
func New(ctx context.Context, cfg ...ConfigFunc) (*S3, error) {
	s := &S3{
		region:             "eu-west-1",
		bufferLen:          1024,
		maxRetries:         10,
		partBodyMaxRetries: 3,
	}
	for _, c := range cfg {
		c(s)
	}

	if s.region == "" {
		return nil, errors.Wrap(ErrInvalidInput, "region")
	}
	if s.bufferLen <= 0 {
		return nil, errors.Wrap(ErrInvalidInput, "buffer length")
	}
	if s.s3Client == nil {
		cfg, err := config.LoadDefaultConfig(ctx,
			config.WithRegion(s.region),
			config.WithRetryMaxAttempts(s.maxRetries),
		)
		if err != nil {
			return nil, errors.New("can't create aws config")
		}
		s.s3Client = s3.NewFromConfig(cfg)
	}
	return s, nil
}

// Upload reads from the reader and uploads it to the S3 bucket with the
// filename key.
func (s *S3) Upload(ctx context.Context, r io.Reader, bucket string, key string) error {
	uploader := manager.NewUploader(s.s3Client, func(u *manager.Uploader) {
		u.BufferProvider = manager.NewBufferedReadSeekerWriteToPool(s.bufferLen)
	})
	_, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   r,
	})
	return errors.Wrap(err, "upload failed")
}

type seqWriterAt struct {
	w            io.Writer
	progressFunc func(n int)
}

func (s *seqWriterAt) WriteAt(p []byte, offset int64) (n int, err error) {
	n, err = s.w.Write(p)
	if s.progressFunc != nil {
		s.progressFunc(n)
	}
	return n, errors.Wrap(err, "can't write bytes at offset")
}

type DownloadFileInfo struct {
	Bucket string
	Key    string
}

// Download downloads the file from the S3 bucket with the filename key and
// writes it to the writer.
func (s *S3) Download(ctx context.Context, w io.Writer, filesinfo ...*DownloadFileInfo) error {
	downloader := manager.NewDownloader(s.s3Client, func(d *manager.Downloader) {
		d.PartBodyMaxRetries = s.partBodyMaxRetries
		d.PartSize = int64(s.bufferLen)
		// we need to force this to be a sequential download.
		d.Concurrency = 1
	})
	ww := &seqWriterAt{w, nil}
	for _, fileinfo := range filesinfo {
		_, err := downloader.Download(ctx, ww, &s3.GetObjectInput{
			Bucket: aws.String(fileinfo.Bucket),
			Key:    aws.String(fileinfo.Key),
		})
		if err != nil {
			return errors.Wrapf(err, "download failed for bucket %s and key %s", fileinfo.Bucket, fileinfo.Key)
		}
	}
	return nil
}
