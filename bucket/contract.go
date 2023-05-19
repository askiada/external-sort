package bucket

import (
	"github.com/askiada/external-sort/internal/progress"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3ClientAPI S3 client contract for this repo.
type S3ClientAPI interface {
	manager.UploadAPIClient
	manager.DownloadAPIClient
	s3.HeadObjectAPIClient
}

// ConfigFunc is a function that can be passed to the New function to configure
// the S3 object.
type ConfigFunc func(s *S3)

// Region sets the region of the S3 bucket.
func Region(region string) ConfigFunc {
	return func(s *S3) {
		s.region = region
	}
}

// PartBodyMaxRetries sets the number of retries when performing upload multi part.
func PartBodyMaxRetries(r int) ConfigFunc {
	return func(s *S3) {
		s.partBodyMaxRetries = r
	}
}

const mbConversion = 1024 * 1024

// Buffer is the amount of memory in MB to use for buffering the data.
func Buffer(buffer int) ConfigFunc {
	return func(s *S3) {
		s.bufferLen = buffer * mbConversion
	}
}

// Client sets the S3 client to use. If you provide this option, we will not be
// able to set the region.
func Client(client S3ClientAPI) ConfigFunc {
	return func(s *S3) {
		s.s3Client = client
	}
}

// MaxRetries sets the maximum number of retried per request before returning an error.
func MaxRetries(maxRetries int) ConfigFunc {
	return func(s *S3) {
		s.maxRetries = maxRetries
	}
}

// Progress sets a progress bar to be used when performing bucket actions.
func Progress(p progress.Progress) ConfigFunc {
	return func(s *S3) {
		s.progress = p
	}
}
