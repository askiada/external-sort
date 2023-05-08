package rw

import (
	"context"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/askiada/external-sort/bucket"
	"github.com/askiada/external-sort/internal"
	"github.com/askiada/external-sort/internal/progress"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

var logger = logrus.StandardLogger()

type InputOutput struct {
	s3Client    bucket.S3ClientAPI
	Input       io.Reader
	inputPipe   *io.PipeReader
	Output      io.Writer
	outputPipe  *io.PipeWriter
	g           *errgroup.Group
	internalCtx context.Context //nolint //containedcontext
}

func NewInputOutput(ctx context.Context) *InputOutput {
	g, dCtx := errgroup.WithContext(ctx)
	return &InputOutput{
		g:           g,
		internalCtx: dCtx,
	}
}

func (i *InputOutput) s3Check(ctx context.Context) error {
	if i.s3Client != nil {
		return nil
	}
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(internal.S3Region),
		config.WithRetryMaxAttempts(internal.S3RetryMaxAttempts),
	)
	if err != nil {
		return errors.New("can't create aws config")
	}
	i.s3Client = s3.NewFromConfig(cfg)
	return nil
}

func (i *InputOutput) SetInputReader(ctx context.Context, inputFiles ...string) (err error) {
	if strings.HasPrefix(inputFiles[0], "s3") || strings.HasPrefix(inputFiles[0], "S3") {
		err = i.s3Check(ctx)
		if err != nil {
			return errors.Wrap(err, "can't check s3")
		}
		s3Api, err := bucket.New(ctx,
			bucket.Client(i.s3Client),
			bucket.Buffer(1_000_000),
			bucket.Progress(&progress.Pb{}),
		)
		if err != nil {
			return errors.Wrap(err, "can't create s3 client")
		}
		files := []*bucket.S3FileInfo{}
		for _, inputFile := range inputFiles {
			u, _ := url.Parse(inputFile)
			u.Path = strings.TrimLeft(u.Path, "/")
			logger.Debugf("Proto: %q, Bucket: %q, Key: %q", u.Scheme, u.Host, u.Path)
			files = append(files, &bucket.S3FileInfo{
				Bucket: u.Host,
				Key:    u.Path,
			})
		}

		pr, pw := io.Pipe()
		i.Input = pr
		i.inputPipe = pr
		i.g.Go(func() error {
			defer pw.Close() //nolint:errcheck //no need to check this error
			err := s3Api.Download(i.internalCtx, pw, files...)
			if err != nil {
				return errors.Wrap(err, "can't download files")
			}
			return nil
		})
	} else {
		var files []io.Reader
		for _, inputFile := range inputFiles {
			f, err := os.Open(inputFile)
			if err != nil {
				return errors.Wrapf(err, "can't open file %s", inputFile)
			}
			files = append(files, f)
		}
		i.Input = io.MultiReader(files...)
	}
	return nil
}

func (i *InputOutput) SetOutputWriter(ctx context.Context, outputFile string) (err error) {
	if strings.HasPrefix(outputFile, "s3") || strings.HasPrefix(outputFile, "S3") {
		err = i.s3Check(ctx)
		if err != nil {
			return errors.Wrap(err, "can't check s3")
		}
		u, _ := url.Parse(outputFile)
		u.Path = strings.TrimLeft(u.Path, "/")
		logger.Debugf("Proto: %q, Bucket: %q, Key: %q", u.Scheme, u.Host, u.Path)
		s3Api, err := bucket.New(ctx,
			bucket.Client(i.s3Client),
			bucket.Buffer(1_000_000),
			bucket.Progress(&progress.Pb{}),
		)
		if err != nil {
			return errors.Wrap(err, "can't create s3 client")
		}

		pr, pw := io.Pipe()
		i.Output = pw
		i.outputPipe = pw
		i.g.Go(func() error {
			defer pr.Close() //nolint:errcheck //no need to check this error
			err := s3Api.Upload(i.internalCtx, pr, u.Host, u.Path)
			if err != nil {
				return errors.Wrapf(err, "can't upload file %s", outputFile)
			}
			return nil
		})
	} else {
		i.Output, err = os.Create(filepath.Clean(outputFile))
		if err != nil {
			return errors.Wrapf(err, "can't create file %s", outputFile)
		}
	}
	return nil
}

func (i *InputOutput) Do(f func() error) {
	i.g.Go(func() error {
		err := f()
		if err != nil {
			return err
		}
		err = i.Close()
		if err != nil {
			return err
		}
		return nil
	})
}

func (i *InputOutput) Close() error {
	if i.inputPipe != nil {
		err := i.inputPipe.Close()
		if err != nil {
			return errors.Wrap(err, "can't close input reader")
		}
	}
	if i.outputPipe != nil {
		err := i.outputPipe.Close()
		if err != nil {
			return errors.Wrap(err, "can't close output writer")
		}
	}
	return nil
}

func (i *InputOutput) Err() error {
	if err := i.g.Wait(); err != nil {
		return errors.Wrap(err, "one of the go routines went wrong")
	}
	return nil
}
