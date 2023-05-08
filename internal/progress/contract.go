// Package progress defines standard and simple progress bar to track file download progress.
package progress

import (
	"math"

	"github.com/cheggaaa/pb/v3"
	"github.com/sirupsen/logrus"
)

// Progress defines a simple progress bar contract.
type Progress interface {
	// Begin sets and starts the progress bar.
	Begin(total int64)
	// Add increments the progress bar with n elements
	Add(n int64)
	// End terminates the progress bar
	End()
}

// Pb implements Progress contract using cheggaaa pb v3.
type Pb struct {
	bar *pb.ProgressBar
}

// Begin start a new progress bar in byte mode.
func (p *Pb) Begin(total int64) {
	p.bar = pb.Full.Start64(total)
	p.bar.Set(pb.Bytes, true)
}

// Add increment the bar by n elements.
func (p *Pb) Add(n int64) {
	p.bar.Add64(n)
}

// End terminates the bar.
func (p *Pb) End() {
	p.bar.Finish()
}

var _ Progress = &Pb{}

// Basic implements Progress contract using stdout to print status.
type Basic struct {
	total     float64
	written   float64
	milestone int
}

// Begin start a new progress bar.
func (b *Basic) Begin(total int64) {
	b.total = float64(total)
}

// Add increment the bar by n elements.
func (b *Basic) Add(val int64) {
	b.written += float64(val)
	progress := int(math.Round(b.written / b.total * 100)) //nolint //gomnd
	if progress >= b.milestone {
		b.milestone += 5 // every 5%
		logrus.Debugf("Download from S3 at %3d%%\n\n", progress)
	}
}

// End noop.
func (b *Basic) End() {}
