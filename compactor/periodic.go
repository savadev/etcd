// Copyright 2017 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compactor

import (
	"context"
	"sync"
	"time"

	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/mvcc"

	"github.com/jonboulle/clockwork"
)

// Periodic compacts the log by purging revisions older than
// the configured retention time.
type Periodic struct {
	clock  clockwork.Clock
	period time.Duration
	revs   []int64

	rg RevGetter
	c  Compactable

	ctx    context.Context
	cancel context.CancelFunc

	// mu protects paused
	mu     sync.RWMutex
	paused bool
}

// NewPeriodic creates a new instance of Periodic compactor that purges
// the log older than h Duration.
func NewPeriodic(h time.Duration, rg RevGetter, c Compactable) *Periodic {
	return &Periodic{
		clock:  clockwork.NewRealClock(),
		period: h,
		revs:   make([]int64, 0),
		rg:     rg,
		c:      c,
	}
}

func (t *Periodic) Run() {
	t.ctx, t.cancel = context.WithCancel(context.Background())
	interval := t.getInterval()

	last := t.clock.Now()
	go func() {
		for {
			t.revs = append(t.revs, t.rg.Rev())
			select {
			case <-t.ctx.Done():
				return
			case <-t.clock.After(interval):
				t.mu.Lock()
				p := t.paused
				t.mu.Unlock()
				if p {
					continue
				}
			}

			// wait up to initial given period
			if t.clock.Now().Sub(last) < t.period {
				continue
			}
			rev := t.revs[0]

			plog.Noticef("Starting auto-compaction at revision %d (retention: %v)", rev, t.period)
			_, err := t.c.Compact(t.ctx, &pb.CompactionRequest{Revision: rev})
			if err == nil || err == mvcc.ErrCompacted {
				plog.Noticef("Finished auto-compaction at revision %d", rev)
				// move to next sliding window
				t.revs = t.revs[1:]
			} else {
				plog.Noticef("Failed auto-compaction at revision %d (%v)", rev, err)
				plog.Noticef("Retry after %v", interval)
			}
		}
	}()
}

// if given compaction period x is <1-hour, compact every x duration.
// (e.g. --auto-compaction-mode 'periodic' --auto-compaction-retention='10m', then compact every 10-minute)
// if given compaction period x is >1-hour, compact every hour.
// (e.g. --auto-compaction-mode 'periodic' --auto-compaction-retention='2h', then compact every 1-hour)
func (t *Periodic) getInterval() time.Duration {
	itv := t.period
	if itv > time.Hour {
		itv = time.Hour
	}
	return itv
}

func (t *Periodic) Stop() {
	t.cancel()
}

func (t *Periodic) Pause() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.paused = true
}

func (t *Periodic) Resume() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.paused = false
}
