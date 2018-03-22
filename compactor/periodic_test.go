// Copyright 2015 The etcd Authors
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
	"reflect"
	"testing"
	"time"

	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/pkg/testutil"

	"github.com/jonboulle/clockwork"
)

func TestPeriodicHourly(t *testing.T) {
	fc := clockwork.NewFakeClock()
	rg := &fakeRevGetter{testutil.NewRecorderStream(), 0}
	compactable := &fakeCompactable{testutil.NewRecorderStream()}
	tb := &Periodic{
		clock:  fc,
		period: 24 * time.Hour,
		rg:     rg,
		c:      compactable,
	}

	tb.Run()
	defer tb.Stop()

	for i := 0; i < 24*2; i++ {
		// first 24-hour only with rev gets
		if _, err := rg.Wait(1); err != nil {
			t.Fatal(err)
		}
		fc.Advance(time.Hour)

		// after 24-hour compact kicks in, every hour
		// with 24-hour retention window
		if i >= 23 {
			ca, err := compactable.Wait(1)
			if err != nil {
				t.Fatal(err)
			}
			expectedRevision := int64(i + 2 - 24)
			if !reflect.DeepEqual(ca[0].Params[0], &pb.CompactionRequest{Revision: expectedRevision}) {
				t.Errorf("compact request = %v, want %v", ca[0].Params[0], &pb.CompactionRequest{Revision: expectedRevision})
			}
		}
	}
}

func TestPeriodicEveryMinute(t *testing.T) {
	fc := clockwork.NewFakeClock()
	rg := &fakeRevGetter{testutil.NewRecorderStream(), 0}
	compactable := &fakeCompactable{testutil.NewRecorderStream()}
	tb := &Periodic{
		clock:  fc,
		period: time.Minute,
		rg:     rg,
		c:      compactable,
	}

	tb.Run()
	defer tb.Stop()

	// expect compact every minute
	for i := 0; i < 10; i++ {
		if _, err := rg.Wait(1); err != nil {
			t.Fatal(err)
		}
		fc.Advance(time.Minute)

		ca, err := compactable.Wait(1)
		if err != nil {
			t.Fatal(err)
		}
		expectedRevision := int64(i + 1)
		if !reflect.DeepEqual(ca[0].Params[0], &pb.CompactionRequest{Revision: expectedRevision}) {
			t.Errorf("compact request = %v, want %v", ca[0].Params[0], &pb.CompactionRequest{Revision: expectedRevision})
		}
	}
}

func TestPeriodicPauseHourly(t *testing.T) {
	fc := clockwork.NewFakeClock()
	rg := &fakeRevGetter{testutil.NewRecorderStream(), 0}
	compactable := &fakeCompactable{testutil.NewRecorderStream()}
	tb := &Periodic{
		clock:  fc,
		period: 10 * time.Hour,
		rg:     rg,
		c:      compactable,
	}

	tb.Run()
	defer tb.Stop()

	tb.Pause()

	// t.revs = [1] (len=1)

	// collect 20 hours of revisions with no compaction
	for i := 0; i < 20; i++ {
		if _, err := rg.Wait(1); err != nil {
			t.Fatal(err)
		}
		fc.Advance(time.Hour)
	}
	select {
	case a := <-compactable.Chan():
		t.Fatalf("unexpected action %v", a)
	case <-time.After(10 * time.Millisecond):
	}

	// If we don't have window slide
	// t.revs = [1, 2, 3, ..., 21] (len=21)
	// but in reality, the size is capped to 10 (retentions)
	// t.revs = [12, 13, 14, 15, 16, 17, 18, 19, 20, 21] (len=10)
	revInPaused := 12

	tb.Resume()

	for i := 0; i < 20; i++ {
		if _, err := rg.Wait(1); err != nil {
			t.Fatal(err)
		}
		fc.Advance(time.Hour)

		ca, err := compactable.Wait(1)
		if err != nil {
			t.Fatal(err)
		}
		expectedRevision := int64(i + revInPaused)
		if !reflect.DeepEqual(ca[0].Params[0], &pb.CompactionRequest{Revision: expectedRevision}) {
			t.Errorf("compact request = %v, want %v", ca[0].Params[0], &pb.CompactionRequest{Revision: expectedRevision})
		}
	}
}
