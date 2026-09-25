package manager

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/manager/link"
)

// dribbleReader serves chunk-sized reads, each blocked for delay. remaining
// < 0 means infinite; remaining == 0 returns EOF.
type dribbleReader struct {
	chunk     []byte
	delay     time.Duration
	remaining int
	closed    bool
}

func (d *dribbleReader) Read(p []byte) (int, error) {
	if d.remaining == 0 {
		return 0, io.EOF
	}
	if d.delay > 0 {
		time.Sleep(d.delay)
	}
	if d.remaining > 0 {
		d.remaining--
	}
	return copy(p, d.chunk), nil
}

func (d *dribbleReader) Close() error { d.closed = true; return nil }

func TestSlowWatchBodyTripsOnSustainedSlowStream(t *testing.T) {
	// 100 bytes per 100ms => ~1 kB/s, far below the 100 kB/s test threshold.
	// Two 150ms windows of blocked read time trip the watchdog.
	var firstWindowBps float64
	var firstWindowCalls int
	w := newSlowWatchBodyWithParams(
		&dribbleReader{chunk: make([]byte, 100), delay: 100 * time.Millisecond, remaining: -1},
		"cdn1.torbox.app",
		func(bps float64) { firstWindowCalls++; firstWindowBps = bps },
		100*1024, 150*time.Millisecond, 2,
	)
	buf := make([]byte, 4096)
	for {
		n, err := w.Read(buf)
		if err == nil {
			if n == 0 {
				t.Fatal("nil error with zero bytes")
			}
			continue
		}
		var serr *link.SlowStreamError
		if !errors.As(err, &serr) {
			t.Fatalf("expected *link.SlowStreamError, got %T (%v)", err, err)
		}
		if n != 0 {
			t.Fatalf("trip must drop the in-flight bytes, got n=%d", n)
		}
		if serr.Host != "cdn1.torbox.app" {
			t.Errorf("host not carried: %q", serr.Host)
		}
		if serr.Bps >= 100*1024 {
			t.Errorf("measured bps implausible: %f", serr.Bps)
		}
		if serr.Bytes <= 0 {
			t.Error("expected some bytes counted before the trip")
		}
		if lerr := link.GetLinkError(err); lerr == nil || !lerr.ShouldRefetch() {
			t.Error("tripped error must classify as refetchable")
		}
		break
	}
	if firstWindowCalls != 1 {
		t.Errorf("onFirstWindow called %d times, want 1", firstWindowCalls)
	}
	if firstWindowBps <= 0 || firstWindowBps >= 100*1024 {
		t.Errorf("first-window bps implausible: %f", firstWindowBps)
	}
}

func TestSlowWatchBodyHealthyStreamNoTrip(t *testing.T) {
	// ~10 MB/s: 100 kB per 10ms, 20 chunks then EOF.
	w := newSlowWatchBodyWithParams(
		&dribbleReader{chunk: make([]byte, 100*1024), delay: 10 * time.Millisecond, remaining: 20},
		"cdn1.torbox.app", nil,
		100*1024, 50*time.Millisecond, 2,
	)
	buf := make([]byte, 256*1024)
	var total int
	for {
		n, err := w.Read(buf)
		total += n
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("healthy stream must not trip: %v", err)
		}
	}
	if total != 20*100*1024 {
		t.Errorf("short read: got %d bytes", total)
	}
}

func TestSlowWatchBodyConsumerPausesDontCount(t *testing.T) {
	// Reads return instantly (no blocked time); long pauses between them must
	// not accumulate toward a window, so no trip is possible.
	w := newSlowWatchBodyWithParams(
		&dribbleReader{chunk: make([]byte, 100), remaining: 10},
		"cdn1.torbox.app", nil,
		100*1024, 100*time.Millisecond, 2,
	)
	buf := make([]byte, 4096)
	var total int
	for i := 0; i < 10; i++ {
		n, err := w.Read(buf)
		total += n
		if err != nil {
			t.Fatalf("consumer-paced reads must not trip the watchdog: %v", err)
		}
		time.Sleep(150 * time.Millisecond) // pauses are not blocked Read time
	}
	if total != 1000 {
		t.Errorf("got %d bytes, want 1000", total)
	}
}

func TestSlowWatchBodyRecoveryAfterBadWindow(t *testing.T) {
	// One bad window followed by a fast window resets the consecutive count:
	// a single slow patch must not trip the watchdog (it needs badWindows
	// *consecutive* slow windows).
	dr := &dribbleReader{chunk: make([]byte, 100), delay: 60 * time.Millisecond, remaining: -1}
	w := newSlowWatchBodyWithParams(dr, "cdn1.torbox.app", nil,
		100*1024, 50*time.Millisecond, 2)
	buf := make([]byte, 4096)

	// Phase 1: one slow read completes exactly one bad window (~1.6 kB/s).
	if _, err := w.Read(buf); err != nil {
		t.Fatalf("single bad window must not trip: %v", err)
	}
	// Phase 2: fast reads (~10 MB/s) complete a healthy window and reset the
	// consecutive-bad counter.
	dr.chunk = make([]byte, 100*1024)
	dr.delay = 10 * time.Millisecond
	for i := 0; i < 6; i++ {
		if _, err := w.Read(buf); err != nil {
			t.Fatalf("fast reads must not trip: %v", err)
		}
	}
	// Phase 3: slow again — still only one consecutive bad window, no trip.
	dr.chunk = make([]byte, 100)
	dr.delay = 60 * time.Millisecond
	if _, err := w.Read(buf); err != nil {
		t.Fatalf("isolated bad window after recovery must not trip: %v", err)
	}
}

// TestHttpTransportSlowStreamRecover wires the full trip path: the watchdog
// error reaches recover, cools the CDN host, refreshes the link, and arms
// the before/after report for the replacement body.
func TestHttpTransportSlowStreamRecover(t *testing.T) {
	var notedHost string
	var notedBps float64
	var refreshes int
	tr := &httpTransport{
		client: http.DefaultClient,
		noteSlow: func(host string, bps float64) {
			notedHost, notedBps = host, bps
		},
		getLink: func(context.Context) (types.DownloadLink, error) {
			return types.DownloadLink{Filename: "f", DownloadLink: "https://cdn1.torbox.app/x"}, nil
		},
		refresh: func(_ context.Context, bad types.DownloadLink) (types.DownloadLink, error) {
			refreshes++
			return types.DownloadLink{Filename: "f", DownloadLink: "https://cdn9.torbox.app/x"}, nil
		},
	}
	tr.last = types.DownloadLink{Filename: "f", DownloadLink: "https://cdn1.torbox.app/x"}

	serr := link.NewSlowStreamError("cdn1.torbox.app", 1<<20, 22*1024)
	if err := tr.recover(context.Background(), serr, 0); err != nil {
		t.Fatalf("recover should succeed, got %v", err)
	}
	if notedHost != "cdn1.torbox.app" || notedBps != 22*1024 {
		t.Errorf("noteSlow got host=%q bps=%f", notedHost, notedBps)
	}
	if refreshes != 1 {
		t.Errorf("expected exactly one refresh, got %d", refreshes)
	}
	tr.mu.Lock()
	report := tr.swapArmed
	tr.mu.Unlock()
	if report == nil || report.oldHost != "cdn1.torbox.app" || report.beforeBps != 22*1024 {
		t.Fatalf("swap report not armed: %+v", report)
	}
}

// TestHttpTransportSlowStreamRecoverNilHooks ensures transports built without
// the slow-stream hooks (e.g. existing unit tests) still recover fine.
func TestHttpTransportSlowStreamRecoverNilHooks(t *testing.T) {
	tr := &httpTransport{
		client: http.DefaultClient,
		getLink: func(context.Context) (types.DownloadLink, error) {
			return types.DownloadLink{Filename: "f", DownloadLink: "https://cdn1.torbox.app/x"}, nil
		},
		refresh: func(_ context.Context, bad types.DownloadLink) (types.DownloadLink, error) {
			return types.DownloadLink{Filename: "f", DownloadLink: "https://cdn9.torbox.app/x"}, nil
		},
	}
	tr.last = types.DownloadLink{Filename: "f", DownloadLink: "https://cdn1.torbox.app/x"}
	if err := tr.recover(context.Background(), link.NewSlowStreamError("cdn1.torbox.app", 1, 1), 0); err != nil {
		t.Fatalf("recover with nil hooks should succeed, got %v", err)
	}
}

// TestWatchBodyHostFromFinalURL opens through a real test server and asserts
// the watchdog measures the final (post-redirect) host — the CDN node serving
// the bytes — and returns a *slowWatchBody.
func TestWatchBodyHostFromFinalURL(t *testing.T) {
	var logged strings.Builder
	logger := zerolog.New(&logged)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "4")
		_, _ = w.Write([]byte("data"))
	}))
	defer server.Close()

	tr := &httpTransport{
		client: server.Client(),
		logger: &logger,
		getLink: func(context.Context) (types.DownloadLink, error) {
			return types.DownloadLink{Filename: "f", DownloadLink: server.URL + "/f"}, nil
		},
		refresh: func(_ context.Context, bad types.DownloadLink) (types.DownloadLink, error) {
			return types.DownloadLink{Filename: "f", DownloadLink: server.URL + "/f"}, nil
		},
	}
	tr.swapArmed = &slowSwapReport{oldHost: "cdn1.torbox.app", beforeBps: 22 * 1024}

	body, err := tr.open(context.Background(), 0)
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	defer body.Close()
	w, ok := body.(*slowWatchBody)
	if !ok {
		t.Fatalf("open should return a *slowWatchBody, got %T", body)
	}
	u, _ := url.Parse(server.URL)
	if w.host != u.Host {
		t.Errorf("watchdog host = %q, want test server host %q", w.host, u.Host)
	}
	if _, err := io.ReadAll(w); err != nil {
		t.Fatalf("read failed: %v", err)
	}
}
