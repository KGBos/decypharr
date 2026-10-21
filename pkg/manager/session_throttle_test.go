package manager

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/request"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
)

func TestTorboxReadFailsFastAndRecovers(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(429)
			return
		}
		w.WriteHeader(206)
		_, _ = w.Write([]byte("data"))
	}))
	defer server.Close()
	gate := request.NewThrottle(1, 20*time.Millisecond, 20*time.Millisecond, zerolog.Nop())
	tr := &httpTransport{client: server.Client(), throttle: gate,
		getLink: func(context.Context) (types.DownloadLink, error) {
			return types.DownloadLink{DownloadLink: server.URL}, nil
		},
		refresh: func(context.Context, types.DownloadLink) (types.DownloadLink, error) {
			t.Error("throttle must not refresh a link")
			return types.DownloadLink{}, nil
		},
	}
	s := newSession(context.Background(), tr, 4, 0)
	defer s.Close()
	start := time.Now()
	_, err := s.Read(make([]byte, 4))
	if request.BackpressureError(err) == nil || customerror.IsRetriableError(err) {
		t.Fatalf("expected terminal error for current chunk, got %v", err)
	}
	if time.Since(start) > time.Second || calls.Load() != 1 || s.resumes.Load() != 0 {
		t.Fatal("read entered retry ladder")
	}
	time.Sleep(1100 * time.Millisecond)
	data, err := io.ReadAll(s)
	if err != nil || string(data) != "data" {
		t.Fatalf("automatic recovery: %q, %v", data, err)
	}
}

// TestReadHonorsFullRetryAfterWithNoInterveningCalls is the regression test
// for KGBos/liteflix#257 at the read path: a read-side 429 with a Retry-After
// longer than the read-wait threshold fails the current chunk fast, and every
// read attempted before the exact server deadline is refused by the shared
// gate with zero additional provider wire calls. After the deadline the same
// session reads the healthy file again without any repair or restart.
func TestReadHonorsFullRetryAfterWithNoInterveningCalls(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "2")
			w.WriteHeader(429)
			return
		}
		w.WriteHeader(206)
		_, _ = w.Write([]byte("data"))
	}))
	defer server.Close()
	// A tiny read-wait forces the fail-fast path for this 2s ban, like the
	// production 90s threshold does for longer bans.
	gate := request.NewThrottle(1, 20*time.Millisecond, 20*time.Millisecond, zerolog.Nop()).
		WithReadWait(50 * time.Millisecond)
	tr := &httpTransport{client: server.Client(), throttle: gate,
		getLink: func(context.Context) (types.DownloadLink, error) {
			return types.DownloadLink{DownloadLink: server.URL}, nil
		},
		refresh: func(context.Context, types.DownloadLink) (types.DownloadLink, error) {
			t.Error("throttle must not refresh a link")
			return types.DownloadLink{}, nil
		},
	}
	s := newSession(context.Background(), tr, 4, 0)
	defer s.Close()

	start := time.Now()
	_, err := s.Read(make([]byte, 4))
	if request.BackpressureError(err) == nil || customerror.IsRetriableError(err) {
		t.Fatalf("expected terminal backpressure for current chunk, got %v", err)
	}
	if time.Since(start) > time.Second || calls.Load() != 1 {
		t.Fatalf("first read did not fail fast on the gate: %v calls=%d", err, calls.Load())
	}

	// Halfway through the ban: the gate must refuse without a wire call.
	time.Sleep(500 * time.Millisecond)
	if _, err := s.Read(make([]byte, 4)); request.BackpressureError(err) == nil {
		t.Fatalf("expected backpressure during the ban, got %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("provider was called during the Retry-After window: calls=%d", calls.Load())
	}

	// After the exact deadline the same session reads the file again.
	time.Sleep(1800 * time.Millisecond)
	data, err := io.ReadAll(s)
	if err != nil || string(data) != "data" {
		t.Fatalf("post-deadline recovery: %q, %v", data, err)
	}
	if calls.Load() != 2 {
		t.Fatalf("expected exactly one post-deadline provider call, calls=%d", calls.Load())
	}
}

// TestShortBanIsWaitedOutOnTheReadPath covers the complementary case: the
// read that discovers a short ban fails fast (a 429 ends the current
// operation), and the follow-up read waits out the remainder of the exact
// Retry-After deadline — bounded and interruptible — with zero wire calls
// before it, then completes normally.
func TestShortBanIsWaitedOutOnTheReadPath(t *testing.T) {
	var calls atomic.Int64
	var mu sync.Mutex
	var callTimes []time.Time
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		callTimes = append(callTimes, time.Now())
		mu.Unlock()
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(429)
			return
		}
		w.WriteHeader(206)
		_, _ = w.Write([]byte("data"))
	}))
	defer server.Close()
	gate := request.NewThrottle(1, 20*time.Millisecond, 20*time.Millisecond, zerolog.Nop())
	tr := &httpTransport{client: server.Client(), throttle: gate,
		getLink: func(context.Context) (types.DownloadLink, error) {
			return types.DownloadLink{DownloadLink: server.URL}, nil
		},
		refresh: func(context.Context, types.DownloadLink) (types.DownloadLink, error) {
			t.Error("throttle must not refresh a link")
			return types.DownloadLink{}, nil
		},
	}
	s := newSession(context.Background(), tr, 4, 0)
	defer s.Close()

	// The discovering read ends fast with terminal backpressure.
	if _, err := s.Read(make([]byte, 4)); request.BackpressureError(err) == nil {
		t.Fatalf("expected terminal backpressure on the discovering read, got %v", err)
	}
	// The follow-up read waits out the exact deadline, then succeeds.
	start := time.Now()
	data, err := io.ReadAll(s)
	if err != nil || string(data) != "data" {
		t.Fatalf("waited-out read: %q, %v", data, err)
	}
	if elapsed := time.Since(start); elapsed < 900*time.Millisecond {
		t.Fatalf("follow-up read did not honor the deadline: elapsed=%v", elapsed)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(callTimes) != 2 {
		t.Fatalf("expected exactly 2 provider calls, got %d", len(callTimes))
	}
	if gap := callTimes[1].Sub(callTimes[0]); gap < 900*time.Millisecond {
		t.Fatalf("second provider call landed before the Retry-After deadline: gap=%v", gap)
	}
}
