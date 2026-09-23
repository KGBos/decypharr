package request

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
)

func TestPersistentGateFailsClosedAfterRestartDuringBan(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	path := filepath.Join(t.TempDir(), "gate", "state.json")
	first := NewThrottle(3, time.Minute, 5*time.Minute, zerolog.Nop())
	if err := first.WithPersistentState(path); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "2634")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()
	resp, err := New(WithThrottle(first), WithMaxRetries(5)).Get(server.URL)
	if err != nil || resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("initial 429: status=%v err=%v", resp, err)
	}
	resp.Body.Close()
	restarted := NewThrottle(3, time.Minute, 5*time.Minute, zerolog.Nop())
	if err := restarted.WithPersistentState(path); err != nil {
		t.Fatal(err)
	}
	_, err = New(WithThrottle(restarted)).Get(server.URL)
	if BackpressureError(err) == nil || BackpressureError(err).RetryAfter >= 0 || calls.Load() != 1 {
		t.Fatalf("restart sent provider request during ban: err=%v calls=%d", err, calls.Load())
	}
	restarted.now = func() time.Time { return time.Now().Add(2635 * time.Second) }
	if err := restarted.Before(); BackpressureError(err) == nil {
		t.Fatalf("restart inferred a safe deadline from the wall clock: %v", err)
	}
}

func TestPersistentGateFailsClosedAfterUncertainRequest(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	path := filepath.Join(t.TempDir(), "gate", "state.json")
	first := NewThrottle(3, time.Minute, 5*time.Minute, zerolog.Nop())
	if err := first.WithPersistentState(path); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		conn.Close()
	}))
	defer server.Close()
	_, err := New(WithThrottle(first), WithMaxRetries(0)).Get(server.URL)
	if BackpressureError(err) == nil {
		t.Fatalf("unknown outcome did not fail closed: %v", err)
	}
	restarted := NewThrottle(3, time.Minute, 5*time.Minute, zerolog.Nop())
	if err := restarted.WithPersistentState(path); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Before(); BackpressureError(err) == nil || BackpressureError(err).RetryAfter >= 0 {
		t.Fatalf("restart lost unresolved marker: %v", err)
	}
}

func TestPersistentGateFailsClosedWithoutProviderDeadline(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	path := filepath.Join(t.TempDir(), "gate", "state.json")
	first := NewThrottle(3, time.Minute, 5*time.Minute, zerolog.Nop())
	if err := first.WithPersistentState(path); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()
	resp, err := New(WithThrottle(first), WithMaxRetries(5)).Get(server.URL)
	if resp != nil {
		resp.Body.Close()
	}
	if BackpressureError(err) == nil || calls.Load() != 1 {
		t.Fatalf("unknown deadline retried: err=%v calls=%d", err, calls.Load())
	}
	restarted := NewThrottle(3, time.Minute, 5*time.Minute, zerolog.Nop())
	if err := restarted.WithPersistentState(path); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Before(); BackpressureError(err) == nil || BackpressureError(err).RetryAfter >= 0 {
		t.Fatalf("restart lost unknown deadline: %v", err)
	}
}

func TestPersistentGateRefusesWireWhenJournalUnavailable(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	path := filepath.Join(t.TempDir(), "gate", "state.json")
	b := NewThrottle(3, time.Minute, 5*time.Minute, zerolog.Nop())
	if err := b.WithPersistentState(path); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Dir(path)); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	_, err := New(WithThrottle(b)).Get(server.URL)
	if BackpressureError(err) == nil || calls.Load() != 0 {
		t.Fatalf("wire request escaped failed journal: err=%v calls=%d", err, calls.Load())
	}
}

func TestConcurrentAttemptsStopAtFirst429(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	b := NewThrottle(3, time.Minute, 5*time.Minute, zerolog.Nop())
	if err := b.WithPersistentState(filepath.Join(t.TempDir(), "gate", "state.json")); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "2634")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()
	client := New(WithThrottle(b), WithMaxRetries(5))
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			resp, err := client.Get(server.URL)
			if resp != nil {
				resp.Body.Close()
			}
			if err != nil && BackpressureError(err) == nil {
				t.Errorf("unexpected request error: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if got := calls.Load(); got != 1 {
		t.Fatalf("provider calls after first 429: %d", got)
	}
}
