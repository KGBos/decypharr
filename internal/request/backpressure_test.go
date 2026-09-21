package request

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
)

func TestRateLimitBackoff(t *testing.T) {
	resp := &http.Response{StatusCode: 429, Header: http.Header{"Retry-After": {"120"}}}
	if got := retryAfterBackoff(time.Second, 5*time.Minute, 0, resp); got != 120*time.Second {
		t.Fatalf("Retry-After 120: %s", got)
	}
	resp.Header.Set("Retry-After", time.Now().Add(120*time.Second).UTC().Format(http.TimeFormat))
	if got := retryAfterBackoff(time.Second, 5*time.Minute, 0, resp); got < 119*time.Second || got > 120*time.Second {
		t.Fatalf("HTTP-date: %s", got)
	}
	resp.Header.Set("Retry-After", "9223372036854775807")
	if got := retryAfterBackoff(time.Second, 5*time.Minute, 0, resp); got != 5*time.Minute {
		t.Fatalf("overflow-safe cap: %s", got)
	}
	for _, header := range []string{"", "invalid", "-1", "0", time.Now().Add(-time.Minute).UTC().Format(http.TimeFormat)} {
		resp.Header.Set("Retry-After", header)
		seen := map[time.Duration]bool{}
		for i := 0; i < 32; i++ {
			got := retryAfterBackoff(time.Second, 5*time.Minute, 3, resp)
			if got < 4*time.Second || got > 8*time.Second {
				t.Fatalf("exponential jitter: %s", got)
			}
			seen[got] = true
		}
		if len(seen) < 2 {
			t.Fatal("backoff has no jitter")
		}
	}
}

func TestThrottleOpensAndRecovers(t *testing.T) {
	now := time.Now()
	b := NewThrottle(3, time.Minute, 5*time.Minute, zerolog.Nop())
	b.now = func() time.Time { return now }
	resp := &http.Response{StatusCode: 429, Header: make(http.Header)}
	for i := 0; i < 3; i++ {
		if err := b.Before(); err != nil {
			t.Fatal(err)
		}
		b.Observe(resp, false)
		if i < 2 {
			now = now.Add(10 * time.Second)
		}
	}
	if !b.open {
		t.Fatal("breaker did not open at threshold")
	}
	if b.Before() == nil {
		t.Fatal("open breaker allowed a request")
	}
	// A response already in flight must not close the open breaker.
	b.Observe(&http.Response{StatusCode: 200}, false)
	if b.Before() == nil {
		t.Fatal("in-flight success closed breaker")
	}
	now = now.Add(time.Minute)
	if err := b.Before(); err != nil {
		t.Fatal(err)
	}
	if b.open {
		t.Fatal("breaker did not close after cooldown")
	}
	// Consecutive 429s survive cooldown so a re-trip can tell there was no
	// success in between.
	if b.consecutive != 3 {
		t.Fatalf("cooldown reset consecutive: %d", b.consecutive)
	}
	b.Observe(&http.Response{StatusCode: 200}, false)
	if b.consecutive != 0 || b.escalation != 0 {
		t.Fatalf("success must reset consecutive and escalation: %d/%d", b.consecutive, b.escalation)
	}
}

func TestRetryAfterLongBanBoundaries(t *testing.T) {
	resp := &http.Response{StatusCode: 429, Header: http.Header{"Retry-After": {"86400"}}}
	// Exactly 24h is honored at a 24h ceiling, not truncated to the old 5m/15m.
	if got := retryAfterBackoff(time.Second, 24*time.Hour, 0, resp); got != 24*time.Hour {
		t.Fatalf("86400s truncated at 24h ceiling: %s", got)
	}
	if got := retryAfterBackoff(time.Second, 25*time.Hour, 0, resp); got != 24*time.Hour {
		t.Fatalf("86400s truncated below server advice: %s", got)
	}
	if got := retryAfterBackoff(time.Second, time.Hour, 0, resp); got != time.Hour {
		t.Fatalf("ceiling not applied: %s", got)
	}
}

func TestThrottleHonorsLongRetryAfter(t *testing.T) {
	now := time.Now()
	b := NewThrottle(1, time.Minute, 24*time.Hour, zerolog.Nop())
	b.now = func() time.Time { return now }
	b.Observe(&http.Response{StatusCode: 429, Header: http.Header{"Retry-After": {"86400"}}}, false)
	if got := b.until.Sub(now); got != 24*time.Hour {
		t.Fatalf("long ban truncated: %s", got)
	}
	if b.Before() == nil {
		t.Fatal("24h ban did not hold the breaker")
	}
}

func TestThrottleEscalatesOnRetrip(t *testing.T) {
	now := time.Now()
	b := NewThrottle(1, time.Minute, 24*time.Hour, zerolog.Nop())
	b.now = func() time.Time { return now }
	resp := &http.Response{StatusCode: 429, Header: http.Header{"Retry-After": {"600"}}}
	b.Observe(resp, false)
	if got := b.until.Sub(now); got != 10*time.Minute {
		t.Fatalf("first open: %s", got)
	}
	now = now.Add(10 * time.Minute)
	if err := b.Before(); err != nil {
		t.Fatal(err)
	}
	b.Observe(resp, false)
	if got := b.until.Sub(now); got != 20*time.Minute {
		t.Fatalf("re-trip did not double: %s", got)
	}
	now = now.Add(20 * time.Minute)
	if err := b.Before(); err != nil {
		t.Fatal(err)
	}
	b.Observe(resp, false)
	if got := b.until.Sub(now); got != 40*time.Minute {
		t.Fatalf("second re-trip did not double: %s", got)
	}
}

func TestThrottleEscalationResetsOnSuccess(t *testing.T) {
	now := time.Now()
	b := NewThrottle(1, time.Minute, 24*time.Hour, zerolog.Nop())
	b.now = func() time.Time { return now }
	resp := &http.Response{StatusCode: 429, Header: http.Header{"Retry-After": {"600"}}}
	b.Observe(resp, false)
	now = now.Add(10 * time.Minute)
	if err := b.Before(); err != nil {
		t.Fatal(err)
	}
	b.Observe(resp, false)
	now = now.Add(20 * time.Minute)
	if err := b.Before(); err != nil {
		t.Fatal(err)
	}
	b.Observe(&http.Response{StatusCode: 200}, false)
	if b.escalation != 0 || b.consecutive != 0 {
		t.Fatalf("success did not reset: escalation=%d consecutive=%d", b.escalation, b.consecutive)
	}
	b.Observe(resp, false)
	if got := b.until.Sub(now); got != 10*time.Minute {
		t.Fatalf("post-success wait not reset to base: %s", got)
	}
}

func TestThrottleLogsLongBanDistinctly(t *testing.T) {
	now := time.Now()
	var long bytes.Buffer
	b := NewThrottle(1, time.Minute, 24*time.Hour, zerolog.New(&long))
	b.now = func() time.Time { return now }
	b.Observe(&http.Response{StatusCode: 429, Header: http.Header{"Retry-After": {"86400"}}}, false)
	if !strings.Contains(long.String(), "TorBox long ban: retry-after=86400s") {
		t.Fatalf("long ban not logged distinctly: %s", long.String())
	}
	var short bytes.Buffer
	s := NewThrottle(1, time.Minute, 24*time.Hour, zerolog.New(&short))
	s.now = func() time.Time { return now }
	s.Observe(&http.Response{StatusCode: 429, Header: http.Header{"Retry-After": {"600"}}}, false)
	if strings.Contains(short.String(), "long ban") {
		t.Fatalf("short ban logged as long: %s", short.String())
	}
}

func TestThrottleSharesAPIAndReadBackpressure(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(429)
	}))
	defer server.Close()
	b := NewThrottle(3, time.Minute, 5*time.Minute, zerolog.Nop())
	client := New(WithThrottle(b), WithRetryWait(time.Second, 5*time.Minute), WithMaxRetries(3))
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", server.URL, nil)
	_, err := client.Do(req)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("retry wait must be cancellable: %v", err)
	}
	if got := b.Remaining(); got < 115*time.Second || got > 120*time.Second {
		t.Fatalf("configured retry wait: %s", got)
	}
	req, _ = http.NewRequest("HEAD", server.URL, nil)
	_, err = b.Do(server.Client(), req)
	if BackpressureError(err) == nil {
		t.Fatalf("read not rejected: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("provider hammered: %d calls", calls.Load())
	}
	other := NewThrottle(3, time.Minute, 5*time.Minute, zerolog.Nop())
	if other.Before() != nil {
		t.Fatal("provider isolation lost")
	}
}

func TestRetryAfter120WallClock(t *testing.T) {
	if os.Getenv("TORBOX_LONG_RETRY_TEST") != "1" {
		t.Skip("set TORBOX_LONG_RETRY_TEST=1 for the two-minute wire test")
	}
	config.SetConfigPath(t.TempDir())
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "120")
			w.WriteHeader(429)
			return
		}
		w.WriteHeader(200)
	}))
	defer server.Close()
	b := NewThrottle(3, time.Minute, 5*time.Minute, zerolog.Nop())
	c := New(WithThrottle(b), WithRetryWait(time.Second, 5*time.Minute), WithMaxRetries(1))
	start := time.Now()
	resp, err := c.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	elapsed := time.Since(start)
	if resp.StatusCode != 200 || calls.Load() != 2 || elapsed < 120*time.Second || elapsed > 125*time.Second {
		t.Fatalf("status=%d calls=%d wait=%s", resp.StatusCode, calls.Load(), elapsed)
	}
	t.Logf("Retry-After: 120 measured wait: %s", elapsed)
}

type countingLimiter struct{ calls atomic.Int64 }

func (l *countingLimiter) Take() time.Time { l.calls.Add(1); return time.Now() }

func TestThrottleCountsAndLimitsEveryRetry(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(429) }))
	defer server.Close()
	b := NewThrottle(3, time.Minute, time.Millisecond, zerolog.Nop())
	limiter := &countingLimiter{}
	c := New(WithThrottle(b), WithRateLimiter(limiter), WithRetryWait(time.Millisecond, time.Millisecond), WithMaxRetries(5))
	resp, err := c.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 429 || calls.Load() != 3 || limiter.calls.Load() != 3 {
		t.Fatalf("status=%d requests=%d limiter=%d", resp.StatusCode, calls.Load(), limiter.calls.Load())
	}
	_, err = c.Get(server.URL)
	if BackpressureError(err) == nil || calls.Load() != 3 || limiter.calls.Load() != 3 {
		t.Fatalf("open circuit did not reject before limiter: %v", err)
	}
}

func TestReadGateCoversRedirects(t *testing.T) {
	b := NewThrottle(1, time.Minute, 5*time.Minute, zerolog.Nop())
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path == "/" {
			http.Redirect(w, r, "/cdn", 302)
			return
		}
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(429)
	}))
	defer server.Close()
	req, _ := http.NewRequest("GET", server.URL, nil)
	_, err := b.Do(server.Client(), req)
	if BackpressureError(err) == nil || calls.Load() != 2 {
		t.Fatalf("redirect throttle not observed: %v, calls=%d", err, calls.Load())
	}
	if b.read429 != 1 || b.reads != 2 {
		t.Fatalf("read counters: %d/%d", b.read429, b.reads)
	}
}

func TestOpenCircuitPolicyPreservesNetworkError(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	gate := NewThrottle(1, time.Minute, 5*time.Minute, zerolog.Nop())
	c := New(WithThrottle(gate))
	gate.Observe(&http.Response{StatusCode: 429, Header: make(http.Header)}, false)
	failure := errors.New("network failed while another request opened the circuit")
	retry, err := c.client.CheckRetry(context.Background(), nil, failure)
	if retry || !errors.Is(err, failure) {
		t.Fatalf("retry=%v err=%v; want original network error without retry", retry, err)
	}
}

func TestNetworkFailureWhileAnotherRequestOpensCircuit(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	entered := make(chan struct{})
	release := make(chan struct{})
	var networkCalls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/throttle" {
			w.Header().Set("Retry-After", "120")
			w.WriteHeader(429)
			return
		}
		if networkCalls.Add(1) == 1 {
			close(entered)
		}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		conn.Close()
	}))
	defer server.Close()
	gate := NewThrottle(1, time.Minute, 5*time.Minute, zerolog.Nop())
	c := New(WithThrottle(gate), WithMaxRetries(3))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	type result struct {
		resp *http.Response
		err  error
	}
	done := make(chan result, 1)
	go func() {
		req, _ := http.NewRequestWithContext(ctx, "GET", server.URL+"/network", nil)
		resp, err := c.Do(req)
		done <- result{resp, err}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("network request never started")
	}
	req, _ := http.NewRequestWithContext(ctx, "GET", server.URL+"/throttle", nil)
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 429 || !gate.isOpen() {
		t.Fatal("parallel request did not open breaker")
	}
	close(release)
	select {
	case got := <-done:
		if got.resp != nil || !errors.Is(got.err, io.EOF) || networkCalls.Load() != 1 {
			t.Fatalf("resp=%v err=%v calls=%d", got.resp, got.err, networkCalls.Load())
		}
	case <-ctx.Done():
		t.Fatal("network failure did not return")
	}
}

func TestDefaultRetryStatusesOnWire(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	for _, status := range []int{429, 500, 502, 503, 504} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			var calls atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) == 1 {
					w.WriteHeader(status)
					return
				}
				w.WriteHeader(200)
			}))
			defer server.Close()
			c := New(WithThrottle(NewThrottle(3, time.Minute, time.Millisecond, zerolog.Nop())), WithRetryWait(time.Millisecond, time.Millisecond), WithMaxRetries(1))
			resp, err := c.Get(server.URL)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != 200 || calls.Load() != 2 {
				t.Fatalf("status=%d calls=%d", resp.StatusCode, calls.Load())
			}
		})
	}
}
