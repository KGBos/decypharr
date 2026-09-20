package request

import (
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"go.uber.org/ratelimit"
)

// ThrottleError ends the current read/chunk attempt without poisoning the file.
// A later caller can retry automatically after RetryAfter; no repair is needed.
type ThrottleError struct{ RetryAfter time.Duration }

func (e *ThrottleError) Error() string {
	return fmt.Sprintf("TorBox HTTP 429 backpressure: retry after %s", e.RetryAfter.Round(time.Millisecond))
}
func (e *ThrottleError) IsRetryable() bool { return false }
func BackpressureError(err error) *ThrottleError {
	var e *ThrottleError
	if errors.As(err, &e) {
		return e
	}
	return nil
}

// Throttle is shared by a configured provider's API and streaming clients.
// Only actual responses count; rejected requests do not extend the cooldown.
type Throttle struct {
	mu                                  sync.Mutex
	threshold, consecutive              int
	cooldown, backoffMax                time.Duration
	until                               time.Time
	open                                bool
	requests, throttled, reads, read429 uint64
	now                                 func() time.Time
	logger                              zerolog.Logger
}

func NewThrottle(threshold int, cooldown, backoffMax time.Duration, log zerolog.Logger) *Throttle {
	return &Throttle{threshold: threshold, cooldown: cooldown, backoffMax: backoffMax, now: time.Now, logger: log}
}
func (b *Throttle) Before() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if wait := b.until.Sub(b.now()); wait > 0 {
		return &ThrottleError{RetryAfter: wait}
	}
	if b.open {
		b.open = false
		b.consecutive = 0
		b.logger.Info().Uint64("requests", b.requests).Uint64("429s", b.throttled).Uint64("read_requests", b.reads).Uint64("read_429s", b.read429).Msg("TorBox circuit closed after cooldown")
	}
	return nil
}
func (b *Throttle) Remaining() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	return max(0, b.until.Sub(b.now()))
}
func (b *Throttle) Observe(resp *http.Response, read bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if resp == nil {
		return
	}
	if resp.StatusCode != http.StatusTooManyRequests {
		// Do not let late successes erase cooldown or the consecutive count.
		if !b.open && !b.now().Before(b.until) && (resp.StatusCode >= 200 && resp.StatusCode < 300 || resp.StatusCode >= 400) {
			b.consecutive = 0
		}
		return
	}
	b.throttled++
	if read {
		b.read429++
	}
	b.consecutive++
	wait := retryAfterBackoff(time.Second, b.backoffMax, b.consecutive-1, resp)
	opening := !b.open && b.consecutive >= b.threshold
	if b.consecutive >= b.threshold {
		b.open = true
		wait = max(wait, b.cooldown)
	}
	until := b.now().Add(wait)
	if until.After(b.until) {
		b.until = until
	}
	if opening {
		b.logger.Warn().Dur("cooldown", wait).Int("consecutive_429s", b.consecutive).Uint64("requests", b.requests).Uint64("429s", b.throttled).Uint64("read_requests", b.reads).Uint64("read_429s", b.read429).Msg("TorBox circuit opened")
	} else if read {
		b.logger.Debug().Dur("retry_after", wait).Uint64("read_requests", b.reads).Uint64("read_429s", b.read429).Msg("TorBox read failed fast on HTTP 429")
	}
}
func (b *Throttle) isOpen() bool { b.mu.Lock(); defer b.mu.Unlock(); return b.open }

// throttleTransport gates every wire attempt, including retries and redirects.
// Only opted-in providers use it; other providers retain their existing policy.
type throttleTransport struct {
	next     http.RoundTripper
	throttle *Throttle
	limiter  ratelimit.Limiter
	read     bool
}

func (t *throttleTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if err := req.Context().Err(); err != nil {
		return nil, err
	}
	if err := t.throttle.Before(); err != nil {
		return nil, err
	}
	if t.limiter != nil {
		t.limiter.Take()
	}
	if err := req.Context().Err(); err != nil {
		return nil, err
	}
	if err := t.throttle.Before(); err != nil {
		return nil, err
	}
	t.throttle.mu.Lock()
	t.throttle.requests++
	if t.read {
		t.throttle.reads++
	}
	t.throttle.mu.Unlock()
	resp, err := t.next.RoundTrip(req)
	t.throttle.Observe(resp, t.read)
	if t.read && resp != nil && resp.StatusCode == http.StatusTooManyRequests {
		// Never drain a rate-limit body: even that can stall the read.
		resp.Body.Close()
		return nil, &ThrottleError{RetryAfter: t.throttle.Remaining()}
	}
	return resp, err
}

// Do applies the shared gate to a streaming GET/HEAD, including redirects,
// without mutating the manager's shared HTTP client or adding Authorization.
func (b *Throttle) Do(client *http.Client, req *http.Request) (*http.Response, error) {
	copyClient := *client
	next := client.Transport
	if next == nil {
		next = http.DefaultTransport
	}
	copyClient.Transport = &throttleTransport{next: next, throttle: b, read: true}
	return copyClient.Do(req)
}

// ThrottleProvider is optional: other debrid implementations need no changes.
type ThrottleProvider interface{ RequestThrottle() *Throttle }

func WithThrottle(b *Throttle) ClientOption { return func(c *Client) { c.throttle = b } }
