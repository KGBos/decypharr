package request

import (
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"go.uber.org/ratelimit"
)

const (
	// maxEscalationWait bounds repeated re-trips so a long ban is not probed
	// at a fixed short interval. Server advice above this is never truncated.
	maxEscalationWait = 24 * time.Hour
	// longBanThreshold is the server-advised wait above which a ban is logged
	// distinctly. It matches the pre-#160 accepted upper bound.
	longBanThreshold = 15 * time.Minute
	// escalationQuietPeriod drops a stale escalation level once the provider
	// has gone a full day without a 429.
	escalationQuietPeriod = 24 * time.Hour
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
	escalation                          int
	cooldown, backoffMax                time.Duration
	until                               time.Time
	last429                             time.Time
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
		// Consecutive 429s and the escalation level survive the cooldown: a
		// re-trip without an intervening success doubles the next wait.
		b.logger.Info().Int("escalation", b.escalation).Uint64("requests", b.requests).Uint64("429s", b.throttled).Uint64("read_requests", b.reads).Uint64("read_429s", b.read429).Msg("TorBox circuit closed after cooldown")
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
	now := b.now()
	if resp.StatusCode != http.StatusTooManyRequests {
		// Do not let late successes erase cooldown or the consecutive count.
		if !b.open && !now.Before(b.until) && (resp.StatusCode >= 200 && resp.StatusCode < 300 || resp.StatusCode >= 400) {
			b.consecutive = 0
			b.escalation = 0
		}
		return
	}
	if b.escalation > 0 && !b.last429.IsZero() && now.Sub(b.last429) >= escalationQuietPeriod {
		b.consecutive = 0
		b.escalation = 0
	}
	b.last429 = now
	b.throttled++
	if read {
		b.read429++
	}
	b.consecutive++
	serverWait, hasServerWait := retryAfterWait(resp)
	wait := retryAfterBackoff(time.Second, b.backoffMax, b.consecutive-1, resp)
	opening := !b.open && b.consecutive >= b.threshold
	if b.consecutive >= b.threshold {
		// A re-trip (consecutive beyond the threshold, no success since the
		// last open) escalates the wait instead of repeating the same one.
		if opening && b.consecutive > b.threshold {
			b.escalation++
		}
		b.open = true
		wait = max(wait, b.cooldown)
		wait = escalateWait(wait, b.escalation)
	}
	until := now.Add(wait)
	if until.After(b.until) {
		b.until = until
	}
	switch {
	case hasServerWait && serverWait >= longBanThreshold:
		seconds := int64(serverWait / time.Second)
		b.logger.Warn().Int64("retry_after_seconds", seconds).Dur("wait", wait).Int("escalation", b.escalation).Int("consecutive_429s", b.consecutive).Uint64("requests", b.requests).Uint64("429s", b.throttled).Msg(fmt.Sprintf("TorBox long ban: retry-after=%ds", seconds))
	case opening:
		b.logger.Warn().Dur("cooldown", wait).Int("consecutive_429s", b.consecutive).Int("escalation", b.escalation).Uint64("requests", b.requests).Uint64("429s", b.throttled).Uint64("read_requests", b.reads).Uint64("read_429s", b.read429).Msg("TorBox circuit opened")
	case read:
		b.logger.Debug().Dur("retry_after", wait).Uint64("read_requests", b.reads).Uint64("read_429s", b.read429).Msg("TorBox read failed fast on HTTP 429")
	}
}
func (b *Throttle) isOpen() bool { b.mu.Lock(); defer b.mu.Unlock(); return b.open }

// retryAfterWait returns the server's unbounded Retry-After advice for logging.
// The second result is false when the header is absent, invalid, or in the past.
func retryAfterWait(resp *http.Response) (time.Duration, bool) {
	if resp == nil {
		return 0, false
	}
	ra := strings.TrimSpace(resp.Header.Get("Retry-After"))
	if secs, err := strconv.ParseUint(ra, 10, 64); err == nil && secs > 0 {
		if secs > math.MaxInt64/uint64(time.Second) {
			return 0, false
		}
		return time.Duration(secs) * time.Second, true
	}
	if at, err := http.ParseTime(ra); err == nil {
		if wait := time.Until(at); wait > 0 {
			return wait, true
		}
	}
	return 0, false
}

// escalateWait doubles wait once per escalation level, capped at
// maxEscalationWait. The honored wait is a floor, so server advice is never
// shortened by escalation.
func escalateWait(wait time.Duration, level int) time.Duration {
	if level <= 0 || wait <= 0 {
		return wait
	}
	ceiling := max(wait, maxEscalationWait)
	for i := 0; i < level; i++ {
		if wait >= ceiling {
			break
		}
		if wait > ceiling/2 {
			return ceiling
		}
		wait *= 2
	}
	return wait
}

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
