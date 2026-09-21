package request

import (
	"context"
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

	// DefaultReadWait is the longest remaining cooldown a read will wait out
	// instead of failing fast. A read that would block longer keeps today's
	// ThrottleError so a FUSE read cannot hang for minutes.
	DefaultReadWait = 90 * time.Second
	// MinReadWait is the smallest configurable read-wait threshold.
	MinReadWait = time.Second
	// MaxReadWait bounds the configurable read-wait threshold so a
	// misconfiguration can still only block a read for a short ban.
	MaxReadWait = 5 * time.Minute

	// CounterLogInterval is how often the periodic counter line is emitted.
	CounterLogInterval = 5 * time.Minute
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
	readWait                            time.Duration
	now                                 func() time.Time
	sleep                               func(context.Context, time.Duration) error
	logger                              zerolog.Logger

	// requestdl is the shared /requestdl budget; requestdlMatch selects the
	// requests it applies to. Both are set once at provider startup via
	// UseRequestdl and nil for providers that do not use it.
	requestdl      *RequestdlLimiter
	requestdlMatch func(*http.Request) bool
}

func NewThrottle(threshold int, cooldown, backoffMax time.Duration, log zerolog.Logger) *Throttle {
	return &Throttle{
		threshold:  threshold,
		cooldown:   cooldown,
		backoffMax: backoffMax,
		readWait:   DefaultReadWait,
		now:        time.Now,
		sleep:      sleepContext,
		logger:     log,
	}
}

// WithReadWait sets the longest remaining cooldown a read waits out before it
// proceeds. Non-positive values disable the wait (reads always fail fast).
func (b *Throttle) WithReadWait(d time.Duration) *Throttle {
	if d > 0 {
		b.readWait = d
	}
	return b
}

// UseRequestdl attaches the shared /requestdl budget and the predicate that
// selects the requests it governs. It is set once at provider construction
// before any request is served.
func (b *Throttle) UseRequestdl(limiter *RequestdlLimiter, match func(*http.Request) bool) {
	b.mu.Lock()
	b.requestdl = limiter
	b.requestdlMatch = match
	b.mu.Unlock()
}

// requestdlFor returns the shared budget when req is a governed /requestdl
// call, otherwise nil.
func (b *Throttle) requestdlFor(req *http.Request) *RequestdlLimiter {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	limiter, match := b.requestdl, b.requestdlMatch
	b.mu.Unlock()
	if limiter == nil || match == nil || !match(req) {
		return nil
	}
	return limiter
}

// RequestdlStats exposes the shared budget snapshot for the local API.
func (b *Throttle) RequestdlStats() RequestdlStats {
	if b == nil {
		return RequestdlStats{}
	}
	b.mu.Lock()
	limiter := b.requestdl
	b.mu.Unlock()
	if limiter == nil {
		return RequestdlStats{}
	}
	return limiter.Snapshot()
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

// WaitRead admits a read through an open circuit. When the remaining cooldown
// is at most the read-wait threshold it blocks for the remainder, re-checking
// state (never holding the lock while asleep) and honoring ctx, then proceeds.
// Longer cooldowns return the same ThrottleError as before so a long ban still
// fails fast. The total wait is bounded by one threshold.
func (b *Throttle) WaitRead(ctx context.Context) error {
	return b.waitRead(ctx, b.now().Add(b.readWait))
}

func (b *Throttle) waitRead(ctx context.Context, deadline time.Time) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := b.Before()
		if err == nil {
			return nil
		}
		var throttleErr *ThrottleError
		if !errors.As(err, &throttleErr) {
			return err
		}
		wait := throttleErr.RetryAfter
		if wait <= 0 || wait > b.readWait {
			return err
		}
		if b.now().Add(wait).After(deadline) {
			return err
		}
		if err := b.sleep(ctx, wait); err != nil {
			return err
		}
	}
}

func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
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

// StartCounterLogging emits the throttle counters every interval until ctx is
// done. It is safe to leave running for the provider's lifetime; callers that
// need to stop it (tests) cancel ctx.
func (b *Throttle) StartCounterLogging(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		b.runCounterLog(ctx, ticker.C)
	}()
}

// runCounterLog is the injectable core of StartCounterLogging: tests drive it
// with a synthetic tick channel instead of a wall-clock ticker.
func (b *Throttle) runCounterLog(ctx context.Context, ticks <-chan time.Time) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticks:
			b.LogCounters()
		}
	}
}

// LogCounters reports the cumulative counters once. Exported so the periodic
// line and any on-demand diagnostics share one format.
func (b *Throttle) LogCounters() {
	b.mu.Lock()
	requests, throttled, reads, read429 := b.requests, b.throttled, b.reads, b.read429
	limiter := b.requestdl
	b.mu.Unlock()
	event := b.logger.Info().
		Uint64("requests", requests).
		Uint64("429s", throttled).
		Uint64("read_requests", reads).
		Uint64("read_429s", read429)
	limiter.requestdlLogFields(event)
	event.Msg("TorBox throttle counters")
}

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
	ctx := req.Context()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Reads wait out short cooldowns (bounded, interruptible); API calls keep
	// failing fast exactly as before. Both gate checks in one attempt share a
	// single deadline so the total wait cannot exceed the threshold.
	gate := func() error { return t.throttle.Before() }
	if t.read {
		deadline := t.throttle.now().Add(t.throttle.readWait)
		gate = func() error { return t.throttle.waitRead(ctx, deadline) }
	}
	if err := gate(); err != nil {
		return nil, err
	}
	if t.limiter != nil {
		t.limiter.Take()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := gate(); err != nil {
		return nil, err
	}
	// The shared /requestdl budget is a separate, provider-wide layer on top
	// of the circuit: it rate-limits and prioritizes only requestdl calls, and
	// the final gate re-check means a penalty that starts while this call waits
	// for a token still results in zero wire calls.
	limiter := t.throttle.requestdlFor(req)
	class := ClassOf(ctx)
	if limiter != nil {
		if err := limiter.Take(ctx, class); err != nil {
			return nil, err
		}
		if err := gate(); err != nil {
			return nil, err
		}
	}
	t.throttle.mu.Lock()
	t.throttle.requests++
	if t.read {
		t.throttle.reads++
	}
	t.throttle.mu.Unlock()
	resp, err := t.next.RoundTrip(req)
	t.throttle.Observe(resp, t.read)
	if limiter != nil {
		// Mirror the breaker's honored cooldown (Retry-After bounded by the
		// configured max) so the budget and the breaker freeze together.
		limiter.Observe(resp, class, t.throttle.Remaining())
	}
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
