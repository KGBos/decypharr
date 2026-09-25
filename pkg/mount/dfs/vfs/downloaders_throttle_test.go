package vfs

import (
	"errors"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/request"
)

// closeTestDownloaders shuts a fixture Downloaders down. The bench fixture
// pre-marks a fake stream registration to keep the read path off the nil
// manager; that marker must be cleared first so Close does not dereference
// the nil manager when un-tracking the stream.
func closeTestDownloaders(dls *Downloaders) {
	dls.mu.Lock()
	dls.streamID = ""
	dls.streamTracked.Store(false)
	dls.mu.Unlock()
	_ = dls.Close(nil)
}

// TestBackpressureDoesNotTripFileCircuitBreaker reproduces the read-side 429
// storm from KGBos/liteflix#257: every chunk fails with provider backpressure
// while the TorBox gate holds the Retry-After deadline. Backpressure is a
// gate-level condition with a known deadline, not a file failure, so it must
// never consume the file's error budget or trip the file-local circuit
// breaker. Otherwise the file stays unreadable for the 2-minute breaker
// cooldown even after the provider gate has cleared.
func TestBackpressureDoesNotTripFileCircuitBreaker(t *testing.T) {
	_, dls := newBenchItem(t, 64<<20)
	defer closeTestDownloaders(dls)

	// More failures than the breaker budget allows.
	for i := 0; i < maxErrorCount+5; i++ {
		dls.countErrors(0, &request.ThrottleError{RetryAfter: 120 * time.Second})
	}

	dls.mu.Lock()
	count, lastErr := dls.errorCount, dls.lastErr
	dls.mu.Unlock()
	if count != 0 {
		t.Fatalf("backpressure consumed the file error budget: count=%d", count)
	}
	if lastErr != nil {
		t.Fatalf("backpressure recorded as the file's last error: %v", lastErr)
	}
	if dls.isCircuitOpen() {
		t.Fatal("backpressure tripped the file circuit breaker")
	}
}

// TestBackpressureDoesNotConsumeErrorBudget interleaves real failures with
// backpressure: only the real failures may advance the breaker budget.
func TestBackpressureDoesNotConsumeErrorBudget(t *testing.T) {
	_, dls := newBenchItem(t, 64<<20)
	defer closeTestDownloaders(dls)

	boom := errors.New("connection reset")
	for i := 0; i < maxErrorCount-1; i++ {
		dls.countErrors(0, boom)
		dls.countErrors(0, &request.ThrottleError{RetryAfter: 120 * time.Second})
	}
	if dls.isCircuitOpen() {
		t.Fatal("breaker opened before the real-error budget was exhausted")
	}
	dls.countErrors(0, boom)
	if !dls.isCircuitOpen() {
		t.Fatal("real errors must still trip the file circuit breaker")
	}
}

// TestRealErrorsStillTripFileCircuitBreaker guards against overcorrection:
// genuine chunk failures keep the existing breaker behavior.
func TestRealErrorsStillTripFileCircuitBreaker(t *testing.T) {
	_, dls := newBenchItem(t, 64<<20)
	defer closeTestDownloaders(dls)

	for i := 0; i < maxErrorCount; i++ {
		dls.countErrors(0, errors.New("connection reset"))
	}
	if !dls.isCircuitOpen() {
		t.Fatal("sustained chunk failures must trip the file circuit breaker")
	}
}
