package vfs

import (
	"errors"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/request"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/vfs/ranges"
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

// TestActiveWaitersFailFastOnUnwaitableBackpressure verifies that when backpressure
// exceeds the read-wait tolerance (e.g. 120s > 90s), kickWaiters fails active waiters
// with the typed ThrottleError cleanly instead of respawning downloaders in a tight loop.
func TestActiveWaitersFailFastOnUnwaitableBackpressure(t *testing.T) {
	_, dls := newBenchItem(t, 64<<20)
	defer closeTestDownloaders(dls)

	errChan := make(chan error, 1)
	dls.mu.Lock()
	dls.waiters = append(dls.waiters, waiter{r: ranges.Range{Pos: 0, Size: 1 << 20}, errChan: errChan})
	dls.waiterCount.Add(1)
	dls.recomputeMinWaiterEndLocked()
	dls.mu.Unlock()

	dls.countErrors(0, &request.ThrottleError{RetryAfter: 120 * time.Second})
	dls.kickWaiters()

	select {
	case err := <-errChan:
		tErr := request.BackpressureError(err)
		if tErr == nil || tErr.RetryAfter <= 0 {
			t.Fatalf("expected positive ThrottleError, got %v", err)
		}
	default:
		t.Fatal("active waiter was not unblocked on unwaitable backpressure")
	}

	dls.mu.Lock()
	count, lastErr, waitersLen, dlsLen := dls.errorCount, dls.lastErr, len(dls.waiters), len(dls.dls)
	dls.mu.Unlock()

	if count != 0 {
		t.Fatalf("backpressure consumed error budget: count=%d", count)
	}
	if lastErr != nil {
		t.Fatalf("backpressure set as lastErr: %v", lastErr)
	}
	if dls.isCircuitOpen() {
		t.Fatal("backpressure opened circuit breaker")
	}
	if waitersLen != 0 {
		t.Fatalf("waiters slice not emptied: len=%d", waitersLen)
	}
	if dlsLen != 0 {
		t.Fatalf("downloaders were spawned despite unwaitable backpressure: len=%d", dlsLen)
	}
}

// TestActiveWaitersFailFastOnUncertainBackpressure verifies that uncertain rate limits
// (RetryAfter < 0) immediately fail active waiters with the operator recovery error.
func TestActiveWaitersFailFastOnUncertainBackpressure(t *testing.T) {
	_, dls := newBenchItem(t, 64<<20)
	defer closeTestDownloaders(dls)

	errChan := make(chan error, 1)
	dls.mu.Lock()
	dls.waiters = append(dls.waiters, waiter{r: ranges.Range{Pos: 0, Size: 1 << 20}, errChan: errChan})
	dls.waiterCount.Add(1)
	dls.recomputeMinWaiterEndLocked()
	dls.mu.Unlock()

	dls.countErrors(0, &request.ThrottleError{RetryAfter: -1})
	dls.kickWaiters()

	select {
	case err := <-errChan:
		tErr := request.BackpressureError(err)
		if tErr == nil || tErr.RetryAfter >= 0 {
			t.Fatalf("expected uncertain ThrottleError (RetryAfter < 0), got %v", err)
		}
	default:
		t.Fatal("active waiter was not unblocked on uncertain backpressure")
	}

	dls.mu.Lock()
	count, waitersLen, dlsLen := dls.errorCount, len(dls.waiters), len(dls.dls)
	dls.mu.Unlock()

	if count != 0 {
		t.Fatalf("uncertain backpressure consumed error budget: count=%d", count)
	}
	if dls.isCircuitOpen() {
		t.Fatal("uncertain backpressure opened circuit breaker")
	}
	if waitersLen != 0 {
		t.Fatalf("waiters slice not emptied: len=%d", waitersLen)
	}
	if dlsLen != 0 {
		t.Fatalf("downloaders spawned during uncertain backpressure: len=%d", dlsLen)
	}
}

// TestShortBackpressureDoesNotFailWaitersAndAvoidsTightLoop verifies that waitable short
// backpressure (e.g. 50ms <= readWait) does not fail active waiters and prevents tight-loop
// respawning.
func TestShortBackpressureDoesNotFailWaitersAndAvoidsTightLoop(t *testing.T) {
	_, dls := newBenchItem(t, 64<<20)
	defer closeTestDownloaders(dls)

	errChan := make(chan error, 1)
	dls.mu.Lock()
	dls.waiters = append(dls.waiters, waiter{r: ranges.Range{Pos: 0, Size: 1 << 20}, errChan: errChan})
	dls.waiterCount.Add(1)
	dls.recomputeMinWaiterEndLocked()
	dls.mu.Unlock()

	dls.countErrors(0, &request.ThrottleError{RetryAfter: 50 * time.Millisecond})
	dls.kickWaiters()

	// Waiter should NOT be failed during waitable short backpressure
	select {
	case err := <-errChan:
		t.Fatalf("waiter was failed prematurely during short backpressure: %v", err)
	default:
	}

	dls.mu.Lock()
	waitersLen, dlsLen := len(dls.waiters), len(dls.dls)
	dls.mu.Unlock()

	if waitersLen != 1 {
		t.Fatalf("waiter should remain pending: len=%d", waitersLen)
	}
	if dlsLen != 0 {
		t.Fatalf("downloaders should not be spawned while backpressure cooldown is active: len=%d", dlsLen)
	}

	// Wait for cooldown to pass
	time.Sleep(70 * time.Millisecond)

	dls.mu.Lock()
	bp := dls.currentBackpressureLocked()
	dls.mu.Unlock()
	if bp != nil {
		t.Fatalf("backpressure should have cleared after cooldown: %v", bp)
	}
}

// TestCachedRangeFulfilledEvenDuringBackpressure verifies that waiters requesting
// ranges already present in cache are fulfilled with nil even when the gate is throttled.
func TestCachedRangeFulfilledEvenDuringBackpressure(t *testing.T) {
	item, dls := newBenchItem(t, 64<<20)
	defer closeTestDownloaders(dls)

	r := ranges.Range{Pos: 0, Size: 1 << 20}
	item.metaMu.Lock()
	item.info.Rs = ranges.Ranges{r}
	item.metaMu.Unlock()

	errChan := make(chan error, 1)
	dls.mu.Lock()
	dls.waiters = append(dls.waiters, waiter{r: r, errChan: errChan})
	dls.waiterCount.Add(1)
	dls.recomputeMinWaiterEndLocked()
	dls.mu.Unlock()

	dls.countErrors(0, &request.ThrottleError{RetryAfter: 120 * time.Second})
	dls.kickWaiters()

	select {
	case err := <-errChan:
		if err != nil {
			t.Fatalf("expected nil error for cached range, got %v", err)
		}
	default:
		t.Fatal("cached waiter was not fulfilled")
	}
}

// TestEnsureDownloaderRejectsUnwaitableBackpressure verifies ensureDownloaderLocked rejects
// unwaitable backpressure without spawning downloaders.
func TestEnsureDownloaderRejectsUnwaitableBackpressure(t *testing.T) {
	_, dls := newBenchItem(t, 64<<20)
	defer closeTestDownloaders(dls)

	dls.countErrors(0, &request.ThrottleError{RetryAfter: 120 * time.Second})

	dls.mu.Lock()
	err := dls.ensureDownloaderLocked(ranges.Range{Pos: 0, Size: 1 << 20}, false)
	dlsLen := len(dls.dls)
	dls.mu.Unlock()

	if request.BackpressureError(err) == nil {
		t.Fatalf("expected ThrottleError, got %v", err)
	}
	if dlsLen != 0 {
		t.Fatalf("downloader spawned during unwaitable backpressure: len=%d", dlsLen)
	}
}

// TestBackpressureDoesNotSpinInfiniteDownloaders reproduces the Red Team review scenario
// where downloaders hitting 429 backpressure could trigger an infinite kickWaiters loop
// spawning dozens of downloaders per second. It verifies that when a downloader fails with
// unwaitable backpressure, active waiters are terminated fast and 0 new downloaders are spawned.
func TestBackpressureDoesNotSpinInfiniteDownloaders(t *testing.T) {
	_, dls := newBenchItem(t, 64<<20)
	defer closeTestDownloaders(dls)

	errChan := make(chan error, 1)
	dls.mu.Lock()
	dls.waiters = append(dls.waiters, waiter{r: ranges.Range{Pos: 0, Size: 1 << 20}, errChan: errChan})
	dls.waiterCount.Add(1)
	dls.recomputeMinWaiterEndLocked()
	dls.mu.Unlock()

	// Simulate downloader terminating with 120s backpressure
	dls.countErrors(0, &request.ThrottleError{RetryAfter: 120 * time.Second})
	dls.kickWaiters()

	// Waiter must be unblocked immediately with ThrottleError
	select {
	case err := <-errChan:
		if request.BackpressureError(err) == nil {
			t.Fatalf("expected ThrottleError, got %v", err)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("waiter remained hung on errChan instead of failing fast")
	}

	// Verify no downloaders were spawned
	dls.mu.Lock()
	dlsLen := len(dls.dls)
	waitersLen := len(dls.waiters)
	dls.mu.Unlock()

	if dlsLen != 0 {
		t.Fatalf("downloaders were spawned in a loop: len=%d", dlsLen)
	}
	if waitersLen != 0 {
		t.Fatalf("waiter was not removed: len=%d", waitersLen)
	}
}
