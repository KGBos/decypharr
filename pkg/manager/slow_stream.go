package manager

import (
	"io"
	"time"

	"github.com/sirrobot01/decypharr/pkg/manager/link"
)

const (
	// slowStreamThresholdBps is the sustained throughput below which a stream
	// is considered degraded. 100 kB/s is far below any usable video bitrate
	// — and well above the ~22 kB/s observed in the incident that motivated
	// this — so healthy streams, including slow starts, never trip it.
	slowStreamThresholdBps = 100 * 1024
	// slowStreamWindow is the throughput measurement window. It accumulates
	// blocked Read time only, so consumer pauses and small-buffer reads never
	// count against the stream.
	slowStreamWindow = 30 * time.Second
	// slowStreamBadWindows is how many consecutive sub-threshold windows
	// trigger a swap: 60 seconds of sustained degradation.
	slowStreamBadWindows = 2
)

// slowWatchBody wraps a stream body and measures the throughput of actual
// blocked Read time. When throughput stays below the threshold for
// slowStreamBadWindows consecutive windows it fails the Read with a
// *link.SlowStreamError, which the session recovery path turns into an
// immediate link refresh (see httpTransport.recover) — no waiting for the
// scheduled link-refresh interval.
type slowWatchBody struct {
	rc   io.ReadCloser
	host string

	thresholdBps float64
	window       time.Duration
	badWindows   int

	winBytes   int64
	winDur     time.Duration
	consecBad  int
	totalBytes int64
	tripped    bool

	onFirstWindow   func(bps float64)
	firstWindowDone bool
}

func newSlowWatchBody(rc io.ReadCloser, host string, onFirstWindow func(float64)) *slowWatchBody {
	return newSlowWatchBodyWithParams(rc, host, onFirstWindow,
		slowStreamThresholdBps, slowStreamWindow, slowStreamBadWindows)
}

func newSlowWatchBodyWithParams(rc io.ReadCloser, host string, onFirstWindow func(float64),
	thresholdBps float64, window time.Duration, badWindows int) *slowWatchBody {
	return &slowWatchBody{
		rc:            rc,
		host:          host,
		thresholdBps:  thresholdBps,
		window:        window,
		badWindows:    badWindows,
		onFirstWindow: onFirstWindow,
	}
}

func (w *slowWatchBody) Read(p []byte) (int, error) {
	start := time.Now()
	n, err := w.rc.Read(p)
	d := time.Since(start)

	// A terminating read (EOF, error) passes through untouched: the session's
	// normal recovery already handles failures, and tripping on the way out
	// would swap a link the consumer was done with.
	if err != nil {
		return n, err
	}

	w.winBytes += int64(n)
	w.winDur += d
	w.totalBytes += int64(n)

	// Evaluate every elapsed window, not just one: a single Read blocked
	// for minutes with zero bytes (a hard stall the 90s stall watchdog
	// would otherwise ride with a same-link retry) trips here instead.
	// Consumed windows are subtracted pro-rata so leftover blocked time and
	// bytes carry into the next window instead of being discarded.
	for !w.tripped && w.winDur >= w.window {
		// window is always > 0, so this division is safe.
		bps := float64(w.winBytes) / w.winDur.Seconds()
		if !w.firstWindowDone {
			w.firstWindowDone = true
			if w.onFirstWindow != nil {
				w.onFirstWindow(bps)
			}
		}
		if bps < w.thresholdBps {
			w.consecBad++
		} else {
			w.consecBad = 0
		}
		frac := float64(w.window) / float64(w.winDur)
		w.winBytes -= int64(float64(w.winBytes) * frac)
		w.winDur -= w.window
		if w.consecBad >= w.badWindows {
			w.tripped = true
			// Drop this Read's bytes — they are re-read at the same offset
			// after the swap. Returning them alongside the error would have
			// the session deliver the bytes and swallow the error, and the
			// swap would never happen.
			return 0, link.NewSlowStreamError(w.host, w.totalBytes, bps)
		}
	}
	return n, nil
}

func (w *slowWatchBody) Close() error { return w.rc.Close() }
