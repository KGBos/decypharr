package torbox

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/request"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
)

func playbackTorbox(t *testing.T, host string, expiry time.Duration) *Torbox {
	t.Helper()
	config.SetConfigPath(t.TempDir())
	tb, err := New(config.Debrid{Name: "torbox-test", Provider: "torbox", APIKey: "main-key", DownloadAPIKeys: []string{"private-download-token"}, TorboxBackoffMax: "20ms", RequestdlFreezeMax: "20ms"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	tb.Host = host
	tb.autoExpiresLinksAfter = expiry
	return tb
}

func TestPlaybackStoresCDNAndCapsExpiry(t *testing.T) {
	for _, tc := range []struct {
		name             string
		configured, want time.Duration
	}{
		{"long setting", 48 * time.Hour, 3 * time.Hour},
		{"short setting", time.Hour, time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != requestdlPath || r.Method != http.MethodGet {
					t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
				}
				if r.URL.Query().Get("token") != "private-download-token" {
					t.Error("wrong download account")
				}
				calls.Add(1)
				_, _ = fmt.Fprint(w, `{"success":true,"data":"https://cdn.example.test/movie"}`)
			}))
			defer server.Close()
			tb := playbackTorbox(t, server.URL, tc.configured)
			file := &types.File{Id: "2", Link: "torbox://1/2", Name: "movie", Size: 20}
			// The repair/health probe must not spend a requestdl admission.
			probe, err := tb.GetDownloadLink(context.Background(), "1", file)
			if err != nil || probe.Empty() || calls.Load() != 0 {
				t.Fatalf("probe: %v, calls=%d", err, calls.Load())
			}
			start := time.Now()
			dl, err := tb.GetDownloadLinkForPlayback(request.WithClass(context.Background(), request.ClassPlayback), "1", file)
			if err != nil {
				t.Fatal(err)
			}
			if dl.DownloadLink != "https://cdn.example.test/movie" {
				t.Fatalf("stored link=%q", dl.DownloadLink)
			}
			if got := dl.ExpiresAt.Sub(start); got < tc.want-time.Second || got > tc.want+time.Second {
				t.Fatalf("expiry=%s, want %s", got, tc.want)
			}
			cached, err := tb.GetDownloadLink(context.Background(), "1", file)
			if err != nil || cached.DownloadLink != dl.DownloadLink || calls.Load() != 1 {
				t.Fatalf("cache=%q, err=%v, calls=%d", cached.DownloadLink, err, calls.Load())
			}
			_, err = tb.GetDownloadLinkForPlayback(context.Background(), "1", file)
			if err != nil || calls.Load() != 1 {
				t.Fatalf("repeat: %v, calls=%d", err, calls.Load())
			}
		})
	}
}

func TestPlaybackRejectsBadRequestdlResponseWithoutCaching(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{"empty body", 200, ""},
		{"empty data", 200, `{"success":true,"data":""}`},
		{"non 2xx", 403, `{"success":false,"detail":"denied"}`},
		{"server error is not retried", 503, `{"success":false,"detail":"unavailable"}`},
		{"daily cap", 403, `{"success":false,"error":"DAILY_BANDWIDTH_LIMIT_EXCEEDED","detail":"token=SECRET"}`},
		{"daily cap in success status", 200, `{"success":false,"error":"DAILY_BANDWIDTH_LIMIT_EXCEEDED","detail":"token=SECRET"}`},
		{"untrusted code", 403, `{"success":false,"error":"token=SECRET","detail":"token=SECRET"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(tc.status)
				_, _ = fmt.Fprint(w, tc.body)
			}))
			defer server.Close()
			tb := playbackTorbox(t, server.URL, time.Hour)
			file := &types.File{Id: "2", Link: "torbox://1/2", Name: "movie"}
			dl, err := tb.GetDownloadLinkForPlayback(context.Background(), "1", file)
			if err == nil || !dl.Empty() {
				t.Fatalf("link=%q, err=%v", dl.DownloadLink, err)
			}
			if strings.Contains(tc.name, "daily cap") && (!strings.Contains(err.Error(), "DAILY_BANDWIDTH_LIMIT_EXCEEDED") || strings.Contains(err.Error(), "SECRET")) {
				t.Fatalf("provider code missing or detail leaked: %v", err)
			}
			if tc.name == "untrusted code" && (strings.Contains(err.Error(), "SECRET") || !strings.Contains(err.Error(), "provider_error_code=UNKNOWN")) {
				t.Fatalf("untrusted provider text leaked: %v", err)
			}
			if got := tb.AccountManager().Current().DownloadLinksCount(); got != 0 {
				t.Fatalf("failed resolution cached %d links", got)
			}
			if calls.Load() != 1 {
				t.Fatalf("resolution made %d requestdl calls", calls.Load())
			}
			cached, err := tb.GetDownloadLink(context.Background(), "1", file)
			if err != nil || strings.Contains(cached.DownloadLink, "cdn") {
				t.Fatalf("cached=%q, err=%v", cached.DownloadLink, err)
			}
		})
	}
}

func TestPlaybackRedactsTransportURL(t *testing.T) {
	tb := playbackTorbox(t, "https://127.0.0.1:1", time.Hour)
	tb.client = request.New(request.WithMaxRetries(0))
	_, err := tb.GetDownloadLinkForPlayback(context.Background(), "1", &types.File{Id: "2", Link: "torbox://1/2", Name: "movie"})
	if err == nil || strings.Contains(err.Error(), "private-download-token") {
		t.Fatalf("transport error leaked token: %v", err)
	}
}

// cdnServer answers every requestdl call with a CDN URL and counts the calls.
func cdnServer(t *testing.T, calls *atomic.Int64) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = fmt.Fprint(w, `{"success":true,"data":"https://cdn.example.test/movie"}`)
	}))
	t.Cleanup(server.Close)
	return server
}

// budgetTorbox is playbackTorbox with a budget generous enough that the
// default call spacing does not slow tests that make several resolutions.
func budgetTorbox(t *testing.T, host string, expiry time.Duration) *Torbox {
	t.Helper()
	config.SetConfigPath(t.TempDir())
	tb, err := New(config.Debrid{Name: "torbox-test", Provider: "torbox", APIKey: "main-key", DownloadAPIKeys: []string{"private-download-token"}, RequestdlBudget: "6000/minute", TorboxBackoffMax: "20ms", RequestdlFreezeMax: "20ms"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	tb.Host = host
	tb.autoExpiresLinksAfter = expiry
	return tb
}

func lanes(tb *Torbox) request.RequestdlStats { return tb.RequestdlStats().(request.RequestdlStats) }

// Repair and health probes call GetDownloadLink. They must get the cheap
// placeholder without touching the network or the requestdl budget.
func TestProbeLookupsSpendNoRequestdlBudget(t *testing.T) {
	var calls atomic.Int64
	server := cdnServer(t, &calls)
	tb := playbackTorbox(t, server.URL, time.Hour)
	for _, id := range []string{"2", "3"} {
		file := &types.File{Id: id, Link: "torbox://1/" + id, Name: "movie" + id}
		for range 3 {
			dl, err := tb.GetDownloadLink(context.Background(), "1", file)
			if err != nil || !strings.HasPrefix(dl.DownloadLink, server.URL+requestdlPath+"?") || !strings.Contains(dl.DownloadLink, "redirect=true") {
				t.Fatalf("probe link=%q err=%v, want the redirect placeholder", dl.DownloadLink, err)
			}
		}
	}
	if s := lanes(tb); calls.Load() != 0 || s.RequestsPlayback != 0 || s.RequestsBackground != 0 {
		t.Fatalf("probe spent requestdl: wire=%d stats=%+v", calls.Load(), s)
	}
}

// The admission lane follows the caller's class: playback is charged to the
// playback lane, unclassified (bulk) work to the background lane.
func TestPlaybackResolutionIsChargedToTheCallersLane(t *testing.T) {
	var calls atomic.Int64
	server := cdnServer(t, &calls)
	tb := budgetTorbox(t, server.URL, time.Hour)
	if _, err := tb.GetDownloadLinkForPlayback(request.WithClass(context.Background(), request.ClassPlayback), "1", &types.File{Id: "2", Link: "torbox://1/2", Name: "a"}); err != nil {
		t.Fatal(err)
	}
	if s := lanes(tb); s.RequestsPlayback != 1 || s.RequestsBackground != 0 {
		t.Fatalf("playback call: %+v", s)
	}
	if _, err := tb.GetDownloadLinkForPlayback(context.Background(), "1", &types.File{Id: "3", Link: "torbox://1/3", Name: "b"}); err != nil {
		t.Fatal(err)
	}
	if s := lanes(tb); s.RequestsPlayback != 1 || s.RequestsBackground != 1 || calls.Load() != 2 {
		t.Fatalf("unclassified call: wire=%d stats=%+v", calls.Load(), s)
	}
}

// An expired cached CDN URL is replaced by exactly one new budgeted
// resolution, which is then cached again.
func TestPlaybackReresolvesOnceAfterCDNExpiry(t *testing.T) {
	var calls atomic.Int64
	server := cdnServer(t, &calls)
	tb := budgetTorbox(t, server.URL, time.Hour)
	file := &types.File{Id: "2", Link: "torbox://1/2", Name: "movie", Size: 20}
	play := request.WithClass(context.Background(), request.ClassPlayback)

	dl, err := tb.GetDownloadLinkForPlayback(play, "1", file)
	if err != nil || calls.Load() != 1 {
		t.Fatalf("first resolve: %v, calls=%d", err, calls.Load())
	}
	dl.ExpiresAt = time.Now().Add(-time.Minute)
	tb.accountsManager.StoreDownloadLink(dl)

	fresh, err := tb.GetDownloadLinkForPlayback(play, "1", file)
	if err != nil || fresh.DownloadLink != "https://cdn.example.test/movie" || !fresh.ExpiresAt.After(time.Now()) {
		t.Fatalf("after expiry: link=%q expires=%v err=%v", fresh.DownloadLink, fresh.ExpiresAt, err)
	}
	if calls.Load() != 2 || lanes(tb).RequestsPlayback != 2 {
		t.Fatalf("expiry cost: wire=%d stats=%+v, want exactly one more resolution", calls.Load(), lanes(tb))
	}
	if _, err := tb.GetDownloadLinkForPlayback(play, "1", file); err != nil || calls.Load() != 2 {
		t.Fatalf("fresh link not cached: %v, calls=%d", err, calls.Load())
	}
}
