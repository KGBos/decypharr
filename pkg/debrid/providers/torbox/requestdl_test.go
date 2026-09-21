package torbox

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/request"
	"github.com/sirrobot01/decypharr/pkg/debrid/account"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
)

func TestRequestdlOptionsDefaults(t *testing.T) {
	limiter, ttl := requestdlOptions(config.Debrid{}, 5*time.Minute, zerolog.Nop())
	if got := limiter.BudgetPerMinute(); got != request.DefaultRequestdlBudgetPerMinute {
		t.Fatalf("budget = %v, want %v", got, request.DefaultRequestdlBudgetPerMinute)
	}
	if ttl != request.DefaultRequestdlURLCacheTTL {
		t.Fatalf("url cache ttl = %s, want %s", ttl, request.DefaultRequestdlURLCacheTTL)
	}
	if got := limiter.RampPeriod(); got != time.Duration(request.DefaultRequestdlRampSeconds)*time.Second {
		t.Fatalf("ramp = %s, want %ds", got, request.DefaultRequestdlRampSeconds)
	}
}

func TestRequestdlOptionsOverrides(t *testing.T) {
	limiter, ttl := requestdlOptions(config.Debrid{
		RequestdlBudget:      "120/minute",
		RequestdlRampSeconds: 60,
		RequestdlURLCacheTTL: "2m",
	}, 5*time.Minute, zerolog.Nop())
	if got := limiter.BudgetPerMinute(); got != 120 {
		t.Fatalf("budget = %v, want 120", got)
	}
	if got := limiter.RampPeriod(); got != time.Minute {
		t.Fatalf("ramp = %s, want 1m", got)
	}
	if ttl != 2*time.Minute {
		t.Fatalf("url cache ttl = %s, want 2m", ttl)
	}
}

func TestRequestdlOptionsInvalidFallBackToDefaults(t *testing.T) {
	limiter, ttl := requestdlOptions(config.Debrid{
		RequestdlBudget:      "bogus",
		RequestdlRampSeconds: -5,
		RequestdlURLCacheTTL: "not-a-duration",
	}, 5*time.Minute, zerolog.Nop())
	if got := limiter.BudgetPerMinute(); got != request.DefaultRequestdlBudgetPerMinute {
		t.Fatalf("invalid budget = %v, want the default", got)
	}
	if got := limiter.RampPeriod(); got != time.Duration(request.DefaultRequestdlRampSeconds)*time.Second {
		t.Fatalf("invalid ramp = %s, want the default", got)
	}
	if ttl != request.DefaultRequestdlURLCacheTTL {
		t.Fatalf("invalid ttl = %s, want the default", ttl)
	}
}

func TestRequestdlOptionsCapsRunawayBudget(t *testing.T) {
	limiter, _ := requestdlOptions(config.Debrid{RequestdlBudget: "100000/minute"}, time.Minute, zerolog.Nop())
	if got := limiter.BudgetPerMinute(); got != request.MaxRequestdlBudgetPerMinute {
		t.Fatalf("runaway budget = %v, want the cap %v", got, request.MaxRequestdlBudgetPerMinute)
	}
}

func TestRequestdlOptionsCanDisableURLCache(t *testing.T) {
	_, ttl := requestdlOptions(config.Debrid{RequestdlURLCacheTTL: "0s"}, time.Minute, zerolog.Nop())
	if ttl != 0 {
		t.Fatalf("explicit 0s ttl = %s, want disabled", ttl)
	}
}

func testTorboxWithCache(autoExpire, cacheTTL time.Duration) *Torbox {
	return &Torbox{
		Host:                  "https://api.torbox.app/v1",
		logger:                zerolog.Nop(),
		config:                config.Debrid{Name: "torbox"},
		autoExpiresLinksAfter: autoExpire,
		requestdlCache:        request.NewURLCache[types.DownloadLink](cacheTTL),
	}
}

func TestFetchDownloadLinkCachesWithinTTL(t *testing.T) {
	tb := testTorboxWithCache(time.Hour, time.Minute)
	acc := &account.Account{Token: "token-a", Debrid: "torbox"}
	file := &types.File{Id: "2", Name: "movie.mkv", Size: 10, Link: "torbox://1/2"}

	first, err := tb.fetchDownloadLink(acc, "1", file)
	if err != nil {
		t.Fatal(err)
	}
	second, err := tb.fetchDownloadLink(acc, "1", file)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Generated.Equal(second.Generated) || first.DownloadLink != second.DownloadLink {
		t.Fatal("repeat resolution was not served from the URL cache")
	}

	// The key includes the file and the account, so neither reuses the entry.
	otherFile, err := tb.fetchDownloadLink(acc, "1", &types.File{Id: "3", Name: "other.mkv", Size: 10, Link: "torbox://1/3"})
	if err != nil {
		t.Fatal(err)
	}
	if otherFile.DownloadLink == first.DownloadLink {
		t.Fatal("cache key ignored the file id")
	}
	otherAccount, err := tb.fetchDownloadLink(&account.Account{Token: "token-b", Debrid: "torbox"}, "1", file)
	if err != nil {
		t.Fatal(err)
	}
	if otherAccount.DownloadLink == first.DownloadLink {
		t.Fatal("cache key ignored the account")
	}
}

func TestFetchDownloadLinkCacheExpires(t *testing.T) {
	tb := testTorboxWithCache(time.Hour, time.Millisecond)
	acc := &account.Account{Token: "token-a", Debrid: "torbox"}
	file := &types.File{Id: "2", Name: "movie.mkv", Size: 10, Link: "torbox://1/2"}

	first, err := tb.fetchDownloadLink(acc, "1", file)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	second, err := tb.fetchDownloadLink(acc, "1", file)
	if err != nil {
		t.Fatal(err)
	}
	if first.Generated.Equal(second.Generated) {
		t.Fatal("entry survived past its TTL")
	}
}

func TestFetchDownloadLinkRespectsAutoExpire(t *testing.T) {
	tb := testTorboxWithCache(2*time.Millisecond, time.Hour)
	acc := &account.Account{Token: "token-a", Debrid: "torbox"}
	file := &types.File{Id: "2", Name: "movie.mkv", Size: 10, Link: "torbox://1/2"}

	first, err := tb.fetchDownloadLink(acc, "1", file)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(6 * time.Millisecond)
	second, err := tb.fetchDownloadLink(acc, "1", file)
	if err != nil {
		t.Fatal(err)
	}
	if first.Generated.Equal(second.Generated) {
		t.Fatal("URL cache outlived auto_expire_links_after")
	}
}

func TestRequestdlBudgetGovernsOnlyRequestdl(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	tb, err := New(config.Debrid{
		Name:                   "torbox-test",
		Provider:               "torbox",
		APIKey:                 "test",
		DownloadAPIKeys:        []string{"test"},
		RequestdlBudget:        "120/minute",
		TorboxBackoffMax:       "20ms",
		TorboxBreakerThreshold: 1,
		TorboxBreakerCooldown:  "20ms",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// The matcher reads Host dynamically, so the test server stands in for
	// api.torbox.app.
	tb.Host = server.URL

	stats := tb.RequestdlStats().(request.RequestdlStats)
	if stats.BudgetPerMinute != 120 {
		t.Fatalf("budget = %v, want 120", stats.BudgetPerMinute)
	}

	req, _ := http.NewRequest(http.MethodGet, server.URL+"/api/torrents/requestdl?token=test", nil)
	resp, err := tb.throttle.Do(server.Client(), req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	stats = tb.RequestdlStats().(request.RequestdlStats)
	if stats.RequestsBackground != 1 {
		t.Fatalf("requestdl call not counted: %+v", stats)
	}

	// A plain API call must not consume the /requestdl budget or create a
	// resolved-URL cache entry.
	req, _ = http.NewRequest(http.MethodGet, server.URL+"/api/torrents/mylist", nil)
	resp, err = tb.throttle.Do(server.Client(), req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	stats = tb.RequestdlStats().(request.RequestdlStats)
	if stats.RequestsBackground != 1 || stats.CachedURLs != 0 {
		t.Fatalf("non-requestdl traffic consumed the budget: %+v", stats)
	}
	if calls.Load() != 2 {
		t.Fatalf("wire calls = %d, want 2", calls.Load())
	}
}

func TestRequestdlStatsReportsCachedURLs(t *testing.T) {
	tb := testTorboxWithCache(time.Hour, time.Minute)
	acc := &account.Account{Token: "token-a", Debrid: "torbox"}
	if _, err := tb.fetchDownloadLink(acc, "1", &types.File{Id: "2", Name: "movie.mkv", Size: 10, Link: "torbox://1/2"}); err != nil {
		t.Fatal(err)
	}
	stats := tb.RequestdlStats().(request.RequestdlStats)
	if stats.CachedURLs != 1 {
		t.Fatalf("cached urls = %d, want 1", stats.CachedURLs)
	}
}
