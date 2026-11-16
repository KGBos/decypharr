//go:build compare

// Isolated behavior comparison for liteflix #953 / KGBos/decypharr#26.
//
//	go test -tags compare ./pkg/debrid/providers/torbox -run TestDownloadLinkSemanticsComparison -count=1 -v
//
// Both candidates run in the same provider, against the same fake TorBox
// server, the same account-manager cache, and the same throttle/budget; a new
// provider and a new budget are built per cell. Only the download-link
// semantics differ:
//
//	A  stack as pushed: fetchDownloadLink is a cheap placeholder (no network);
//	   GetDownloadLinkForPlayback resolves the CDN URL once through the budget.
//	B  upstream 8162e05's resolve block grafted into fetchDownloadLink:
//	   requestdl?redirect=false on every lookup, falling back to the redirect URL
//	   on any failure. There is no separate playback entry point.
//
// Set COMPARE_OUT=/path/file.md to also write the report as Markdown.
package torbox

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/request"
	"github.com/sirrobot01/decypharr/pkg/debrid/account"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
)

const cmpCDN = "https://cdn.example.test/movie"

type fakeTorbox struct {
	srv           *httptest.Server
	mode          atomic.Value
	wire          atomic.Int64
	redirectFalse atomic.Int64 // requests with redirect=false (or absent): JSON answer expected
	redirectTrue  atomic.Int64
	n             atomic.Int64
}

func newFakeTorbox(t *testing.T, mode string) *fakeTorbox {
	f := &fakeTorbox{}
	f.mode.Store(mode)
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != requestdlPath {
			http.NotFound(w, r)
			return
		}
		f.wire.Add(1)
		n := f.n.Add(1)
		redirect := r.URL.Query().Get("redirect") == "true"
		if redirect {
			f.redirectTrue.Add(1)
		} else {
			f.redirectFalse.Add(1)
		}
		w.Header().Set("Content-Type", "application/json")
		ok := func() {
			if redirect {
				http.Redirect(w, r, cmpCDN, http.StatusFound)
				return
			}
			_, _ = fmt.Fprintf(w, `{"success":true,"data":%q}`, cmpCDN)
		}
		switch f.mode.Load().(string) {
		case "success":
			ok()
		case "429":
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = fmt.Fprint(w, `{"success":false,"error":"RATE_LIMITED","detail":"slow down"}`)
		case "429-once":
			if n == 1 {
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = fmt.Fprint(w, `{"success":false,"error":"RATE_LIMITED"}`)
				return
			}
			ok()
		case "timeout":
			select {
			case <-time.After(8 * time.Second):
			case <-r.Context().Done():
				return
			}
			ok()
		case "malformed":
			_, _ = fmt.Fprint(w, `<html>oops, not json`)
		case "empty-data":
			_, _ = fmt.Fprint(w, `{"success":true,"data":""}`)
		case "fail-403":
			w.WriteHeader(http.StatusForbidden)
			_, _ = fmt.Fprint(w, `{"success":false,"detail":"denied"}`)
		case "fail-503":
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = fmt.Fprint(w, `{"success":false,"detail":"unavailable"}`)
		case "cap-200":
			_, _ = fmt.Fprint(w, `{"success":false,"error":"DAILY_BANDWIDTH_LIMIT_EXCEEDED"}`)
		}
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func newCompareTorbox(t *testing.T, host string) *Torbox {
	t.Helper()
	config.SetConfigPath(t.TempDir())
	tb, err := New(config.Debrid{Name: "torbox-cmp", Provider: "torbox", APIKey: "main-key", DownloadAPIKeys: []string{"private-download-token"},
		RequestdlBudget: "6000/minute", TorboxBackoffMax: "50ms", RequestdlFreezeMax: "2s"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	tb.Host = host
	tb.autoExpiresLinksAfter = 48 * time.Hour // longer than the 3h CDN cap, to show which cap applies
	return tb
}

// fetchUpstreamSemantics is candidate B: the stack's placeholder construction
// plus upstream 8162e05's resolve-then-fall-back block, unchanged.
func (tb *Torbox) fetchUpstreamSemantics(ctx context.Context, acc *account.Account, id string, file *types.File) (types.DownloadLink, error) {
	dl, _ := tb.fetchDownloadLink(ctx, acc, id, file) // redirect=true placeholder, expiry = autoExpiresLinksAfter
	client := acc.Client()
	if client == nil {
		client = tb.client
	}
	var res DownloadLinksResponse
	resp, err := tb.doGetWithClient(ctx, client, "/api/torrents/requestdl", map[string]string{
		"token": acc.Token, "torrent_id": id, "file_id": file.Id, "redirect": "false",
	}, &res)
	if err == nil && resp.StatusCode == http.StatusOK && res.Success && res.Data != nil && strings.HasPrefix(*res.Data, "http") {
		dl.DownloadLink = *res.Data
	}
	return dl, nil
}

var tokRe = regexp.MustCompile(`token=[^&"\s]+`)

func redact(s, host string) string {
	s = strings.ReplaceAll(s, host, "FAKE")
	return tokRe.ReplaceAllString(s, "token=<tok>")
}

func urlKind(u string) string {
	switch {
	case u == "":
		return "none"
	case strings.HasPrefix(u, cmpCDN):
		return "CDN url"
	case strings.Contains(u, requestdlPath) && strings.Contains(u, "redirect=true"):
		return "redirect placeholder"
	default:
		return "other: " + u
	}
}

type callRes struct {
	kind, err                   string
	expires                     time.Duration
	wire, redirFalse, redirTrue int64
	playback, background, probe uint64
	elapsed                     time.Duration
}

type entryFn func(ctx context.Context, tb *Torbox, file *types.File) (types.DownloadLink, error)

var (
	probeCtx = func() context.Context { return request.WithClass(context.Background(), request.ClassProbe) }
	playCtx  = func() context.Context { return request.WithClass(context.Background(), request.ClassPlayback) }
)

// entries: how each candidate serves a probe/repair lookup and a playback open.
var entries = map[string]entryFn{
	"A probe": func(ctx context.Context, tb *Torbox, f *types.File) (types.DownloadLink, error) {
		return tb.GetDownloadLink(ctx, "1", f)
	},
	"A playback": func(ctx context.Context, tb *Torbox, f *types.File) (types.DownloadLink, error) {
		return tb.GetDownloadLinkForPlayback(ctx, "1", f)
	},
	"B probe": func(ctx context.Context, tb *Torbox, f *types.File) (types.DownloadLink, error) {
		return tb.accountsManager.GetDownloadLink(ctx, "1", f, tb.fetchUpstreamSemantics)
	},
	"B playback": func(ctx context.Context, tb *Torbox, f *types.File) (types.DownloadLink, error) {
		return tb.accountsManager.GetDownloadLink(ctx, "1", f, tb.fetchUpstreamSemantics)
	},
}

func ctxFor(entry string) context.Context {
	if strings.HasSuffix(entry, "probe") {
		return probeCtx()
	}
	return playCtx()
}

func runCall(t *testing.T, tb *Torbox, f *fakeTorbox, fn entryFn, entry string, file *types.File, deadline time.Duration) callRes {
	ctx, cancel := context.WithTimeout(ctxFor(entry), deadline)
	defer cancel()
	start := time.Now()
	dl, err := fn(ctx, tb, file)
	r := callRes{elapsed: time.Since(start).Round(10 * time.Millisecond), kind: urlKind(dl.DownloadLink)}
	if err != nil {
		r.err = redact(err.Error(), f.srv.URL)
		if len(r.err) > 90 {
			r.err = r.err[:90] + "..."
		}
	}
	if !dl.ExpiresAt.IsZero() {
		r.expires = time.Until(dl.ExpiresAt).Round(time.Minute)
	}
	s := tb.RequestdlStats().(request.RequestdlStats)
	r.wire, r.redirFalse, r.redirTrue = f.wire.Load(), f.redirectFalse.Load(), f.redirectTrue.Load()
	r.playback, r.background, r.probe = s.RequestsPlayback, s.RequestsBackground, s.RequestsProbe
	return r
}

func (c callRes) String() string {
	e := c.err
	if e == "" {
		e = "-"
	}
	exp := "-"
	if c.expires != 0 {
		exp = c.expires.String()
	}
	return fmt.Sprintf("%s | err=%s | exp=%s | wire=%d (rd=false:%d, rd=true:%d) | lanes pb/bg/probe=%d/%d/%d | %s",
		c.kind, e, exp, c.wire, c.redirFalse, c.redirTrue, c.playback, c.background, c.probe, c.elapsed)
}

func TestDownloadLinkSemanticsComparison(t *testing.T) {
	scenarios := []struct {
		name, mode string
		deadline   time.Duration
	}{
		{"success", "success", 8 * time.Second},
		{"429 + Retry-After:1 (always)", "429", 8 * time.Second},
		{"429 once, then success", "429-once", 8 * time.Second},
		{"timeout (server hangs 8s, caller deadline 1.5s)", "timeout", 1500 * time.Millisecond},
		{"malformed body (not JSON)", "malformed", 8 * time.Second},
		{"empty data (200, success:true, data:\"\")", "empty-data", 8 * time.Second},
		{"redirect-resolution failure: 403", "fail-403", 8 * time.Second},
		{"redirect-resolution failure: 503", "fail-503", 8 * time.Second},
		{"provider cap in a 200 (success:false)", "cap-200", 8 * time.Second},
	}
	var md strings.Builder
	md.WriteString("# download-link semantics: measured comparison\n\n")
	md.WriteString("Each cell: fresh provider + fresh budget (6000/minute) + fresh fake TorBox. Call 1 then call 2 (50ms later) on the same file, same class; in every failure scenario the fake then recovers (switches to success) and call 3 is made 1.3s later, after any Retry-After freeze. `wire` counts requests the fake server received (all on /api/torrents/requestdl); `rd=false` are JSON-resolution requests, `rd=true` are redirect-style requests. Lane counts are the budget's cumulative admissions.\n\n")
	for _, sc := range scenarios {
		fmt.Fprintf(&md, "## %s\n\n| candidate / entry | call | result |\n| --- | --- | --- |\n", sc.name)
		for _, entry := range []string{"A probe", "A playback", "B probe", "B playback"} {
			f := newFakeTorbox(t, sc.mode)
			tb := newCompareTorbox(t, f.srv.URL)
			file := &types.File{Id: "2", Link: "torbox://1/2", Name: "movie", Size: 20}
			for call := 1; call <= 3; call++ {
				label := fmt.Sprint(call)
				if call == 3 {
					if sc.mode == "success" {
						break
					}
					// The provider recovers; wait out any Retry-After freeze, then ask again.
					f.mode.Store("success")
					time.Sleep(1300 * time.Millisecond)
					label = "3 (provider recovered)"
				}
				r := runCall(t, tb, f, entries[entry], entry, file, sc.deadline)
				fmt.Fprintf(&md, "| %s | %s | %s |\n", entry, label, r)
				time.Sleep(50 * time.Millisecond)
			}
		}
		md.WriteString("\n")
	}

	// Cross-effects when both consumers touch one file: which lane pays for the first requestdl?
	md.WriteString("## probe then playback on the same file (success)\n\n| candidate | step | result |\n| --- | --- | --- |\n")
	for _, cand := range []string{"A", "B"} {
		f := newFakeTorbox(t, "success")
		tb := newCompareTorbox(t, f.srv.URL)
		file := &types.File{Id: "2", Link: "torbox://1/2", Name: "movie", Size: 20}
		for _, step := range []string{"probe", "probe", "playback", "playback"} {
			e := cand + " " + step
			r := runCall(t, tb, f, entries[e], e, file, 8*time.Second)
			fmt.Fprintf(&md, "| %s | %s | %s |\n", cand, step, r)
		}
	}
	md.WriteString("\n## playback then probe on the same file (success)\n\n| candidate | step | result |\n| --- | --- | --- |\n")
	for _, cand := range []string{"A", "B"} {
		f := newFakeTorbox(t, "success")
		tb := newCompareTorbox(t, f.srv.URL)
		file := &types.File{Id: "2", Link: "torbox://1/2", Name: "movie", Size: 20}
		for _, step := range []string{"playback", "probe"} {
			e := cand + " " + step
			r := runCall(t, tb, f, entries[e], e, file, 8*time.Second)
			fmt.Fprintf(&md, "| %s | %s | %s |\n", cand, step, r)
		}
	}

	// Fan-out: 8 probes of 8 different files (a repair sweep) then one playback open.
	md.WriteString("\n## repair sweep: 8 probe lookups of 8 different files, then 1 playback open of file 1 (success)\n\n| candidate | wire | rd=false | lanes pb/bg/probe |\n| --- | --- | --- | --- |\n")
	for _, cand := range []string{"A", "B"} {
		f := newFakeTorbox(t, "success")
		tb := newCompareTorbox(t, f.srv.URL)
		var last callRes
		for i := 1; i <= 8; i++ {
			file := &types.File{Id: fmt.Sprint(i), Link: "torbox://1/" + fmt.Sprint(i), Name: "m" + fmt.Sprint(i), Size: 20}
			last = runCall(t, tb, f, entries[cand+" probe"], cand+" probe", file, 8*time.Second)
		}
		_ = last
		runCall(t, tb, f, entries[cand+" playback"], cand+" playback", &types.File{Id: "1", Link: "torbox://1/1", Name: "m1", Size: 20}, 8*time.Second)
		s := tb.RequestdlStats().(request.RequestdlStats)
		fmt.Fprintf(&md, "| %s | %d | %d | %d/%d/%d |\n", cand, f.wire.Load(), f.redirectFalse.Load(), s.RequestsPlayback, s.RequestsBackground, s.RequestsProbe)
	}

	// Concurrency at the provider layer (not through the link service).
	md.WriteString("\n## 8 concurrent playback opens of one file, provider layer only (success)\n\n| candidate | wire | lanes pb/bg/probe |\n| --- | --- | --- |\n")
	for _, cand := range []string{"A", "B"} {
		f := newFakeTorbox(t, "success")
		tb := newCompareTorbox(t, f.srv.URL)
		file := &types.File{Id: "2", Link: "torbox://1/2", Name: "movie", Size: 20}
		done := make(chan struct{}, 8)
		for i := 0; i < 8; i++ {
			go func() {
				ctx, cancel := context.WithTimeout(playCtx(), 8*time.Second)
				defer cancel()
				_, _ = entries[cand+" playback"](ctx, tb, file)
				done <- struct{}{}
			}()
		}
		for i := 0; i < 8; i++ {
			<-done
		}
		s := tb.RequestdlStats().(request.RequestdlStats)
		fmt.Fprintf(&md, "| %s | %d | %d/%d/%d |\n", cand, f.wire.Load(), s.RequestsPlayback, s.RequestsBackground, s.RequestsProbe)
	}

	_ = url.Values{}
	out := md.String()
	t.Log("\n" + out)
	if p := os.Getenv("COMPARE_OUT"); p != "" {
		if err := os.WriteFile(p, []byte(out), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}
