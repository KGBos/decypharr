package link

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/request"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/debrid/providers/torbox"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// Many readers opening the same file at once must cost one budgeted requestdl,
// not one each: the service coalesces concurrent GetLink calls per file and the
// resolved CDN URL is then served from the account cache.
func TestTorboxConcurrentPlaybackOpensCostOneRequestdl(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	var requestdlCalls atomic.Int64
	cdn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer cdn.Close()
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestdlCalls.Add(1)
		_, _ = fmt.Fprintf(w, `{"success":true,"data":%q}`, cdn.URL+"/movie")
	}))
	defer api.Close()
	tb, err := torbox.New(config.Debrid{Name: "torbox-test", Provider: "torbox", APIKey: "test", DownloadAPIKeys: []string{"test"}, RequestdlBudget: "6000/minute", TorboxBackoffMax: "20ms"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	tb.Host = api.URL
	clients := xsync.NewMap[string, debrid.Client]()
	clients.Store("torbox-test", tb)
	service := New(clients, nil, nil, nil, cdn.Client(), 0, zerolog.Nop())
	entry := &storage.Entry{InfoHash: "hash", ActiveProvider: "torbox-test", Files: map[string]*storage.File{"movie": {Name: "movie", Size: 4}}, Providers: map[string]*storage.ProviderEntry{"torbox-test": {ID: "1", Files: map[string]*storage.ProviderFile{"movie": {Id: "2", Link: "torbox://1/2"}}}}}

	const readers = 8
	var wg sync.WaitGroup
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if dl, err := service.GetLink(context.Background(), entry, "movie"); err != nil || dl.DownloadLink != cdn.URL+"/movie" {
				t.Errorf("link=%q err=%v", dl.DownloadLink, err)
			}
		}()
	}
	wg.Wait()
	if got := requestdlCalls.Load(); got != 1 {
		t.Fatalf("%d concurrent opens made %d requestdl calls, want 1", readers, got)
	}
	if stats := tb.RequestdlStats().(request.RequestdlStats); stats.RequestsPlayback != 1 || stats.RequestsBackground != 0 {
		t.Fatalf("budget charge = %+v, want exactly one playback admission", stats)
	}
}
