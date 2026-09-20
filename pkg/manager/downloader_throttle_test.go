package manager

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/request"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/debrid/providers/torbox"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/manager/link"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

func TestLocalDownloadSharesProviderGate(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	var calls atomic.Int64
	var fail atomic.Bool
	fail.Store(true)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method == "GET" && fail.Load() {
			w.Header().Set("Retry-After", "120")
			w.WriteHeader(429)
			return
		}
		w.Header().Set("Content-Length", "4")
		w.WriteHeader(200)
		if r.Method == "GET" {
			_, _ = w.Write([]byte("data"))
		}
	}))
	defer server.Close()
	tb, err := torbox.New(config.Debrid{Name: "torbox", Provider: "torbox", APIKey: "test", DownloadAPIKeys: []string{"test"}, TorboxBackoffMax: "20ms", TorboxBreakerThreshold: 1, TorboxBreakerCooldown: "20ms"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	clients := xsync.NewMap[string, debrid.Client]()
	clients.Store("torbox", tb)
	d := &Downloader{manager: &Manager{ctx: context.Background(), clients: clients, streamClient: server.Client()}, logger: zerolog.Nop()}
	dl := types.DownloadLink{Debrid: "torbox", DownloadLink: server.URL}
	destination := filepath.Join(t.TempDir(), "file")
	err = d.localDownloader(dl, destination, nil, nil)
	if request.BackpressureError(err) == nil {
		t.Fatalf("local GET did not report throttle: %v", err)
	}
	before := calls.Load()
	err = d.localDownloader(dl, destination, nil, nil)
	if request.BackpressureError(err) == nil || calls.Load() != before {
		t.Fatalf("local download bypassed open gate: %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	fail.Store(false)
	if err = d.localDownloader(dl, destination, nil, nil); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(destination)
	if err != nil || string(data) != "data" {
		t.Fatalf("local recovery: %q %v", data, err)
	}
}

func TestLocalLinkResolutionFailsFastOnThrottle(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(429)
	}))
	defer server.Close()
	tb, err := torbox.New(config.Debrid{Name: "torbox", Provider: "torbox", APIKey: "test", DownloadAPIKeys: []string{"test"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	tb.Host = server.URL
	clients := xsync.NewMap[string, debrid.Client]()
	clients.Store("torbox", tb)
	d := &Downloader{manager: &Manager{linkService: link.New(clients, nil, nil, nil, server.Client(), 3, zerolog.Nop())}, logger: zerolog.Nop()}
	entry := &storage.Entry{InfoHash: "hash", ActiveProvider: "torbox", Files: map[string]*storage.File{"file": {Name: "file", Size: 4}}, Providers: map[string]*storage.ProviderEntry{"torbox": {ID: "1", Files: map[string]*storage.ProviderFile{"file": {Id: "2", Link: "torbox://1/2"}}}}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	_, err = d.resolveLinkWithRetry(ctx, entry, "file")
	if request.BackpressureError(err) == nil || calls.Load() != 1 || time.Since(start) > 500*time.Millisecond {
		t.Fatalf("local link retry ladder: %v calls=%d", err, calls.Load())
	}
}
