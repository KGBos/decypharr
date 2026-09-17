package torbox

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/request"
	"github.com/sirrobot01/decypharr/internal/utils"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
)

func TestSubmitMagnetIncludesSanitizedProviderError(t *testing.T) {
	config.SetConfigPath(t.TempDir())

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprint(w, `{"success":false,"error":"BAD_REQUEST","detail":"magnet:?xt=urn:btih:SECRET is invalid","data":null}`)
	}))
	t.Cleanup(server.Close)

	torrent := &debridTypes.Torrent{
		InfoHash: "AABBCC",
		Magnet:   &utils.Magnet{Link: "magnet:?xt=urn:btih:AABBCC"},
	}
	_, err := testTorbox(server.URL).SubmitMagnet(torrent)
	if err == nil {
		t.Fatal("SubmitMagnet() error = nil, want provider error")
	}
	message := err.Error()
	if !strings.Contains(message, "BAD_REQUEST") || !strings.Contains(message, "[redacted magnet]") {
		t.Fatalf("SubmitMagnet() error = %q, want sanitized provider fields", message)
	}
	if strings.Contains(message, "SECRET") || strings.Contains(message, "magnet:?") {
		t.Fatalf("SubmitMagnet() error leaked submitted magnet data: %q", message)
	}
}

func TestGetTorboxStatusKeepsIncompleteDownloadsRetryable(t *testing.T) {
	tb := &Torbox{}
	if got := tb.getTorboxStatus("incomplete", false); got != debridTypes.TorrentStatusDownloading {
		t.Fatalf("getTorboxStatus(incomplete) = %q, want downloading", got)
	}
}

func TestUpdateTorrentTreatsPresentStalledDownloadAsCompleted(t *testing.T) {
	config.SetConfigPath(t.TempDir())

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"success":true,"data":{"id":17,"name":"Reacher.S02.mkv","size":100,"progress":1,"download_state":"stalled (no seeds)","download_finished":false,"download_present":true,"created_at":"2026-01-02T03:04:05Z","hash":"5004BCC4598206C7C7293353936708BC8288C034","files":[{"id":1,"name":"Reacher.S02.mkv","absolute_path":"Reacher.S02.mkv","size":100}]}}`)
	}))
	t.Cleanup(server.Close)

	torrent := &debridTypes.Torrent{Id: "17"}
	if err := testTorbox(server.URL).UpdateTorrent(torrent); err != nil {
		t.Fatalf("UpdateTorrent() error = %v", err)
	}
	if torrent.Status != debridTypes.TorrentStatusDownloaded {
		t.Fatalf("Status = %q, want downloaded", torrent.Status)
	}
	file := torrent.Files["Reacher.S02.mkv"]
	if file.Link != "torbox://17/1" {
		t.Fatalf("file link = %q, want torbox://17/1", file.Link)
	}
}

func TestUpdateTorrentDoesNotCompletePartialPresentDownload(t *testing.T) {
	config.SetConfigPath(t.TempDir())

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"success":true,"data":{"id":18,"name":"Partial.Release.mkv","size":100,"progress":0.5,"download_state":"downloading","download_finished":false,"download_present":true,"created_at":"2026-01-02T03:04:05Z","hash":"DDEEFF","files":[{"id":1,"name":"Partial.Release.mkv","absolute_path":"Partial.Release.mkv","size":100}]}}`)
	}))
	t.Cleanup(server.Close)

	torrent := &debridTypes.Torrent{Id: "18"}
	if err := testTorbox(server.URL).UpdateTorrent(torrent); err != nil {
		t.Fatalf("UpdateTorrent() error = %v", err)
	}
	if torrent.Status != debridTypes.TorrentStatusDownloading {
		t.Fatalf("Status = %q, want downloading", torrent.Status)
	}
	if link := torrent.Files["Partial.Release.mkv"].Link; link != "" {
		t.Fatalf("file link = %q, want empty until progress reaches 100%%", link)
	}
}

func TestCheckStatusPreservesTorboxFailureContext(t *testing.T) {
	config.SetConfigPath(t.TempDir())

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"success":true,"data":{"id":23,"name":"Broken.Release.mkv","size":100,"progress":1,"download_state":"failed (processing)","download_finished":false,"download_present":false,"tracker_message":"storage unavailable","created_at":"2026-01-02T03:04:05Z","hash":"AABBCC","files":[]}}`)
	}))
	t.Cleanup(server.Close)

	torrent, err := testTorbox(server.URL).CheckStatus(&debridTypes.Torrent{Id: "23"})
	if err == nil {
		t.Fatal("CheckStatus() error = nil, want provider failure")
	}
	for _, want := range []string{"failed (processing)", "storage unavailable", "id 23", "hash AABBCC"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("CheckStatus() error = %q, want %q", err, want)
		}
	}
	if torrent == nil || torrent.InfoHash != "AABBCC" {
		t.Fatalf("CheckStatus() torrent = %#v, want returned provider hash", torrent)
	}
}

func TestGetTorrentsBypassesTorboxCache(t *testing.T) {
	config.SetConfigPath(t.TempDir())

	var (
		mu      sync.Mutex
		offsets []string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("bypass_cache"); got != "true" {
			t.Errorf("bypass_cache = %q, want true", got)
		}

		offset := r.URL.Query().Get("offset")
		mu.Lock()
		offsets = append(offsets, offset)
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		if offset == "0" {
			_, _ = fmt.Fprint(w, `{"success":true,"data":[{"id":17,"name":"Release.mkv","size":100,"progress":1,"download_state":"completed","download_finished":true,"created_at":"2026-01-02T03:04:05Z","hash":"ABC","files":[{"id":1,"name":"Release.mkv","absolute_path":"Release.mkv","size":100}]}]}`)
			return
		}
		_, _ = fmt.Fprint(w, `{"success":true,"data":[]}`)
	}))
	t.Cleanup(server.Close)

	tb := testTorbox(server.URL)
	torrents, err := tb.GetTorrents()
	if err != nil {
		t.Fatalf("GetTorrents() error = %v", err)
	}
	if len(torrents) != 1 || torrents[0].Id != "17" {
		t.Fatalf("GetTorrents() = %#v, want torrent 17", torrents)
	}

	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(offsets, []string{"0", "1"}) {
		t.Fatalf("offsets = %v, want [0 1]", offsets)
	}
}

func TestGetTorrentsReturnsPaginationErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("offset") == "0" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, `{"success":true,"data":[{"id":17,"name":"Release.mkv","created_at":"2026-01-02T03:04:05Z"}]}`)
			return
		}
		http.Error(w, "temporary failure", http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)

	tb := testTorbox(server.URL)
	torrents, err := tb.GetTorrents()
	if err == nil {
		t.Fatal("GetTorrents() error = nil, want pagination error")
	}
	if torrents != nil {
		t.Fatalf("GetTorrents() torrents = %#v, want nil after pagination error", torrents)
	}
	if got := err.Error(); !strings.Contains(got, "get TorBox torrents at offset 1:") {
		t.Fatalf("GetTorrents() error = %q, want offset context", got)
	}
}

func testTorbox(host string) *Torbox {
	return &Torbox{
		Host:   host,
		client: request.New(request.WithMaxRetries(0)),
		logger: zerolog.Nop(),
		config: config.Debrid{Name: "torbox"},
	}
}

func TestSubmitMagnetSkipsUncachedWithoutSubmitting(t *testing.T) {
	config.SetConfigPath(t.TempDir())

	var createCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "checkcached"):
			_, _ = fmt.Fprint(w, `{"success":true,"data":{}}`)
		case strings.Contains(r.URL.Path, "createtorrent"):
			createCalls++
			_, _ = fmt.Fprint(w, `{"success":true,"data":{"torrent_id":1,"hash":"AABBCC"}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	torrent := &debridTypes.Torrent{
		Name:     "Uncached.Release",
		InfoHash: "AABBCC",
		Magnet:   &utils.Magnet{Link: "magnet:?xt=urn:btih:AABBCC"},
	}
	_, err := testTorbox(server.URL).SubmitMagnet(torrent)
	if err == nil || !strings.Contains(err.Error(), "not cached") {
		t.Fatalf("SubmitMagnet() error = %v, want not-cached skip", err)
	}
	if createCalls != 0 {
		t.Fatalf("createtorrent called %d times, want 0", createCalls)
	}
}

func TestSubmitMagnetProceedsWhenCached(t *testing.T) {
	config.SetConfigPath(t.TempDir())

	var createCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "checkcached"):
			_, _ = fmt.Fprint(w, `{"success":true,"data":{"AABBCC":{"name":"Cached.Release","size":100,"hash":"AABBCC"}}}`)
		case strings.Contains(r.URL.Path, "createtorrent"):
			createCalls++
			_, _ = fmt.Fprint(w, `{"success":true,"data":{"torrent_id":42,"hash":"AABBCC"}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	torrent := &debridTypes.Torrent{
		Name:     "Cached.Release",
		InfoHash: "AABBCC",
		Magnet:   &utils.Magnet{Link: "magnet:?xt=urn:btih:AABBCC"},
	}
	got, err := testTorbox(server.URL).SubmitMagnet(torrent)
	if err != nil {
		t.Fatalf("SubmitMagnet() error = %v", err)
	}
	if got.Id != "42" || createCalls != 1 {
		t.Fatalf("id = %q, createCalls = %d; want 42 and 1", got.Id, createCalls)
	}
}
