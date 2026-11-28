package torbox

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
)

func TestFailedAvailabilityNeverCachesMissOrFallsBack(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	for _, mediated := range []bool{false, true} {
		for _, tc := range []struct {
			name   string
			status int
			body   string
		}{
			{"http failure", 502, `{"success":false,"error":"DOWNLOAD_NOT_CACHED","data":null}`},
			{"provider failure", 200, `{"success":false,"error":"DOWNLOAD_NOT_CACHED","data":{}}`},
			{"missing data", 200, `{"success":true,"data":null}`},
			{"invalid response", 200, `not json`},
		} {
			t.Run(fmt.Sprintf("mediated=%t/%s", mediated, tc.name), func(t *testing.T) {
				var calls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					w.WriteHeader(tc.status)
					fmt.Fprint(w, tc.body)
				}))
				defer server.Close()
				tb := testTorbox(server.URL)
				if mediated {
					var err error
					tb.mediator, err = NewSubmissionMediator(tb, config.Debrid{TorboxQueueJournalPath: filepath.Join(t.TempDir(), "queue.jsonl")}, tb.getNegativeCache(), zerolog.Nop())
					if err != nil {
						t.Fatal(err)
					}
				}
				for range 2 {
					result, err := tb.IsAvailable([]string{"AbC"})
					if err == nil {
						t.Fatal("failed check reported success")
					}
					if _, ok := result["AbC"]; ok {
						t.Fatal("failed check reported an unavailable verdict")
					}
					if _, ok := tb.getNegativeCache().Get("abc"); ok {
						t.Fatal("failed check populated negative cache")
					}
				}
				if got := calls.Load(); got != 2 {
					t.Fatalf("wire calls=%d; want two attempts without fallback", got)
				}
			})
		}
	}
}

func TestFailedSubmissionPreflightNeverCachesMiss(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	for _, body := range []string{`{"success":false,"error":"DOWNLOAD_NOT_CACHED","data":null}`, `{"success":true,"data":null}`} {
		t.Run(body, func(t *testing.T) {
			var checks, creates atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/torrents/createtorrent" {
					creates.Add(1)
					t.Error("create after failed preflight")
				}
				checks.Add(1)
				fmt.Fprint(w, body)
			}))
			defer server.Close()
			tb := testTorbox(server.URL)
			var err error
			tb.mediator, err = NewSubmissionMediator(tb, config.Debrid{TorboxQueueJournalPath: filepath.Join(t.TempDir(), "queue.jsonl")}, tb.getNegativeCache(), zerolog.Nop())
			if err != nil {
				t.Fatal(err)
			}
			for range 2 {
				_, err := tb.SubmitMagnet(&types.Torrent{InfoHash: "AbC", Magnet: &utils.Magnet{Link: "magnet:?xt=urn:btih:AbC"}})
				if err == nil || errors.Is(err, customerror.TorrentNotCachedError) {
					t.Fatalf("failure misclassified as miss: %v", err)
				}
				if _, ok := tb.getNegativeCache().Get("abc"); ok {
					t.Fatal("failed preflight populated negative cache")
				}
			}
			if checks.Load() != 2 || creates.Load() != 0 {
				t.Fatalf("checks/creates=%d/%d", checks.Load(), creates.Load())
			}
		})
	}
}

func TestMediatorCancelsProviderRequest(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		close(started)
		<-r.Context().Done()
	}))
	defer server.Close()
	tb := testTorbox(server.URL)
	mediator, err := NewSubmissionMediator(tb, config.Debrid{TorboxQueueJournalPath: filepath.Join(t.TempDir(), "queue.jsonl")}, tb.getNegativeCache(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := mediator.Submit(ctx, &types.Torrent{InfoHash: "AbC", DownloadUncached: true, Magnet: &utils.Magnet{Link: "magnet:?xt=urn:btih:AbC"}})
		result <- err
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("provider request did not start")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation lost: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("provider request ignored cancellation")
	}
	if mediator.queue.Len() != 0 {
		t.Fatal("cancelled submission left active queue record")
	}
}
