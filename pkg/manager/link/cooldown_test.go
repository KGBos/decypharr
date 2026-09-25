package link

import (
	"errors"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
)

func newCooldownTestService() *Service {
	return New(
		xsync.NewMap[string, debrid.Client](),
		nil, nil, nil,
		&http.Client{Timeout: 2 * time.Second},
		1,
		zerolog.Nop(),
	)
}

func TestNoteSlowHostCoolsDown(t *testing.T) {
	s := newCooldownTestService()
	if s.hostCooling("cdn1.torbox.app") {
		t.Fatal("unknown host should not be cooling")
	}
	s.NoteSlowHost("cdn1.torbox.app", 22*1024)
	if !s.hostCooling("cdn1.torbox.app") {
		t.Fatal("host should be cooling after NoteSlowHost")
	}
	if s.hostCooling("cdn2.torbox.app") {
		t.Fatal("other hosts must not be affected")
	}
	s.NoteSlowHost("", 22*1024) // empty host is a no-op, must not panic
}

func TestHostCoolingExpires(t *testing.T) {
	s := newCooldownTestService()
	s.cooldowns.Store("stale.torbox.app", time.Now().Add(-time.Minute))
	if s.hostCooling("stale.torbox.app") {
		t.Fatal("expired cooldown should report not cooling")
	}
	if _, ok := s.cooldowns.Load("stale.torbox.app"); ok {
		t.Fatal("expired entry should be reaped on read")
	}
}

func TestCooldownMapBounded(t *testing.T) {
	s := newCooldownTestService()
	// Pre-fill with expired entries: sweep must reclaim space instead of
	// dropping the new note.
	for i := 0; i < maxCooldownHosts; i++ {
		s.cooldowns.Store("stale-host-"+strconv.Itoa(i), time.Now().Add(-time.Hour))
	}
	s.NoteSlowHost("fresh.torbox.app", 1024)
	if !s.hostCooling("fresh.torbox.app") {
		t.Fatal("sweep should have reclaimed space for the fresh entry")
	}
}

func TestCDNHost(t *testing.T) {
	cases := []struct {
		url  string
		want string
	}{
		{"https://cdn12.torbox.app/dl/abc123/video.mkv", "cdn12.torbox.app"},
		{"https://cdn12.torbox.app:8443/dl/x?token=secret", "cdn12.torbox.app:8443"},
		{"http://127.0.0.1:8080/file", "127.0.0.1:8080"},
		{"", ""},
		{":://not a url", ""},
	}
	for _, c := range cases {
		if got := CDNHost(types.DownloadLink{DownloadLink: c.url}); got != c.want {
			t.Errorf("CDNHost(%q) = %q, want %q", c.url, got, c.want)
		}
	}
}

func TestSlowStreamErrorTaxonomy(t *testing.T) {
	serr := NewSlowStreamError("cdn1.torbox.app", 1<<20, 22*1024)
	var as *SlowStreamError
	if !errors.As(serr, &as) {
		t.Fatal("should unwrap to *SlowStreamError")
	}
	if as.Host != "cdn1.torbox.app" || as.Bps != 22*1024 || as.Bytes != 1<<20 {
		t.Fatalf("measurements not carried: %+v", as)
	}
	lerr := GetLinkError(serr)
	if lerr == nil {
		t.Fatal("GetLinkError should find the embedded link error")
	}
	if !lerr.ShouldRefetch() {
		t.Error("slow stream must classify as refetchable")
	}
	if lerr.IsPermanent() {
		t.Error("slow stream must not be permanent")
	}
	if lerr.ShouldDisableAccount() {
		t.Error("slow stream must not disable the account")
	}
}
