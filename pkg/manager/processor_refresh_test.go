package manager

import (
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"testing"
)

func TestQueuedEntryRefreshMaterializesFilesAndPreservesMetadata(t *testing.T) {
	entry := &storage.Entry{InfoHash: "abc", Name: "original", OriginalFilename: "original.torrent", Size: 42, Bytes: 42}
	refreshed := &debridTypes.Torrent{InfoHash: "abc", Debrid: "torbox", Files: map[string]debridTypes.File{"movie.mkv": {Name: "movie.mkv", Size: 123}}}
	applyDebridTorrentToEntry(entry, refreshed)
	if entry.Files["movie.mkv"] == nil || entry.Files["movie.mkv"].Size != 123 {
		t.Fatal("refreshed files missing")
	}
	if entry.Name != "original" || entry.OriginalFilename != "original.torrent" || entry.Size != 42 {
		t.Fatal("empty provider metadata replaced known metadata")
	}
}
