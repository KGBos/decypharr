//go:build linux || (darwin && amd64)

package hanwen

import (
	"context"
	"errors"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/sirrobot01/decypharr/internal/request"
)

func TestClassForCommTagsProbesOnly(t *testing.T) {
	cases := []struct {
		comm string
		want request.Class
	}{
		{"ffprobe", request.ClassProbe},
		{"ffprobe.real", request.ClassProbe},
		{"FFPROBE", request.ClassProbe},
		{"ffprobe\n", request.ClassProbe},
		{"ffprobeish", request.ClassPlayback},
		{"jellyfin", request.ClassPlayback},
		{"ffmpeg", request.ClassPlayback},
		{"Infuse", request.ClassPlayback},
		{"", request.ClassPlayback},
	}
	for _, tc := range cases {
		if got := classForComm(tc.comm); got != tc.want {
			t.Errorf("classForComm(%q) = %v, want %v", tc.comm, got, tc.want)
		}
	}
}

func stubCallerComm(t *testing.T, comm string, err error) {
	t.Helper()
	previous := callerComm
	callerComm = func(uint32) (string, error) { return comm, err }
	t.Cleanup(func() { callerComm = previous })
}

func callerContext() context.Context {
	return fuse.NewContext(context.Background(), &fuse.Caller{Pid: 4242})
}

func TestReadClassUsesCallerComm(t *testing.T) {
	stubCallerComm(t, "ffprobe", nil)
	if got := readClass(callerContext()); got != request.ClassProbe {
		t.Fatalf("ffprobe read tagged %v, want %v", got, request.ClassProbe)
	}

	stubCallerComm(t, "jellyfin", nil)
	if got := readClass(callerContext()); got != request.ClassPlayback {
		t.Fatalf("jellyfin read tagged %v, want %v", got, request.ClassPlayback)
	}
}

func TestReadClassDefaultsToPlayback(t *testing.T) {
	// No caller in the context (kernel readahead and internal calls).
	stubCallerComm(t, "ffprobe", nil)
	if got := readClass(context.Background()); got != request.ClassPlayback {
		t.Fatalf("read without caller tagged %v, want %v", got, request.ClassPlayback)
	}

	// /proc unreadable (the caller exited, or a non-Linux build).
	stubCallerComm(t, "", errors.New("no such process"))
	if got := readClass(callerContext()); got != request.ClassPlayback {
		t.Fatalf("read with unreadable /proc tagged %v, want %v", got, request.ClassPlayback)
	}
}

func TestReadClassKeepsProbeThroughSessionAndLinkDefaults(t *testing.T) {
	// A probe tag must survive the layers that default to playback.
	stubCallerComm(t, "ffprobe", nil)
	ctx := request.WithClass(callerContext(), readClass(callerContext()))
	ctx = request.WithDefaultClass(ctx, request.ClassPlayback)
	ctx = request.WithDefaultClass(ctx, request.ClassPlayback)
	if got := request.ClassOf(ctx); got != request.ClassProbe {
		t.Fatalf("probe tag downgraded to %v by defaults", got)
	}
}
