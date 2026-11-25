//go:build linux || (darwin && amd64)

package hanwen

import (
	"context"
	"os"
	"strconv"
	"strings"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/sirrobot01/decypharr/internal/request"
)

// probeCommPrefixes lists process names whose mount reads are library work,
// not a viewer pressing play. Jellyfin's library scan runs ffprobe over every
// file it re-extracts, and a full scan issues hundreds of them; those reads
// must not share the interactive lane with a viewer (incident #1027).
//
// The match is on the executable name (comm), so "ffprobe" and the
// "ffprobe.real" form used by the timeout shim are both probes. Jellyfin
// itself and its transcoding ffmpeg stay playback: a direct play or a
// transcode start is exactly what must not queue behind a scan.
var probeCommPrefixes = []string{"ffprobe"}

// callerComm returns the executable name of pid. Reads /proc/<pid>/comm on
// Linux; it is a variable so tests can stub the lookup.
var callerComm = readComm

func readComm(pid uint32) (string, error) {
	data, err := os.ReadFile("/proc/" + strconv.FormatUint(uint64(pid), 10) + "/comm")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

// classForComm maps an executable name to its /requestdl lane.
func classForComm(comm string) request.Class {
	comm = strings.ToLower(strings.TrimSpace(comm))
	for _, prefix := range probeCommPrefixes {
		if comm == prefix || strings.HasPrefix(comm, prefix+".") {
			return request.ClassProbe
		}
	}
	return request.ClassPlayback
}

// readClass returns the lane a FUSE read belongs to, from the process that
// issued it. go-fuse puts the caller in the request context; when it is
// missing (kernel readahead, an internal call) or /proc is unreadable, the
// read keeps the playback lane so an unknown caller is never deprioritised.
func readClass(ctx context.Context) request.Class {
	caller, ok := fuse.FromContext(ctx)
	if !ok || caller == nil {
		return request.ClassPlayback
	}
	comm, err := callerComm(caller.Pid)
	if err != nil {
		return request.ClassPlayback
	}
	return classForComm(comm)
}
