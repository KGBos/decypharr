# Upstream synchronization, October 2026

Merge upstream main `3a0e0e23de153c70b2d441e14a7828b29f6e8391` into fork master `71ece258b2a2728cbd06014a083d194d6b03a00f`. Existing commits remain intact. A future merge should preserve this merge ancestry rather than squash the upstream synchronization.

| Fork fix | Disposition | Result |
| --- | --- | --- |
| Completion reconciliation | Retained | Completed progress plus download-present compensates for stale finished flags. |
| Uncached file materialization | Adapted | Refreshed provider files hydrate queued entries; empty provider metadata preserves known values. |
| Create-array compatibility | Retained | Select matching hash and reject unrelated array entries. |
| CDN cache and expiry | Adapted | Context-aware provider/playback APIs retain CDN cache, expiry eviction and three-hour cap. |
| Backpressure and shared admission | Adapted | Provider lanes share throttle state; downloader local retry yields to shared 429 cooldown. |
| Submission mediation | Adapted | Cancellable calls, dedicated provider lanes and persistent admission/queue state. |
| Requestdl priorities | Adapted | Playback, background downloader and new repair service carry their request classes. |
| Redaction | Retained | Provider error codes and sanitized URLs replace sensitive response logging. |
| Slow-stream recovery | Adapted | Integrates with upstream session ownership, cancellation and rewind. |
| Cache-warm limits | Adapted | Process-wide budget and serial season packs; reads published immutable configuration snapshots. |
| Mount ownership | Adapted | Owned leases and delayed teardown protection retained alongside upstream panic/VFS handling. |
| Torrent deletion | Superseded | Upstream typed control payload and response decoding. |
| Torrent-list freshness/pagination | Superseded | Upstream equivalent fresh paginated list implementation. |
| Provider/cache-check errors | Fixed | HTTP/provider/null/decode failures remain errors, omit unknown verdicts and cannot populate negative cache; typed deterministic miss replaces string matching. |
| STRM action | Superseded | Upstream removes the old action and exports through the dedicated STRM reconciliation service. |
| Configuration and image publishing | Retained | Fork keys/defaults and release workflows remain; new Validate workflow does not publish. |

## Validation

On macOS with the repository-supported Go 1.26.5:

- `go test -timeout 3m ./...`: passed.
- `go test -race -timeout 3m ./internal/request ./pkg/debrid/account ./pkg/debrid/providers/torbox ./pkg/manager/link ./pkg/mount/dfs/backend/hanwen`: passed.
- `go vet ./...`: passed.
- New queued-entry hydration/metadata regression: passed.
- `npm ci --ignore-scripts` and `npm run minify-js`: passed; generated assets committed.
- Existing shared connection-draining regressions pass on Go 1.26.5. The host's Go 1.27.1 had failed these checks; the supported version is the validation baseline.

Regression coverage includes matching/nonmatching create arrays, stale completion flags, refreshed queued files, expired CDN links, transient failures, long and deadline-less 429s, shared admission, cancellation, slow streams, cache-warm limits, and delayed mount ownership cleanup.

The Linux Validate job runs the full suite, targeted race tests, vet, both binary builds and isolated FUSE lifecycle acceptance when `/dev/fuse` exists. A skipped real-FUSE test is a limitation, not live mount acceptance. No production action or deployment is included. No formal reviewer approval is claimed while the PR is draft.
