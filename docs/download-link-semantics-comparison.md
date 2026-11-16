# Download-link semantics: measured comparison (liteflix #953, PR #26 decision point)

Reproduce (no production calls, fake TorBox only):

```
git checkout compare/download-link-semantics
go test -tags compare ./pkg/debrid/providers/torbox -run TestDownloadLinkSemanticsComparison -count=1 -v
# optional: COMPARE_OUT=/tmp/report.md writes the raw tables below
```

Harness: `pkg/debrid/providers/torbox/zz_semantics_compare_test.go` (build tag `compare`, so normal test runs and CI never execute it). Environment: Go 1.27.4 darwin/arm64, stack head `76022a9`. Config per cell: fresh provider and budget `6000/minute`, `TorboxBackoffMax=50ms`, `RequestdlFreezeMax=2s`, placeholder lifetime (`autoExpiresLinksAfter`) 48h so the 3h CDN cap is visible.

## Candidates (what was actually compared)

- **A** = the stack as pushed. `fetchDownloadLink` builds a redirect placeholder with no network call. `GetDownloadLinkForPlayback` resolves the CDN URL once through the budget and caches it for min(3h, configured).
- **B** = upstream `8162e05`'s resolve block, unchanged, grafted onto the stack's `fetchDownloadLink`: `requestdl?redirect=false` on every lookup, falling back to the redirect placeholder on any failure. Same account-manager cache, same throttle/budget, same `doGetWithClient`. It is upstream's function inside the fork's stack, **not a pristine v2.7.2 binary** (v2.7.2 has no budget or lanes, so lane counts would be N/A). This is what the stack becomes if upstream's behavior is adopted, and equals the pre-override auto-merge result.

## Summary (all MEASURED unless marked EXPECTED)

| Question | A (stack) | B (upstream block) |
| --- | --- | --- |
| Probe/repair lookup of one file | 0 requestdl, 0 admissions | 1 requestdl, 1 **probe-lane** admission |
| Repair sweep of 8 files, then 1 playback open | 1 requestdl (playback lane) | 8 requestdl (all probe lane); the playback open is then free |
| Is B's resolution counted by the budget? | n/a | **Yes**: admitted in the caller's lane (probe or playback) |
| CDN URL lifetime stored | min(3h, configured) | configured placeholder lifetime (48h here); no 3h cap |
| 429 + Retry-After on playback | error `TorBox HTTP 429 backpressure: retry after ~1s`, no URL, 1 wire call; a second call 50ms later is blocked locally (still 1 wire call) | no error; redirect placeholder cached for the full lifetime; 1 wire call |
| Timeout (caller deadline 1.5s) | error `context deadline exceeded`; each new call retries (wire 1, then 2) and is admitted again | waits the full 1.5s, then no error and the placeholder is cached; a **probe also waits** (A's probe returns in 0s) |
| Malformed body / empty data / 403 / 503 / `success:false` cap in a 200 | error each call, nothing cached; call 2 retries (wire 2), after recovery wire 3 | no error, placeholder cached; **never retried**: wire stays 1 |
| After the provider recovers (call 3, 1.3s later) | serves the CDN URL (3h) | **still the redirect placeholder**, wire unchanged (stuck until the placeholder expires or a validation/refetch drops it) |
| Fallback to the redirect URL when resolution fails | **Not supported**: returns the error, caches nothing | **Supported** for every failure kind above |
| 8 concurrent playback opens, provider layer | 8 requestdl, 8 admissions | 8 requestdl, 8 admissions (same; coalescing is the link service's `GetLink` singleflight) |
| Probe then playback on one file | probe free; playback pays 1 (playback lane) | probe pays 1 (probe lane); playback free (cached CDN URL) |
| Playback then probe on one file | probe free (reuses the CDN URL) | probe free (reuses the CDN URL) |

## Correction to earlier statements

The PR #26 description, the commit 5 message and the #953 comment said upstream's behavior "bypasses the fork's requestdl budget". **That was a code-reading inference and the measurement contradicts it**: B's calls are admitted and counted in the caller's lane. The real differences are who pays and when (probes now spend admissions), what happens on failure (silent cached placeholder vs error plus retry), and the CDN lifetime. The commit 5 message is left as pushed to avoid rewriting reviewed history; this document and the PR description supersede it.

## Not measured / EXPECTED only

- Stream-time behavior of the redirect placeholder in B. Upstream's own code comment says streaming the redirect URL makes every range read a requestdl call against the account-wide limit. This harness stops at link resolution, so the per-read cost, and how the budget and a 429 freeze would treat those reads, are **expected from code, not measured**. (The existing `TestTorboxPlaybackRangesUseCDNAfterOneRequestdl` measures the A path only.)
- Real TorBox behavior: actual CDN URL lifetimes, real 429 frequencies, whether the placeholder redirect itself succeeds. No production calls were made; real-world impact is **not measured**.
- Whether a cached placeholder in B is dropped sooner in production by the link service's validation rules (#434/#439 refetch on 400/403/410). Provider-level cache only; the link service was not in this harness.
- Pure upstream v2.7.2 behavior (no budget) as a third candidate.

## Raw results

Each cell: fresh provider + fresh budget (6000/minute) + fresh fake TorBox. Call 1 then call 2 (50ms later) on the same file, same class; in every failure scenario the fake then recovers (switches to success) and call 3 is made 1.3s later, after any Retry-After freeze. `wire` counts requests the fake server received (all on /api/torrents/requestdl); `rd=false` are JSON-resolution requests, `rd=true` are redirect-style requests. Lane counts are the budget's cumulative admissions.

## success

| candidate / entry | call | result |
| --- | --- | --- |
| A probe | 1 | redirect placeholder | err=- | exp=48h0m0s | wire=0 (rd=false:0, rd=true:0) | lanes pb/bg/probe=0/0/0 | 0s |
| A probe | 2 | redirect placeholder | err=- | exp=48h0m0s | wire=0 (rd=false:0, rd=true:0) | lanes pb/bg/probe=0/0/0 | 0s |
| A playback | 1 | CDN url | err=- | exp=3h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 20ms |
| A playback | 2 | CDN url | err=- | exp=3h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 0s |
| B probe | 1 | CDN url | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=0/0/1 | 20ms |
| B probe | 2 | CDN url | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=0/0/1 | 0s |
| B playback | 1 | CDN url | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 20ms |
| B playback | 2 | CDN url | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 0s |

## 429 + Retry-After:1 (always)

| candidate / entry | call | result |
| --- | --- | --- |
| A probe | 1 | redirect placeholder | err=- | exp=48h0m0s | wire=0 (rd=false:0, rd=true:0) | lanes pb/bg/probe=0/0/0 | 0s |
| A probe | 2 | redirect placeholder | err=- | exp=48h0m0s | wire=0 (rd=false:0, rd=true:0) | lanes pb/bg/probe=0/0/0 | 0s |
| A probe | 3 (provider recovered) | redirect placeholder | err=- | exp=48h0m0s | wire=0 (rd=false:0, rd=true:0) | lanes pb/bg/probe=0/0/0 | 0s |
| A playback | 1 | none | err=TorBox HTTP 429 backpressure: retry after 986ms | exp=- | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 20ms |
| A playback | 2 | none | err=Get "FAKE/api/torrents/requestdl": TorBox HTTP 429 backpressure: retry after 934ms | exp=- | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 0s |
| A playback | 3 (provider recovered) | CDN url | err=- | exp=3h0m0s | wire=2 (rd=false:2, rd=true:0) | lanes pb/bg/probe=2/0/0 | 20ms |
| B probe | 1 | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=0/0/1 | 20ms |
| B probe | 2 | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=0/0/1 | 0s |
| B probe | 3 (provider recovered) | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=0/0/1 | 0s |
| B playback | 1 | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 20ms |
| B playback | 2 | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 0s |
| B playback | 3 (provider recovered) | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 0s |

## 429 once, then success

| candidate / entry | call | result |
| --- | --- | --- |
| A probe | 1 | redirect placeholder | err=- | exp=48h0m0s | wire=0 (rd=false:0, rd=true:0) | lanes pb/bg/probe=0/0/0 | 0s |
| A probe | 2 | redirect placeholder | err=- | exp=48h0m0s | wire=0 (rd=false:0, rd=true:0) | lanes pb/bg/probe=0/0/0 | 0s |
| A probe | 3 (provider recovered) | redirect placeholder | err=- | exp=48h0m0s | wire=0 (rd=false:0, rd=true:0) | lanes pb/bg/probe=0/0/0 | 0s |
| A playback | 1 | none | err=TorBox HTTP 429 backpressure: retry after 991ms | exp=- | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 20ms |
| A playback | 2 | none | err=Get "FAKE/api/torrents/requestdl": TorBox HTTP 429 backpressure: retry after 938ms | exp=- | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 0s |
| A playback | 3 (provider recovered) | CDN url | err=- | exp=3h0m0s | wire=2 (rd=false:2, rd=true:0) | lanes pb/bg/probe=2/0/0 | 20ms |
| B probe | 1 | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=0/0/1 | 20ms |
| B probe | 2 | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=0/0/1 | 0s |
| B probe | 3 (provider recovered) | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=0/0/1 | 0s |
| B playback | 1 | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 20ms |
| B playback | 2 | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 0s |
| B playback | 3 (provider recovered) | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 0s |

## timeout (server hangs 8s, caller deadline 1.5s)

| candidate / entry | call | result |
| --- | --- | --- |
| A probe | 1 | redirect placeholder | err=- | exp=48h0m0s | wire=0 (rd=false:0, rd=true:0) | lanes pb/bg/probe=0/0/0 | 0s |
| A probe | 2 | redirect placeholder | err=- | exp=48h0m0s | wire=0 (rd=false:0, rd=true:0) | lanes pb/bg/probe=0/0/0 | 0s |
| A probe | 3 (provider recovered) | redirect placeholder | err=- | exp=48h0m0s | wire=0 (rd=false:0, rd=true:0) | lanes pb/bg/probe=0/0/0 | 0s |
| A playback | 1 | none | err=Get "FAKE/api/torrents/requestdl": context deadline exceeded | exp=- | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 1.52s |
| A playback | 2 | none | err=Get "FAKE/api/torrents/requestdl": context deadline exceeded | exp=- | wire=2 (rd=false:2, rd=true:0) | lanes pb/bg/probe=2/0/0 | 1.51s |
| A playback | 3 (provider recovered) | CDN url | err=- | exp=3h0m0s | wire=3 (rd=false:3, rd=true:0) | lanes pb/bg/probe=3/0/0 | 10ms |
| B probe | 1 | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=0/0/1 | 1.51s |
| B probe | 2 | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=0/0/1 | 0s |
| B probe | 3 (provider recovered) | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=0/0/1 | 0s |
| B playback | 1 | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 1.51s |
| B playback | 2 | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 0s |
| B playback | 3 (provider recovered) | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 0s |

## malformed body (not JSON)

| candidate / entry | call | result |
| --- | --- | --- |
| A probe | 1 | redirect placeholder | err=- | exp=48h0m0s | wire=0 (rd=false:0, rd=true:0) | lanes pb/bg/probe=0/0/0 | 0s |
| A probe | 2 | redirect placeholder | err=- | exp=48h0m0s | wire=0 (rd=false:0, rd=true:0) | lanes pb/bg/probe=0/0/0 | 0s |
| A probe | 3 (provider recovered) | redirect placeholder | err=- | exp=48h0m0s | wire=0 (rd=false:0, rd=true:0) | lanes pb/bg/probe=0/0/0 | 0s |
| A playback | 1 | none | err=torbox requestdl response: invalid character '<' looking for beginning of value | exp=- | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 40ms |
| A playback | 2 | none | err=torbox requestdl response: invalid character '<' looking for beginning of value | exp=- | wire=2 (rd=false:2, rd=true:0) | lanes pb/bg/probe=2/0/0 | 130ms |
| A playback | 3 (provider recovered) | CDN url | err=- | exp=3h0m0s | wire=3 (rd=false:3, rd=true:0) | lanes pb/bg/probe=3/0/0 | 20ms |
| B probe | 1 | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=0/0/1 | 10ms |
| B probe | 2 | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=0/0/1 | 0s |
| B probe | 3 (provider recovered) | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=0/0/1 | 0s |
| B playback | 1 | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 10ms |
| B playback | 2 | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 0s |
| B playback | 3 (provider recovered) | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 0s |

## empty data (200, success:true, data:"")

| candidate / entry | call | result |
| --- | --- | --- |
| A probe | 1 | redirect placeholder | err=- | exp=48h0m0s | wire=0 (rd=false:0, rd=true:0) | lanes pb/bg/probe=0/0/0 | 0s |
| A probe | 2 | redirect placeholder | err=- | exp=48h0m0s | wire=0 (rd=false:0, rd=true:0) | lanes pb/bg/probe=0/0/0 | 0s |
| A probe | 3 (provider recovered) | redirect placeholder | err=- | exp=48h0m0s | wire=0 (rd=false:0, rd=true:0) | lanes pb/bg/probe=0/0/0 | 0s |
| A playback | 1 | none | err=torbox requestdl failed: HTTP 200 provider_error_code=UNKNOWN | exp=- | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 10ms |
| A playback | 2 | none | err=torbox requestdl failed: HTTP 200 provider_error_code=UNKNOWN | exp=- | wire=2 (rd=false:2, rd=true:0) | lanes pb/bg/probe=2/0/0 | 150ms |
| A playback | 3 (provider recovered) | CDN url | err=- | exp=3h0m0s | wire=3 (rd=false:3, rd=true:0) | lanes pb/bg/probe=3/0/0 | 20ms |
| B probe | 1 | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=0/0/1 | 10ms |
| B probe | 2 | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=0/0/1 | 0s |
| B probe | 3 (provider recovered) | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=0/0/1 | 0s |
| B playback | 1 | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 10ms |
| B playback | 2 | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 0s |
| B playback | 3 (provider recovered) | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 0s |

## redirect-resolution failure: 403

| candidate / entry | call | result |
| --- | --- | --- |
| A probe | 1 | redirect placeholder | err=- | exp=48h0m0s | wire=0 (rd=false:0, rd=true:0) | lanes pb/bg/probe=0/0/0 | 0s |
| A probe | 2 | redirect placeholder | err=- | exp=48h0m0s | wire=0 (rd=false:0, rd=true:0) | lanes pb/bg/probe=0/0/0 | 0s |
| A probe | 3 (provider recovered) | redirect placeholder | err=- | exp=48h0m0s | wire=0 (rd=false:0, rd=true:0) | lanes pb/bg/probe=0/0/0 | 0s |
| A playback | 1 | none | err=torbox requestdl failed: HTTP 403 provider_error_code=UNKNOWN | exp=- | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 20ms |
| A playback | 2 | none | err=torbox requestdl failed: HTTP 403 provider_error_code=UNKNOWN | exp=- | wire=2 (rd=false:2, rd=true:0) | lanes pb/bg/probe=2/0/0 | 150ms |
| A playback | 3 (provider recovered) | CDN url | err=- | exp=3h0m0s | wire=3 (rd=false:3, rd=true:0) | lanes pb/bg/probe=3/0/0 | 20ms |
| B probe | 1 | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=0/0/1 | 20ms |
| B probe | 2 | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=0/0/1 | 0s |
| B probe | 3 (provider recovered) | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=0/0/1 | 0s |
| B playback | 1 | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 20ms |
| B playback | 2 | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 0s |
| B playback | 3 (provider recovered) | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 0s |

## redirect-resolution failure: 503

| candidate / entry | call | result |
| --- | --- | --- |
| A probe | 1 | redirect placeholder | err=- | exp=48h0m0s | wire=0 (rd=false:0, rd=true:0) | lanes pb/bg/probe=0/0/0 | 0s |
| A probe | 2 | redirect placeholder | err=- | exp=48h0m0s | wire=0 (rd=false:0, rd=true:0) | lanes pb/bg/probe=0/0/0 | 0s |
| A probe | 3 (provider recovered) | redirect placeholder | err=- | exp=48h0m0s | wire=0 (rd=false:0, rd=true:0) | lanes pb/bg/probe=0/0/0 | 0s |
| A playback | 1 | none | err=torbox requestdl failed: HTTP 503 provider_error_code=UNKNOWN | exp=- | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 10ms |
| A playback | 2 | none | err=torbox requestdl failed: HTTP 503 provider_error_code=UNKNOWN | exp=- | wire=2 (rd=false:2, rd=true:0) | lanes pb/bg/probe=2/0/0 | 150ms |
| A playback | 3 (provider recovered) | CDN url | err=- | exp=3h0m0s | wire=3 (rd=false:3, rd=true:0) | lanes pb/bg/probe=3/0/0 | 20ms |
| B probe | 1 | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=0/0/1 | 10ms |
| B probe | 2 | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=0/0/1 | 0s |
| B probe | 3 (provider recovered) | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=0/0/1 | 0s |
| B playback | 1 | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 10ms |
| B playback | 2 | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 0s |
| B playback | 3 (provider recovered) | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 0s |

## provider cap in a 200 (success:false)

| candidate / entry | call | result |
| --- | --- | --- |
| A probe | 1 | redirect placeholder | err=- | exp=48h0m0s | wire=0 (rd=false:0, rd=true:0) | lanes pb/bg/probe=0/0/0 | 0s |
| A probe | 2 | redirect placeholder | err=- | exp=48h0m0s | wire=0 (rd=false:0, rd=true:0) | lanes pb/bg/probe=0/0/0 | 0s |
| A probe | 3 (provider recovered) | redirect placeholder | err=- | exp=48h0m0s | wire=0 (rd=false:0, rd=true:0) | lanes pb/bg/probe=0/0/0 | 0s |
| A playback | 1 | none | err=torbox requestdl failed: HTTP 200 provider_error_code=DAILY_BANDWIDTH_LIMIT_EXCEEDED | exp=- | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 20ms |
| A playback | 2 | none | err=torbox requestdl failed: HTTP 200 provider_error_code=DAILY_BANDWIDTH_LIMIT_EXCEEDED | exp=- | wire=2 (rd=false:2, rd=true:0) | lanes pb/bg/probe=2/0/0 | 150ms |
| A playback | 3 (provider recovered) | CDN url | err=- | exp=3h0m0s | wire=3 (rd=false:3, rd=true:0) | lanes pb/bg/probe=3/0/0 | 20ms |
| B probe | 1 | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=0/0/1 | 20ms |
| B probe | 2 | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=0/0/1 | 0s |
| B probe | 3 (provider recovered) | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=0/0/1 | 0s |
| B playback | 1 | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 20ms |
| B playback | 2 | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 0s |
| B playback | 3 (provider recovered) | redirect placeholder | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 0s |

## probe then playback on the same file (success)

| candidate | step | result |
| --- | --- | --- |
| A | probe | redirect placeholder | err=- | exp=48h0m0s | wire=0 (rd=false:0, rd=true:0) | lanes pb/bg/probe=0/0/0 | 0s |
| A | probe | redirect placeholder | err=- | exp=48h0m0s | wire=0 (rd=false:0, rd=true:0) | lanes pb/bg/probe=0/0/0 | 0s |
| A | playback | CDN url | err=- | exp=3h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 20ms |
| A | playback | CDN url | err=- | exp=3h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 0s |
| B | probe | CDN url | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=0/0/1 | 10ms |
| B | probe | CDN url | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=0/0/1 | 0s |
| B | playback | CDN url | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=0/0/1 | 0s |
| B | playback | CDN url | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=0/0/1 | 0s |

## playback then probe on the same file (success)

| candidate | step | result |
| --- | --- | --- |
| A | playback | CDN url | err=- | exp=3h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 20ms |
| A | probe | CDN url | err=- | exp=3h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 0s |
| B | playback | CDN url | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 20ms |
| B | probe | CDN url | err=- | exp=48h0m0s | wire=1 (rd=false:1, rd=true:0) | lanes pb/bg/probe=1/0/0 | 0s |

## repair sweep: 8 probe lookups of 8 different files, then 1 playback open of file 1 (success)

| candidate | wire | rd=false | lanes pb/bg/probe |
| --- | --- | --- | --- |
| A | 1 | 1 | 1/0/0 |
| B | 8 | 8 | 0/0/8 |

## 8 concurrent playback opens of one file, provider layer only (success)

| candidate | wire | lanes pb/bg/probe |
| --- | --- | --- |
| A | 8 | 8/0/0 |
| B | 8 | 8/0/0 |
