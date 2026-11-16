---
title: Torbox Setup
description: Configure Torbox provider.
---

Torbox is a supported Debrid provider.

## Configuration

```json
{
  "debrids": [
    {
      "provider": "torbox",
      "name": "Torbox",
      "api_key": "YOUR_API_KEY"
    }
  ]
}
```

Get your API key from the Torbox dashboard.

All configuration options from [Real Debrid](./real-debrid/) apply (rate limits, workers, proxy, etc.).

See [Configuration Reference](../configuration/#debrid-providers) for full options.

## Cache-only torrent submissions

When uncached downloads are disabled, Decypharr checks the magnet's info hash
with TorBox's `GET /api/torrents/checkcached` before creating a torrent. An
uncached result returns `DOWNLOAD_NOT_CACHED` to the requesting Arr without a
`createtorrent` call. A cache-check failure returns a separate error, so it
cannot be mistaken for a confirmed miss. A confirmed hit still uses
`add_only_if_cached=true` when creating the torrent.

Explicitly enabled uncached downloads bypass this preflight. TorBox caches
cache-check results for up to an hour, so a newly cached torrent may take time
to become eligible. Availability may also change between the check and create.

See TorBox's [cache-check API](https://www.postman.com/torbox/torbox-api/documentation/b6l9hbv/main-api) and
[API rate limits](https://support.torbox.app/en/articles/13726368-api-rate-limits).
