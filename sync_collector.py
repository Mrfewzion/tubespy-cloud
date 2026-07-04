"""Zero-maintenance YouTube stats collector for Turso.

Run via GitHub Actions every 2 hours:
  1. Read tracked channels from Turso
  2. Walk each channel's uploads playlist (newest only, stop at known)
  3. Supplement with RSS (catches API gaps)
  4. Snapshot all known videos' stats (videos.list, batched 50)
  5. Store results in Turso
"""

import calendar
import os
import re
import sys
import time
import logging

import httpx
import feedparser

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("sync")

# ── Config from environment ──────────────────────────────────────────
TURSO_URL = os.environ["TURSO_DB_URL"]
TURSO_TOKEN = os.environ["TURSO_AUTH_TOKEN"]
YOUTUBE_KEY = os.environ["YOUTUBE_API_KEY"]

RSS_URL = "https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"

# Walk at most 2 pages (50 each) per channel per run — catches ~100 latest uploads
MAX_PLAYLIST_PAGES = 2

# ── Turso HTTP client (replaces libsql-client / Hrana WebSocket) ──────
def _turso_http_url(libsql_url: str) -> str:
    return libsql_url.replace("libsql://", "https://", 1)


def _typed(value):
    """Convert a Python value to a Turso typed arg."""
    if value is None:
        return {"type": "null"}
    if isinstance(value, bool):
        return {"type": "integer", "value": "1" if value else "0"}
    if isinstance(value, int):
        return {"type": "integer", "value": str(value)}
    if isinstance(value, float):
        return {"type": "float", "value": str(value)}
    return {"type": "text", "value": str(value)}


def _untyped(value: dict):
    """Convert a Turso typed value back to a Python value."""
    t = value["type"]
    if t == "null":
        return None
    if t == "integer":
        return int(value["value"])
    if t == "float":
        return float(value["value"])
    return value["value"]


class _TursoRow:
    """Minimal tuple-like row wrapper for backward compat with r[0] access."""

    def __init__(self, values: list):
        self._values = values

    def __getitem__(self, i):
        return self._values[i]

    def __iter__(self):
        return iter(self._values)

    def __len__(self):
        return len(self._values)


class _TursoClient:
    """Wraps Turso HTTP /v2/pipeline to look like libsql_client's execute()."""

    def __init__(self, url: str, token: str):
        self._http = httpx.Client(http2=True, timeout=15.0)
        self._base = _turso_http_url(url) + "/v2/pipeline"
        self._http.headers["Authorization"] = f"Bearer {token}"

    def execute(self, sql: str, args: tuple | list | None = None):
        req = {"type": "execute", "stmt": {"sql": sql}}
        if args is not None:
            req["stmt"]["args"] = [_typed(a) for a in args]
        resp = self._http.post(self._base, json={"requests": [req]})
        resp.raise_for_status()
        data = resp.json()
        result = data["results"][0]
        if result["type"] == "error":
            msg = result.get("error", {}).get("message", str(result))
            raise RuntimeError(f"Turso error: {msg}")
        result = result["response"]["result"]
        cols = result["cols"]
        return [_TursoRow([_untyped(c) for c in row]) for row in result["rows"]]

    def close(self):
        self._http.close()


# ── Parsing helpers ─────────────────────────────────────────────────────
def _parse_duration(iso: str | None) -> int | None:
    """Parse ISO 8601 duration (PT1H30M45S) to seconds."""
    if not iso:
        return None
    m = re.match(r"^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$", iso)
    if not m:
        return None
    h, mn, s = [int(v) if v else 0 for v in m.groups()]
    return h * 3600 + mn * 60 + s


# ── HTTP ───────────────────────────────────────────────────────────────
def _http():
    return httpx.Client(http2=True, timeout=15.0)


# ── YouTube API helpers ───────────────────────────────────────────────
def discover_uploads(http, channel_id, uploads_playlist_id, known_ids):
    """Walk uploads playlist (newest-first), stop at first known ID."""
    ids = []
    page_token = None
    pages = 0
    while pages < MAX_PLAYLIST_PAGES:
        params = {
            "part": "contentDetails",
            "playlistId": uploads_playlist_id,
            "maxResults": 50,
            "key": YOUTUBE_KEY,
            "fields": "items(contentDetails/videoId),nextPageToken",
        }
        if page_token:
            params["pageToken"] = page_token
        resp = http.get(
            "https://www.googleapis.com/youtube/v3/playlistItems",
            params=params,
        )
        if resp.status_code == 403:
            data = resp.json()
            errors = data.get("error", {}).get("errors", [{}])
            if any(e.get("reason") in ("quotaExceeded", "dailyLimitExceeded") for e in errors):
                log.warning("  %s: quota exceeded — stopping discovery", channel_id)
                break
        resp.raise_for_status()
        data = resp.json()
        for item in data.get("items", []):
            vid = item.get("contentDetails", {}).get("videoId")
            if not vid:
                continue
            if vid in known_ids:
                log.info("  %s: stopped at known video %s (page %d, %d found)", channel_id, vid, pages, len(ids))
                return ids
            ids.append(vid)
        page_token = data.get("nextPageToken")
        pages += 1
        if not page_token:
            break
    log.info("  %s: walked %d pages, found %d uploads", channel_id, pages, len(ids))
    return ids


def discover_rss(http, channel_id):
    """Fallback RSS discovery (0 quota, latest ~15)."""
    try:
        resp = http.get(RSS_URL.format(channel_id=channel_id), follow_redirects=True)
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)
        ids = []
        for entry in feed.entries:
            vid = entry.get("yt_videoid")
            if not vid:
                link = entry.get("link", "")
                if "watch?v=" in link:
                    vid = link.split("watch?v=", 1)[1].split("&", 1)[0]
            if vid:
                ids.append(vid)
        return ids
    except Exception:
        log.warning("  %s: RSS fetch failed", channel_id, exc_info=True)
        return []


def snapshot_videos(http, video_ids):
    """Fetch all video metadata + stats, batched 50. Returns list of dicts."""
    results = []
    now = int(time.time())
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i : i + 50]
        resp = http.get(
            "https://www.googleapis.com/youtube/v3/videos",
            params={
                "part": "snippet,contentDetails,statistics",
                "id": ",".join(batch),
                "key": YOUTUBE_KEY,
                "fields": "items(id,snippet(title,description,tags,categoryId,publishedAt,thumbnails/default/url),contentDetails(duration),statistics(viewCount,likeCount,commentCount))",
                "maxResults": 50,
            },
        )
        if resp.status_code == 403:
            data = resp.json()
            errors = data.get("error", {}).get("errors", [{}])
            if any(e.get("reason") in ("quotaExceeded", "dailyLimitExceeded") for e in errors):
                log.warning("Quota exceeded during snapshot — stopping early")
                break
        resp.raise_for_status()
        data = resp.json()
        for item in data.get("items", []):
            snip = item.get("snippet", {})
            stats = item.get("statistics", {})
            cd = item.get("contentDetails", {})
            pub = snip.get("publishedAt")
            results.append({
                "video_id": item["id"],
                "title": snip.get("title", ""),
                "description": snip.get("description", "") or "",
                "tags": snip.get("tags"),
                "category_id": snip.get("categoryId"),
                "published_at": int(calendar.timegm(time.strptime(pub.replace("Z", "").replace("z", ""), "%Y-%m-%dT%H:%M:%S"))) if pub else None,
                "duration_seconds": _parse_duration(cd.get("duration")),
                "thumbnail_url": snip.get("thumbnails", {}).get("default", {}).get("url"),
                "fetched_at": now,
                "view_count": int(stats.get("viewCount", 0)),
                "like_count": int(stats.get("likeCount", 0)) if stats.get("likeCount") else None,
                "comment_count": int(stats.get("commentCount", 0)) if stats.get("commentCount") else None,
            })
    return results


# ── Main ──────────────────────────────────────────────────────────────
def main():
    _t0 = time.perf_counter()
    log.info("Collector starting")

    http = _http()
    db = _TursoClient(TURSO_URL, TURSO_TOKEN)

    try:
        # 1. Load tracked channels from Turso
        rows = db.execute("SELECT channel_id, name, handle, uploads_playlist_id FROM cloud_channels")
        channels = [{"channel_id": r[0], "name": r[1], "handle": r[2], "uploads_playlist_id": r[3]} for r in rows]
        log.info("Loaded %d tracked channels", len(channels))

        # 2. Load known video IDs (for stop-at-known during discovery)
        known_ids = {r[0] for r in db.execute("SELECT video_id FROM cloud_videos")}
        log.info("Known video IDs: %d", len(known_ids))

        # 3. Discover new uploads per channel — track (video_id, channel_id) pairs
        discovered_pairs = []  # list of (video_id, channel_id)
        for ch in channels:
            cid = ch["channel_id"]
            up = ch.get("uploads_playlist_id")
            if not up:
                log.info("  %s: no uploads_playlist_id, skipping", cid)
                continue

            new_ids = discover_uploads(http, cid, up, known_ids)
            if new_ids:
                log.info("  %s: API returned %d new uploads", cid, len(new_ids))
                for vid in new_ids:
                    discovered_pairs.append((vid, cid))

            # RSS supplement (catches videos the API playlist misses)
            rss_ids = discover_rss(http, cid)
            rss_extra = [v for v in rss_ids if v not in known_ids and v not in new_ids]
            if rss_extra:
                log.info("  %s: RSS supplied %d extra", cid, len(rss_extra))
                for vid in rss_extra:
                    discovered_pairs.append((vid, cid))

        # 4. Insert new videos into Turso
        now_ts = int(time.time())
        for vid, cid in discovered_pairs:
            db.execute(
                "INSERT OR IGNORE INTO cloud_videos (video_id, channel_id, first_seen_at) VALUES (?, ?, ?)",
                (vid, cid, now_ts),
            )
        if discovered_pairs:
            log.info("Inserted %d new video records", len(discovered_pairs))

        # 4b. Load published_at for newly discovered videos from playlistItems
        if discovered_pairs:
            for i in range(0, len(discovered_pairs), 50):
                batch = [v for v, _ in discovered_pairs[i:i+50]]
                resp = http.get(
                    "https://www.googleapis.com/youtube/v3/videos",
                    params={
                        "part": "snippet",
                        "id": ",".join(batch),
                        "key": YOUTUBE_KEY,
                        "fields": "items(id,snippet(publishedAt))",
                        "maxResults": 50,
                    },
                )
                if resp.status_code != 200:
                    continue
                for item in resp.json().get("items", []):
                    pub = item.get("snippet", {}).get("publishedAt")
                    if pub:
                        pub_ts = int(calendar.timegm(time.strptime(pub.replace("Z", "").replace("z", ""), "%Y-%m-%dT%H:%M:%S")))
                        db.execute(
                            "UPDATE cloud_videos SET published_at = ? WHERE video_id = ? AND published_at IS NULL",
                            (pub_ts, item["id"]),
                        )

        # 5. Snapshot all known videos
        all_video_ids = [r[0] for r in db.execute("SELECT video_id FROM cloud_videos")]
        log.info("Snapshotting %d videos", len(all_video_ids))

        snapshots = snapshot_videos(http, all_video_ids)
        log.info("Got %d snapshot records", len(snapshots))

        # 5b. Update video metadata from snapshot response
        for s in snapshots:
            if s["title"]:
                db.execute(
                    "UPDATE cloud_videos SET title = ?, description = ?, tags = ?, category_id = ?, duration_seconds = ?, published_at = COALESCE(published_at, ?), thumbnail_url = ? WHERE video_id = ?",
                    (s["title"], s["description"], s["tags"], s["category_id"], s["duration_seconds"], s["published_at"], s["thumbnail_url"], s["video_id"]),
                )

        # 6. Insert snapshots into Turso
        for s in snapshots:
            db.execute(
                "INSERT OR IGNORE INTO cloud_snapshots (video_id, fetched_at, view_count, like_count, comment_count) VALUES (?, ?, ?, ?, ?)",
                (s["video_id"], s["fetched_at"], s["view_count"], s["like_count"], s["comment_count"]),
            )
        log.info("Inserted %d snapshots", len(snapshots))

        # 6b. Compact old snapshots (same 14-day tiered retention as local)
        cutoff = now_ts - 14 * 86400
        db.execute(
            """
            DELETE FROM cloud_snapshots
            WHERE fetched_at < ?
              AND rowid NOT IN (
                SELECT rowid FROM (
                  SELECT rowid,
                         ROW_NUMBER() OVER (
                           PARTITION BY video_id, fetched_at / 86400
                           ORDER BY fetched_at DESC
                         ) AS rn
                  FROM cloud_snapshots
                  WHERE fetched_at < ?
                ) WHERE rn = 1
              )
            """,
            (cutoff, cutoff),
        )
        log.info("Retention compacted cloud_snapshots older than %d", cutoff)

        # 7. Update last_sync
        db.execute("UPDATE cloud_sync_state SET value = ? WHERE key = 'last_sync'", (str(now_ts),))
        log.info("Updated last_sync = %d", now_ts)

        elapsed = time.perf_counter() - _t0
        log.info("Collector done in %.2fs — %d new videos, %d snapshots", elapsed, len(discovered_pairs), len(snapshots))

    except Exception:
        log.exception("Collector failed")
        sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    main()
