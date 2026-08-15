"""Zero-maintenance YouTube stats collector for R2-hosted cloud.db.

Run via GitHub Actions every 2 hours:
  1. Apply schema to the downloaded cloud.db (sqlite file, stdlib driver)
  2. Merge app sidecars (channels.json, catalog.json) — INSERT OR IGNORE
  3. Walk each channel's uploads playlist (newest only, stop at known)
  4. Supplement with RSS (catches API gaps)
  5. Snapshot all known videos' stats (videos.list, batched 50)
  6. Compact old snapshots (14-day tiered retention)
  7. Update last_sync

The workflow uploads the file back to R2 atomically (temp key + copy).
"""

import calendar
import json
import os
import re
import sqlite3
import sys
import time
import logging
from pathlib import Path

import httpx
import feedparser

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("sync")

# ── Config from environment ──────────────────────────────────────────
YOUTUBE_KEY = os.environ["YOUTUBE_API_KEY"]
CLOUD_DB_PATH = Path(os.getenv("CLOUD_DB_PATH", "cloud.db"))
SCHEMA_PATH = Path(os.getenv("SCHEMA_PATH", str(Path(__file__).parent / "schema.sql")))

# ── HTTP tuning (env-overridable) ────────────────────────────────────
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT_SECONDS", "30"))
HTTP_RETRIES = int(os.getenv("HTTP_RETRIES", "5"))
HTTP_BACKOFF_BASE = float(os.getenv("HTTP_BACKOFF_BASE", "1"))
HTTP_MAX_BACKOFF = float(os.getenv("HTTP_MAX_BACKOFF", "30"))

RSS_URL = "https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"

# Walk at most 2 pages (50 each) per channel per run — catches ~100 latest uploads
MAX_PLAYLIST_PAGES = 2


# ── SQLite ────────────────────────────────────────────────────────────
def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _apply_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_PATH.read_text("utf-8"))
    conn.commit()


def _merge_sidecars(conn: sqlite3.Connection) -> dict:
    """Merge app-uploaded channels.json / catalog.json into the cloud DB.

    Returns {"channels": N, "videos": N, "snapshots": N} merged.
    """
    counts = {"channels": 0, "videos": 0, "snapshots": 0}

    channels_path = Path("channels.json")
    if channels_path.exists():
        try:
            channels = json.loads(channels_path.read_text("utf-8"))
            rows = [
                (c["channel_id"], c.get("name", ""), c.get("handle"),
                 c.get("uploads_playlist_id"), int(c.get("added_at", 0)))
                for c in channels
            ]
            conn.executemany(
                "INSERT OR IGNORE INTO cloud_channels"
                " (channel_id, name, handle, uploads_playlist_id, added_at)"
                " VALUES (?, ?, ?, ?, ?)",
                rows,
            )
            conn.commit()
            counts["channels"] = len(rows)
            log.info("Merged %d channels from channels.json", len(rows))
        except Exception:
            log.warning("Failed to merge channels.json", exc_info=True)

    catalog_path = Path("catalog.json")
    if catalog_path.exists():
        try:
            catalog = json.loads(catalog_path.read_text("utf-8"))
            video_rows = []
            snap_rows = []
            for entry in catalog:
                cid = entry["channel_id"]
                for v in entry.get("videos", []):
                    # videos list: video_id, channel_id, title, description, tags,
                    # category_id, published_at, duration_seconds, thumbnail_url,
                    # first_seen_at
                    video_rows.append(
                        (v[0], cid, v[2], v[3], v[4], v[5], v[6], v[7], v[8], v[9])
                    )
                for s in entry.get("snapshots", []):
                    # snapshots list: video_id, fetched_at, view_count,
                    # like_count, comment_count
                    snap_rows.append(tuple(s))
            if video_rows:
                conn.executemany(
                    "INSERT OR IGNORE INTO cloud_videos (video_id, channel_id, title,"
                    " description, tags, category_id, published_at, duration_seconds,"
                    " thumbnail_url, first_seen_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    video_rows,
                )
                counts["videos"] = len(video_rows)
            if snap_rows:
                conn.executemany(
                    "INSERT OR IGNORE INTO cloud_snapshots (video_id, fetched_at,"
                    " view_count, like_count, comment_count) VALUES (?, ?, ?, ?, ?)",
                    snap_rows,
                )
                counts["snapshots"] = len(snap_rows)
            conn.commit()
            log.info(
                "Merged %d videos, %d snapshots from catalog.json",
                counts["videos"], counts["snapshots"],
            )
        except Exception:
            log.warning("Failed to merge catalog.json", exc_info=True)

    return counts


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
def _request_with_retries(client, method, url, **kwargs):
    """Send an httpx request, retrying timeouts/transport errors with backoff."""
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            return client.request(method, url, **kwargs)
        except (httpx.TimeoutException, httpx.TransportError) as e:
            if attempt >= HTTP_RETRIES:
                log.error("Request %s %s failed after %d attempts: %s", method, url, HTTP_RETRIES, e)
                raise
            backoff = min(HTTP_BACKOFF_BASE * (2 ** (attempt - 1)), HTTP_MAX_BACKOFF)
            log.warning(
                "Request %s %s timed out (attempt %d/%d): %s — retrying in %.1fs",
                method, url, attempt, HTTP_RETRIES, e, backoff,
            )
            time.sleep(backoff)


def _http():
    return httpx.Client(http2=True, timeout=HTTP_TIMEOUT)


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
        resp = _request_with_retries(
            http, "GET", "https://www.googleapis.com/youtube/v3/playlistItems",
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
        resp = _request_with_retries(http, "GET", RSS_URL.format(channel_id=channel_id), follow_redirects=True)
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
        resp = _request_with_retries(
            http, "GET", "https://www.googleapis.com/youtube/v3/videos",
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
    log.info("Collector starting (cloud.db=%s)", CLOUD_DB_PATH)

    http = _http()
    conn = _connect(CLOUD_DB_PATH)

    try:
        # 0. Schema + app sidecars
        _apply_schema(conn)
        merged = _merge_sidecars(conn)

        # 1. Load tracked channels
        rows = conn.execute(
            "SELECT channel_id, name, handle, uploads_playlist_id FROM cloud_channels"
        ).fetchall()
        channels = [
            {"channel_id": r[0], "name": r[1], "handle": r[2], "uploads_playlist_id": r[3]}
            for r in rows
        ]
        log.info("Loaded %d tracked channels", len(channels))

        # 2. Load known video IDs (for stop-at-known during discovery)
        known_ids = {r[0] for r in conn.execute("SELECT video_id FROM cloud_videos")}
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

        # 4. Insert new videos (batched)
        now_ts = int(time.time())
        if discovered_pairs:
            conn.executemany(
                "INSERT OR IGNORE INTO cloud_videos (video_id, channel_id, first_seen_at)"
                " VALUES (?, ?, ?)",
                [(vid, cid, now_ts) for vid, cid in discovered_pairs],
            )
            conn.commit()
            log.info("Inserted %d new video records", len(discovered_pairs))

        # 4b. Load published_at for newly discovered videos from playlistItems
        if discovered_pairs:
            for i in range(0, len(discovered_pairs), 50):
                batch = [v for v, _ in discovered_pairs[i:i+50]]
                resp = _request_with_retries(
                    http, "GET", "https://www.googleapis.com/youtube/v3/videos",
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
                pub_stmts = []
                for item in resp.json().get("items", []):
                    pub = item.get("snippet", {}).get("publishedAt")
                    if pub:
                        pub_ts = int(calendar.timegm(time.strptime(pub.replace("Z", "").replace("z", ""), "%Y-%m-%dT%H:%M:%S")))
                        pub_stmts.append(
                            (pub_ts, item["id"])
                        )
                if pub_stmts:
                    conn.executemany(
                        "UPDATE cloud_videos SET published_at = ?"
                        " WHERE video_id = ? AND published_at IS NULL",
                        pub_stmts,
                    )
                    conn.commit()

        # 5. Snapshot all known videos
        all_video_ids = [r[0] for r in conn.execute("SELECT video_id FROM cloud_videos")]
        log.info("Snapshotting %d videos", len(all_video_ids))

        snapshots = snapshot_videos(http, all_video_ids)
        log.info("Got %d snapshot records", len(snapshots))

        # 5b. Update video metadata from snapshot response (batched)
        meta_stmts = [
            (
                s["title"], s["description"], s["tags"], s["category_id"],
                s["duration_seconds"], s["published_at"], s["thumbnail_url"],
                s["video_id"],
            )
            for s in snapshots
            if s["title"]
        ]
        if meta_stmts:
            conn.executemany(
                "UPDATE cloud_videos SET title = ?, description = ?, tags = ?,"
                " category_id = ?, duration_seconds = ?,"
                " published_at = COALESCE(published_at, ?), thumbnail_url = ?"
                " WHERE video_id = ?",
                meta_stmts,
            )
            conn.commit()

        # 6. Insert snapshots (batched)
        snap_stmts = [
            (s["video_id"], s["fetched_at"], s["view_count"], s["like_count"], s["comment_count"])
            for s in snapshots
        ]
        if snap_stmts:
            conn.executemany(
                "INSERT OR IGNORE INTO cloud_snapshots (video_id, fetched_at,"
                " view_count, like_count, comment_count) VALUES (?, ?, ?, ?, ?)",
                snap_stmts,
            )
            conn.commit()
        log.info("Inserted %d snapshots", len(snapshots))

        # 6b. Compact old snapshots (same 14-day tiered retention as local)
        cutoff = now_ts - 14 * 86400
        conn.execute(
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
        conn.commit()
        log.info("Retention compacted cloud_snapshots older than %d", cutoff)

        # 7. Update last_sync
        conn.execute(
            "UPDATE cloud_sync_state SET value = ? WHERE key = 'last_sync'",
            (str(now_ts),),
        )
        conn.commit()
        log.info("Updated last_sync = %d", now_ts)

        # 8. Integrity check before the workflow uploads the file
        check = conn.execute("PRAGMA integrity_check").fetchone()[0]
        if check != "ok":
            raise RuntimeError(f"integrity_check failed: {check}")
        log.info("integrity_check: ok")

        elapsed = time.perf_counter() - _t0
        log.info(
            "Collector done in %.2fs — %d new videos, %d snapshots (merged: %d channels, %d videos, %d snapshots)",
            elapsed, len(discovered_pairs), len(snapshots),
            merged["channels"], merged["videos"], merged["snapshots"],
        )

    except Exception:
        log.exception("Collector failed")
        sys.exit(1)
    finally:
        conn.close()
        http.close()


if __name__ == "__main__":
    main()