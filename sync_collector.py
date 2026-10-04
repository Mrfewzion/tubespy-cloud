"""Zero-maintenance YouTube stats collector for R2-hosted cloud.db.

Run via GitHub Actions every 2 hours:
  1. Apply schema to the downloaded cloud.db (sqlite file, stdlib driver)
  2. Merge app sidecars (channels.json, catalog.json) — INSERT, with a
     title-NULL-only backfill UPSERT so RSS-discovered rows gain titles
  3. Cloud-focus mode: snapshot ONLY videos whose title matches a keyword from
     snapshot_targets.json (app-uploaded, newest-first, capped at FOCUS_CAP)
  4. RSS discovery (free) supplements known videos in focus mode
  5. Snapshot matched videos' stats (videos.list, batched 50)
  6. Compact old snapshots (14-day tiered retention)
  7. Update last_sync

With NO keywords (or a missing/empty snapshot_targets.json) the collector is
fully idle: schema + sidecar merges only, zero YouTube calls.

The workflow uploads the file back to R2 atomically (temp key + copy).
"""

import calendar
import datetime as dt
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
#
# Read with .get(), NEVER os.environ[...]. A module-level subscript makes the
# whole module un-importable without a full set of credentials, and that is not a
# theoretical concern: the workflow's freshness-guard step imports this module
# for `guard_main` and deliberately carries no YouTube key, so run 37203657581
# (2026-10-04) died in 15 s with `KeyError: 'YOUTUBE_API_KEY'` raised at import.
# Requiring a credential to *import* also makes the module unusable from any tool,
# test or future step that is not a full collection.
#
# The key is validated where it is actually used — see _require_youtube_key() —
# so a missing key is still a loud, immediate failure rather than an opaque 403
# from the Data API partway through a run.
YOUTUBE_KEY = os.environ.get("YOUTUBE_API_KEY", "")
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

# Cloud-focus mode: when snapshot_targets.json lists keywords, ONLY videos whose
# title matches are snapshotted (newest-first, capped). No keywords = fully idle.
# The JSON "cap" wins; env FOCUS_CAP is the fallback for old payloads. 0 = no cap.
FOCUS_CAP = int(os.getenv("FOCUS_CAP", "41600"))
SNAPSHOT_TARGETS_PATH = Path(os.getenv("SNAPSHOT_TARGETS_PATH", "snapshot_targets.json"))


# ── Freshness guard (workflow `schedule` events only) ─────────────────
#
# The workflow asks GitHub for a wake-up far more often than a collection is
# actually due, and this decides whether a given wake-up is due. Asking often is
# only affordable because a too-soon wake-up costs one HEAD request instead of a
# ~50 MB download, a YouTube quota spend, and an upload.
#
# The asymmetry IS the contract: skip only on positive proof that cloud.db is
# already fresh. A missing object, a permissions error, or unparseable output all
# collect, because the cost of a needless collection is quota we can afford and
# the cost of a wrongly skipped one is silently stale data nobody can see.
#
# The decision lives here, not in the workflow YAML, so it is unit-testable.
# The previous version put it in shell (`${LAST:+...}`), where the one bug it
# ever had — printing the raw epoch glued onto the age, "age=288 min1791020025"
# — could only ever be checked by simulating it by hand. See OpenCodeLog #129
# and #270.

FRESHNESS_GUARD_SECONDS = int(os.getenv("FRESHNESS_GUARD_SECONDS", "7200"))


def should_collect(age_seconds: float | None, threshold: int | None = None) -> bool:
    """Whether a wake-up should do a full collection.

    `age_seconds` is how old cloud.db is, or None when that could not be
    determined. None always collects — fail open, never on absence of evidence.

    A NEGATIVE age is treated as unknown for the same reason: it means the
    runner's clock is behind the object's LastModified, which is not evidence
    that the data is fresh. Only a positive, readable age can justify a skip.
    """
    if age_seconds is None:
        return True
    limit = FRESHNESS_GUARD_SECONDS if threshold is None else threshold
    try:
        age = float(age_seconds)
    except (TypeError, ValueError):
        return True
    if age < 0:
        return True
    return age >= limit


def parse_last_modified(raw: str | None) -> float | None:
    """Epoch seconds from `aws s3api head-object --output text`, else None.

    Tolerant by necessity: a missing key, a denied request, or a failed step all
    arrive here as empty or garbage text, and "None" is a real observed stdout.
    Every unparseable form must map to None so the caller fails open.
    """
    if raw is None:
        return None
    text = raw.strip().strip("'\"")
    if not text or text.lower() in ("none", "null", "-"):
        return None
    try:
        return float(text)
    except ValueError:
        pass
    try:
        return dt.datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def guard_main(
    last_modified: str | None,
    github_env_path: str,
    now: float | None = None,
    event: str = "schedule",
) -> int:
    """Decide, append SKIP to $GITHUB_ENV, and always return 0.

    Returning 0 unconditionally is deliberate: the guard must never be the
    reason a run goes red. Writing the flag matters just as much — every
    downstream step is gated on `env.SKIP != 'true'`, so failing to write it
    silently turns the guard into a no-op that still reads as protection.

    `event` makes the DECISION event-aware while the workflow always runs this
    step. A manual dispatch never skips: it is the only way a user can force a
    refresh after a bad window, and gating the whole step on the event instead
    would have meant no manual run could ever verify this code.
    """
    stamp = parse_last_modified(last_modified)
    if event != "schedule":
        # Not a scheduled wake-up: report the age for the log, never skip.
        age_desc = (
            "%.0f min" % ((time.time() if now is None else now) - stamp)
            if stamp is not None else "unknown"
        )
        log.info(
            "Freshness guard: %s event, cloud.db age %s -> always collect",
            event, age_desc,
        )
        skip = False
    elif stamp is None:
        log.info("Freshness guard: cloud.db age unknown -> collecting")
        skip = False
    else:
        age = (time.time() if now is None else now) - stamp
        skip = not should_collect(age)
        log.info(
            "Freshness guard: cloud.db is %.0f min old (threshold %d min) -> %s",
            age / 60,
            FRESHNESS_GUARD_SECONDS // 60,
            "skip" if skip else "collect",
        )
    try:
        with open(github_env_path, "a", encoding="utf-8") as fh:
            fh.write(f"SKIP={'true' if skip else 'false'}\n")
    except OSError:
        # Unset SKIP satisfies `env.SKIP != 'true'`, so the steps still collect.
        log.warning("Freshness guard: could not write %s", github_env_path, exc_info=True)
    return 0


# ── Cloud focus (keyword-driven targets) ─────────────────────────────
def escape_like(pattern: str) -> str:
    """Escape LIKE wildcards so a keyword matches literally, not as a pattern."""
    return (
        pattern.replace("\\", "\\\\")
        .replace("%", "\\%")
        .replace("_", "\\_")
    )


def load_focus() -> tuple[list[str], int]:
    """(Keywords, cap) from snapshot_targets.json (app-uploaded).

    Tolerant: missing/empty/malformed files mean idle mode (empty keywords).
    Cap: JSON value when a non-negative int, else env FOCUS_CAP, else 41600.
    0 means no cap (snapshot ALL matches).
    """
    if not SNAPSHOT_TARGETS_PATH.exists():
        return [], FOCUS_CAP
    try:
        data = json.loads(SNAPSHOT_TARGETS_PATH.read_text("utf-8"))
        keywords = data.get("keywords", [])
        raw_cap = data.get("cap", FOCUS_CAP)
    except (json.JSONDecodeError, OSError, AttributeError):
        log.warning("snapshot_targets.json unreadable — collector idle")
        return [], FOCUS_CAP
    cleaned: list[str] = []
    for kw in keywords:
        if not isinstance(kw, str):
            continue
        word = kw.strip().lower()
        if word and word not in cleaned:
            cleaned.append(word)
    cap = FOCUS_CAP
    if isinstance(raw_cap, int) and not isinstance(raw_cap, bool) and raw_cap >= 0:
        cap = raw_cap
    else:
        log.warning("snapshot_targets.json cap invalid (%r) — using env default %d", raw_cap, FOCUS_CAP)
    return cleaned, cap


def focus_targets(conn: sqlite3.Connection, keywords: list[str], cap: int | None = FOCUS_CAP) -> list[str]:
    """Video IDs whose title matches any keyword (case-insensitive, LIKE-escaped).

    Deduped across keywords, newest-first (NULL published_at last). A positive
    `cap` limits the run so quota stays bounded; None/0 means no limit.
    """
    if not keywords:
        return []
    where = " OR ".join(["lower(title) LIKE ? ESCAPE '\\'"] * len(keywords))
    patterns = [f"%{escape_like(k)}%" for k in keywords]
    limit = "" if cap is None or cap <= 0 else " LIMIT ?"
    params = (*patterns, cap) if limit else tuple(patterns)
    rows = conn.execute(
        f"SELECT DISTINCT video_id FROM cloud_videos WHERE {where}"
        " ORDER BY published_at IS NULL, published_at DESC" + limit,
        params,
    ).fetchall()
    return [r[0] for r in rows]


# ── SQLite ────────────────────────────────────────────────────────────
def _connect(db_path: Path) -> sqlite3.Connection:
    # Same PRAGMA set as the app (Build Spec §4). Python's implicit 5 s
    # busy_timeout and synchronous=FULL both bite here: this is a long-running
    # writer over a WAL database that can overlap the app's read of an uploaded
    # copy, and without a busy_timeout that surfaces as "database is locked"
    # instead of waiting.
    #
    # These two PRAGMAs were lost in 0b5cfda (2026-08-15, Turso -> R2) and went
    # unnoticed for a month because TubeSpy's test for this invariant read a
    # stale in-repo COPY of this file rather than the file that actually runs.
    # Do not remove them; tests/test_cloud_sync.py
    # ::test_the_cloud_collector_writer_uses_the_app_pragma_set asserts them.
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
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
                # UPSERT: backfill metadata ONLY on rows whose title is NULL
                # (RSS-discovered rows carry no title, so they could never match
                # focus keywords otherwise). Rows with a title are left untouched
                # (no rewrite churn); first_seen_at is preserved on conflict.
                conn.executemany(
                    "INSERT INTO cloud_videos (video_id, channel_id, title,"
                    " description, tags, category_id, published_at, duration_seconds,"
                    " thumbnail_url, first_seen_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    " ON CONFLICT(video_id) DO UPDATE SET"
                    " title = excluded.title,"
                    " description = excluded.description,"
                    " tags = excluded.tags,"
                    " category_id = excluded.category_id,"
                    " published_at = excluded.published_at,"
                    " duration_seconds = excluded.duration_seconds,"
                    " thumbnail_url = excluded.thumbnail_url"
                    " WHERE cloud_videos.title IS NULL",
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
    """Fallback RSS discovery (0 quota, latest ~15).

    Returns list of dicts: {video_id, title, published_at}. Titles come
    straight from the feed so focus keywords can match brand-new uploads
    immediately — a title-less row can never match a keyword, and nothing
    backfills it while the app is closed (catalog.json only covers newly
    added channels)."""
    try:
        resp = _request_with_retries(http, "GET", RSS_URL.format(channel_id=channel_id), follow_redirects=True)
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)
        out = []
        for entry in feed.entries:
            vid = entry.get("yt_videoid")
            if not vid:
                link = entry.get("link", "")
                if "watch?v=" in link:
                    vid = link.split("watch?v=", 1)[1].split("&", 1)[0]
            if not vid:
                continue
            published_at = None
            if entry.get("published_parsed"):
                published_at = calendar.timegm(entry.published_parsed)
            out.append({
                "video_id": vid,
                "title": entry.get("title"),
                "published_at": published_at,
            })
        return out
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
                "tags": json.dumps(snip.get("tags")) if snip.get("tags") else None,
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
def _require_youtube_key() -> None:
    """Exit non-zero, loudly, if there is no API key.

    The module deliberately reads YOUTUBE_API_KEY with .get() so it can be
    imported without credentials. That must not turn a missing key into a
    confusing 403 from the Data API several minutes into a run, so the check
    happens here instead — at the point of use, before any network call.
    """
    if not YOUTUBE_KEY.strip():
        log.error(
            "YOUTUBE_API_KEY is not set — cannot collect. Set it in the "
            "workflow env (it is only present on the 'Run collector' step, "
            "which is correct: the freshness guard does not need it)."
        )
        sys.exit(2)


def main():
    _require_youtube_key()
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

        # 2. Cloud-focus mode?
        keywords, focus_cap = load_focus()
        now_ts = int(time.time())
        discovered_pairs = []  # list of (video_id, channel_id, title, published_at)
        snapshots = []

        if not keywords:
            # Idle mode: no keywords configured — zero YouTube calls this run.
            log.info("No cloud-focus keywords — collector idle (schema + sidecar merges only)")
        else:
            log.info("Cloud focus active: %s (cap %s)", keywords, focus_cap or "unlimited")

            # 3a. RSS discovery only (free, 0 quota) — catches newest uploads so a
            #     later keyword match can include them. Playlist walk is skipped:
            #     its quota cost is exactly what focus mode exists to avoid.
            #     Feed entries carry titles + published_at, so every upsert below
            #     backfills title-less rows (previous RSS inserts) in the same pass.
            for ch in channels:
                refs = discover_rss(http, ch["channel_id"])
                for ref in refs:
                    discovered_pairs.append(
                        (ref["video_id"], ch["channel_id"], ref["title"], ref["published_at"])
                    )
            log.info("RSS discovery returned %d feed entries", len(discovered_pairs))

            # 3b. Upsert RSS-discovered videos (batched). Backfills titles ONLY on
            #     rows whose title is NULL (same pattern as the catalog merge), so
            #     orphaned title-less rows from earlier runs get fixed here; rows
            #     that already carry metadata are left untouched.
            if discovered_pairs:
                conn.executemany(
                    "INSERT INTO cloud_videos (video_id, channel_id, title,"
                    " published_at, first_seen_at) VALUES (?, ?, ?, ?, ?)"
                    " ON CONFLICT(video_id) DO UPDATE SET"
                    " title = excluded.title,"
                    " published_at = COALESCE(excluded.published_at, cloud_videos.published_at)"
                    " WHERE cloud_videos.title IS NULL",
                    [(vid, cid, title, published_at, now_ts) for vid, cid, title, published_at in discovered_pairs],
                )
                conn.commit()
                log.info("Upserted %d RSS-discovered video records (titles backfilled)", len(discovered_pairs))

            # 4. Snapshot ONLY focus-matched videos (newest-first, capped)
            target_ids = focus_targets(conn, keywords, focus_cap)
            log.info("Focus matched %d videos (cap %s) — snapshotting", len(target_ids), focus_cap or "unlimited")
            snapshots = snapshot_videos(http, target_ids)
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
            "Collector done in %.2fs — %d RSS entries upserted, %d snapshots (merged: %d channels, %d videos, %d snapshots)",
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