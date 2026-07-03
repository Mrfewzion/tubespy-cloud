-- TubeSpy Cloud DB schema.
-- Run once: turso db shell tubespy-cloud < cloud/schema.sql

CREATE TABLE IF NOT EXISTS cloud_channels (
    channel_id          TEXT PRIMARY KEY,
    name                TEXT NOT NULL,
    handle              TEXT,
    uploads_playlist_id TEXT,
    subscriber_count    INTEGER,
    added_at            INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS cloud_videos (
    video_id         TEXT PRIMARY KEY,
    channel_id       TEXT NOT NULL,
    title            TEXT,
    published_at     INTEGER,
    description      TEXT,
    tags             TEXT,
    category_id      INTEGER,
    duration_seconds INTEGER,
    thumbnail_url    TEXT,
    first_seen_at    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS cloud_snapshots (
    video_id      TEXT NOT NULL,
    fetched_at    INTEGER NOT NULL,
    view_count    INTEGER NOT NULL,
    like_count    INTEGER,
    comment_count INTEGER,
    PRIMARY KEY (video_id, fetched_at)
);

CREATE TABLE IF NOT EXISTS cloud_sync_state (
    key   TEXT PRIMARY KEY,
    value TEXT
);

INSERT OR IGNORE INTO cloud_sync_state (key, value) VALUES ('last_sync', '0');
