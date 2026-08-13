#!/usr/bin/env python3
"""Import historical playback data into Jellyfin's PlaybackActivity table.

Reads JSON (array of objects) or CSV and inserts rows into
playback_reporting.db, resolving usernames to Jellyfin user UUIDs via
jellyfin.db first.

Usage:
  python3 scripts/import_history.py <input.json|input.csv> [options]

Required columns/keys (case-insensitive):
  username        — Jellyfin username (resolved to UUID via jellyfin.db)
  play_duration   — seconds watched (int or float)

Optional columns/keys (sensible defaults applied):
  date_created    — ISO 8601 or YYYY-MM-DD HH:MM:SS (default: now)
  item_name       — media title
  item_type       — e.g. Movie, Episode (default: 'Imported')
  item_id         — Jellyfin item UUID (default: '')
  playback_method — e.g. DirectStream, Transcode (default: 'Import')
  client_name     — client app name (default: 'Import')
  device_name     — device name (default: 'Import')
"""

import csv
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone

#Allow importing jellyfin db from the parent directory
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import jellyfin_db

PLAYBACK_DB_PATH = os.environ.get("PLAYBACK_DB_PATH", "")
JELLYFIN_DB_PATH = os.environ.get("JELLYFIN_DB_PATH", "")


def _load_rows(path: str) -> list[dict]:
    """Load rows from JSON (array of objects) or CSV."""
    ext = path.rsplit(".", 1)[-1].lower()
    if ext == "json":
        with open(path) as f:
            data = json.load(f)
        if not isinstance(data, list):
            raise ValueError("JSON input must be an array of objects.")
        return data
    elif ext == "csv":
        with open(path, newline="") as f:
            return list(csv.DictReader(f))
    else:
        #Try JSON as a fallback
        try:
            with open(path) as f:
                data = json.load(f)
            return data if isinstance(data, list) else [data]
        except json.JSONDecodeError:
            raise ValueError(f"Unsupported file type: .{ext}. Use .json or .csv.")


def _norm_key(row: dict, *candidates) -> str | None:
    """Case-insensitive key lookup with multiple candidate names."""
    lower_map = {k.lower(): v for k, v in row.items()}
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    return None


def _parse_date(val) -> str:
    """Accept ISO 8601 or common date strings. Return Jellyfin-style datetime."""
    if not val:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.0000000Z")
    s = str(val).strip()
    #Already ISO, pass through
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%dT%H:%M:%S.0000000Z")
    except ValueError:
        pass
    #Try common SQL style formats
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%Y-%m-%dT%H:%M:%S.0000000Z")
        except ValueError:
            continue
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.0000000Z")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    input_path = sys.argv[1]

    if not PLAYBACK_DB_PATH or not JELLYFIN_DB_PATH:
        print("ERROR: Set PLAYBACK_DB_PATH and JELLYFIN_DB_PATH env vars.")
        sys.exit(1)
    if not os.path.exists(PLAYBACK_DB_PATH):
        print(f"ERROR: playback_reporting.db not found at {PLAYBACK_DB_PATH}")
        sys.exit(1)

    rows = _load_rows(input_path)
    if not rows:
        print("No rows found in input file.")
        sys.exit(0)

    #Pre resolve all usernames to UUIDs in one pass
    raw_names = {_norm_key(r, "username", "user", "user_name") for r in rows}
    raw_names.discard(None)
    uid_cache: dict[str, str | None] = {}
    missing: list[str] = []
    for name in raw_names:
        assert name is not None  #discarded above
        uid = jellyfin_db.resolve_username_to_id(name, JELLYFIN_DB_PATH)
        #ponytail Jellyfin PlaybackReporting stores UUIDs bare lowercase with no hyphens, normalize so imported rows group with real rows in GROUP BY queries instead of splitting into separate users
        if uid:
            uid = uid.replace("-", "").lower()
        uid_cache[name] = uid
        if uid is None:
            missing.append(name)
    if missing:
        print(f"⚠️  Could not resolve {len(missing)} username(s) in jellyfin.db: {', '.join(missing)}")
        print("   These rows will be skipped. Aborting — fix the usernames and retry.")
        sys.exit(1)

    #Build insertable rows
    insert_rows = []
    for r in rows:
        username = _norm_key(r, "username", "user", "user_name")
        duration = _norm_key(r, "play_duration", "duration", "playduration", "seconds")
        if username is None or duration is None:
            continue
        uid = uid_cache.get(username)
        if not uid:
            continue
        insert_rows.append((
            _parse_date(_norm_key(r, "date_created", "date", "timestamp")),
            uid,
            _norm_key(r, "item_id", "itemid") or "",
            _norm_key(r, "item_type", "type", "itemtype") or "Imported",
            _norm_key(r, "item_name", "name", "itemname", "title") or "Unknown",
            _norm_key(r, "playback_method", "method") or "Import",
            _norm_key(r, "client_name", "client") or "Import",
            _norm_key(r, "device_name", "device") or "Import",
            int(float(duration)),
        ))

    if not insert_rows:
        print("No valid rows to insert (check required fields: username, play_duration).")
        sys.exit(0)

    conn = sqlite3.connect(PLAYBACK_DB_PATH)
    try:
        conn.executemany(
            "INSERT INTO PlaybackActivity "
            "(DateCreated, UserId, ItemId, ItemType, ItemName, PlaybackMethod, ClientName, DeviceName, PlayDuration) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            insert_rows,
        )
        conn.commit()
        print(f"✅ Inserted {len(insert_rows)} rows into PlaybackActivity.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
