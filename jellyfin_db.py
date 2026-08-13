"""Jellyfin SQLite queries — watch-time leaderboard and username resolution.

Reads directly from the Jellyfin PlaybackReporting plugin DB and the core
Jellyfin DB. Files are opened read-only (URI mode) so we never risk writing
to a live Jellyfin database, and connects are short-lived (open → query → close)
to avoid holding a stale WAL handle across long-lived bot uptime.
"""

import os
import sqlite3
from collections import defaultdict


def _ro_uri(path: str) -> str:
    """file: URI with immutable=1 so SQLite refuses any write."""
    abs_path = os.path.abspath(path)
    return f"file:{abs_path}?mode=ro&immutable=1"


def resolve_usernames(user_ids: list[str], jellyfin_db_path: str) -> dict[str, str]:
    """Map Jellyfin user UUIDs → usernames in one query. Falls back to the
    short UUID prefix for any id not found (e.g. deleted users)."""
    if not user_ids or not os.path.exists(jellyfin_db_path):
        return {uid: uid[:8] for uid in user_ids}
    #ponytail PlaybackActivity stores UUIDs lowercase with no hyphens while Users stores them uppercase with hyphens, normalize both sides to bare lowercase for the join
    norm_ids = [uid.replace("-", "").lower() for uid in user_ids]
    conn = sqlite3.connect(_ro_uri(jellyfin_db_path), uri=True)
    conn.row_factory = sqlite3.Row
    try:
        placeholders = ",".join("?" * len(user_ids))
        rows = conn.execute(
            f"SELECT LOWER(REPLACE(Id, '-', '')) AS norm_id, Username "
            f"FROM Users WHERE LOWER(REPLACE(Id, '-', '')) IN ({placeholders})",
            norm_ids,
        ).fetchall()
    finally:
        conn.close()
    found = {r["norm_id"]: r["Username"] for r in rows}
    return {uid: found.get(uid.replace("-", "").lower(), uid[:8]) for uid in user_ids}


def get_leaderboard(playback_db_path: str, jellyfin_db_path: str, limit: int = 10) -> list[dict]:
    """Top-N users by total watch duration. Returns list of dicts:
    [{"user_id", "username", "total_seconds", "play_count"}, ...] sorted desc."""
    if not os.path.exists(playback_db_path):
        return []
    conn = sqlite3.connect(_ro_uri(playback_db_path), uri=True)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT UserId, SUM(PlayDuration) AS total, COUNT(*) AS plays "
            "FROM PlaybackActivity "
            "WHERE UserId IS NOT NULL AND PlayDuration > 0 "
            "GROUP BY UserId ORDER BY total DESC LIMIT ?",
            (limit,),
        ).fetchall()
    finally:
        conn.close()

    user_ids = [r["UserId"] for r in rows]
    names = resolve_usernames(user_ids, jellyfin_db_path)
    return [
        {
            "user_id": r["UserId"],
            "username": names.get(r["UserId"], r["UserId"][:8]),
            "total_seconds": r["total"],
            "play_count": r["plays"],
        }
        for r in rows
    ]


def resolve_username_to_id(username: str, jellyfin_db_path: str) -> str | None:
    """Case-insensitive username → Jellyfin user UUID. Used by import_history."""
    if not os.path.exists(jellyfin_db_path):
        return None
    conn = sqlite3.connect(_ro_uri(jellyfin_db_path), uri=True)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT Id FROM Users WHERE NormalizedUsername = ? OR Username = ?",
            (username.upper(), username),
        ).fetchone()
    finally:
        conn.close()
    return row["Id"] if row else None


def get_media_leaderboard(playback_db_path: str, limit: int = 10):
    """Top-N most-watched media by play count. Episodes are grouped into
    their parent series (extracted from the 'SeriesName - sXXeYY' pattern);
    everything else is grouped by raw ItemName. Returns list of dicts:
    [{"name","item_type","plays","total_seconds"}, ...] sorted by plays desc.

    ponytail: SQLite has no regex; INSTR(' - s') is reliable for Jellyfin's
    episode naming convention. Misses series with ' - s' in the title itself,
    upgrade to a regex if that ever surfaces."""
    if not os.path.exists(playback_db_path):
        return []
    conn = sqlite3.connect(_ro_uri(playback_db_path), uri=True)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT "
            "  CASE WHEN ItemType='Episode' AND INSTR(ItemName,' - s')>0 "
            "    THEN SUBSTR(ItemName,1,INSTR(ItemName,' - s')-1) "
            "    ELSE ItemName END AS media_name, "
            "  ItemType, COUNT(*) AS plays, SUM(PlayDuration) AS total_dur "
            "FROM PlaybackActivity WHERE ItemId != '' "
            "GROUP BY media_name "
            "ORDER BY plays DESC LIMIT ?",
            (limit,),
        ).fetchall()
    finally:
        conn.close()
    return [
        {
            "name": r["media_name"],
            "item_type": r["ItemType"],
            "plays": r["plays"],
            "total_seconds": r["total_dur"] or 0,
        }
        for r in rows
    ]


def get_recent_activity(playback_db_path: str, jellyfin_db_path: str, limit: int = 10):
    """Most recent playback entries. Returns list of dicts:
    [{"username","item_name","duration","when"}, ...] newest first."""
    if not os.path.exists(playback_db_path):
        return []
    conn = sqlite3.connect(_ro_uri(playback_db_path), uri=True)
    conn.row_factory = sqlite3.Row
    try:
        #ponytail DateCreated has mixed formats, raw text sort puts T format above space format on the same date scrambling chronology, normalize for sort
        rows = conn.execute(
            "SELECT UserId, ItemName, PlayDuration, DateCreated "
            "FROM PlaybackActivity WHERE UserId IS NOT NULL "
            "AND ItemId != '' AND PlayDuration > 0 "
            "ORDER BY REPLACE(REPLACE(DateCreated, 'T', ' '), 'Z', '') DESC LIMIT ?",
            (limit,),
        ).fetchall()
    finally:
        conn.close()
    uids = [r["UserId"] for r in rows]
    names = resolve_usernames(uids, jellyfin_db_path)
    return [
        {
            "username": names.get(r["UserId"], r["UserId"][:8]),
            "item_name": r["ItemName"],
            "duration": r["PlayDuration"],
            "when": r["DateCreated"],
        }
        for r in rows
    ]


def get_user_stats(
    username: str, playback_db_path: str, jellyfin_db_path: str
) -> dict | None:
    """Single-user watch profile: total time, play count, top-3 media, last
    watched. Returns None if user not found in Jellyfin DB."""
    if not os.path.exists(playback_db_path):
        return None
    uid = resolve_username_to_id(username, jellyfin_db_path)
    if not uid:
        return None
    #ponytail normalize to bare lowercase to match PlaybackActivity format
    norm = uid.replace("-", "").lower()
    conn = sqlite3.connect(_ro_uri(playback_db_path), uri=True)
    conn.row_factory = sqlite3.Row
    try:
        total = conn.execute(
            "SELECT SUM(PlayDuration) AS dur, COUNT(*) AS plays "
            "FROM PlaybackActivity WHERE UserId = ? AND ItemId != '' "
            "AND PlayDuration > 0",
            (norm,),
        ).fetchone()
        top = conn.execute(
            "SELECT "
            "  CASE WHEN ItemType='Episode' AND INSTR(ItemName,' - s')>0 "
            "    THEN SUBSTR(ItemName,1,INSTR(ItemName,' - s')-1) "
            "    ELSE ItemName END AS media_name, "
            "  COUNT(*) AS plays "
            "FROM PlaybackActivity WHERE UserId = ? AND ItemId != '' "
            "AND PlayDuration > 0 "
            "GROUP BY media_name ORDER BY plays DESC LIMIT 3",
            (norm,),
        ).fetchall()
        last = conn.execute(
            "SELECT ItemName, PlayDuration, DateCreated "
            "FROM PlaybackActivity WHERE UserId = ? AND ItemId != '' "
            "AND PlayDuration > 0 "
            "ORDER BY REPLACE(REPLACE(DateCreated, 'T', ' '), 'Z', '') DESC LIMIT 1",
            (norm,),
        ).fetchone()
    finally:
        conn.close()
    return {
        "username": username,
        "total_seconds": total["dur"] or 0,
        "play_count": total["plays"] or 0,
        "top_media": [{"name": r["media_name"], "plays": r["plays"]} for r in top],
        "last_watched": {
            "name": last["ItemName"] if last else None,
            "duration": last["PlayDuration"] if last else 0,
            "when": last["DateCreated"] if last else None,
        } if last else None,
    }


#Self check
if __name__ == "__main__":
    import sys
    pdb = os.environ.get("PLAYBACK_DB_PATH", "")
    jdb = os.environ.get("JELLYFIN_DB_PATH", "")
    if not pdb or not jdb:
        print("Set PLAYBACK_DB_PATH and JELLYFIN_DB_PATH to run self-check.")
        sys.exit(1)
    board = get_leaderboard(pdb, jdb)
    if not board:
        print("No PlaybackActivity rows found (table empty). Queries are valid.")
    else:
        for i, e in enumerate(board, 1):
            mins = e["total_seconds"] // 60
            print(f"  {i}. {e['username']} — {mins}m ({e['play_count']} plays)")
    #Round trip username resolution
    uid = resolve_username_to_id("lunkman", jdb)
    assert uid, "resolve_username_to_id failed for 'lunkman'"
    print(f"OK: lunkman → {uid}")
