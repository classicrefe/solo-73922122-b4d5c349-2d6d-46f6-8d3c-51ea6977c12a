"""SQLite access shared by every worker thread / process.

Design notes
------------
* WAL + ``busy_timeout`` let two API instances on the same volume take
  short write turns without seeing "database is locked".
* Each thread keeps its own connection (check_same_thread is per
  connection) and every connection is opened in autocommit mode; the
  ``transaction()`` context manager supplies explicit BEGIN/COMMIT so
  locking scope is obvious.
"""
from __future__ import annotations

import sqlite3
import threading
import time
from contextlib import contextmanager
from typing import Iterator

from . import config

# How long a writer waits behind another instance's lock before failing.
BUSY_TIMEOUT_MS = 15_000

_SCHEMA = """
CREATE TABLE IF NOT EXISTS blobs (
    sha256          TEXT PRIMARY KEY,
    size_bytes      INTEGER NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('ready', 'deleting')),
    quarantine_path TEXT,
    -- Token refreshed every time the original's bytes are (re)published or
    -- repaired.  An inspection compares it before/after hashing the file,
    -- so a verdict computed from stale bytes is never applied to an
    -- original that has since been repaired.
    generation      TEXT NOT NULL DEFAULT '',
    last_inspected_at TEXT,
    last_inspect_verdict TEXT
);

CREATE TABLE IF NOT EXISTS batches (
    batch_id    TEXT PRIMARY KEY,
    sha256      TEXT NOT NULL,
    size_bytes  INTEGER NOT NULL,
    status      TEXT NOT NULL CHECK (status IN ('active', 'retired')),
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    retired_at  TEXT,
    -- Set when an inspection found the shared original missing/corrupt:
    -- the batch keeps its number and its reference but is not served.
    suspended_at TEXT,
    suspend_reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_batches_sha256 ON batches(sha256);

-- One row per blob currently scheduled for / in the middle of garbage
-- collection.  Surviving rows after a crash make GC resumable.
CREATE TABLE IF NOT EXISTS gc_queue (
    sha256     TEXT PRIMARY KEY,
    enqueued_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
"""

# Columns added after the initial release: ALTER existing databases
# forward (CREATE TABLE IF NOT EXISTS leaves old files untouched).
_MIGRATIONS = (
    ("blobs", "generation",
     "ALTER TABLE blobs ADD COLUMN generation TEXT NOT NULL DEFAULT ''"),
    ("blobs", "last_inspected_at",
     "ALTER TABLE blobs ADD COLUMN last_inspected_at TEXT"),
    ("blobs", "last_inspect_verdict",
     "ALTER TABLE blobs ADD COLUMN last_inspect_verdict TEXT"),
    ("batches", "suspended_at",
     "ALTER TABLE batches ADD COLUMN suspended_at TEXT"),
    ("batches", "suspend_reason",
     "ALTER TABLE batches ADD COLUMN suspend_reason TEXT"),
)

_local = threading.local()
# Every live connection handed out, across all threads in the process.
# Connections live for the whole process; reset_connections() drains this.
_live_conns: set[sqlite3.Connection] = set()
_all_conns_lock = threading.Lock()


def _is_lock_contention(exc: sqlite3.OperationalError) -> bool:
    return "locked" in str(exc).lower()


def _run_with_lock_retry(fn, attempts: int = 30) -> None:
    """Retry a schema/pragma statement across brief lock conflicts.

    Switching the journal mode or altering the schema needs a lock
    *upgrade*, which SQLite refuses to wait for via the busy handler (it
    fails fast to avoid deadlocks).  Two instances starting together on a
    fresh shared volume hit exactly that, so retry in application code.
    """
    for attempt in range(attempts):
        try:
            fn()
            return
        except sqlite3.OperationalError as exc:
            if not _is_lock_contention(exc) or attempt == attempts - 1:
                raise
            time.sleep(0.1)


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(
        config.DB_PATH,
        timeout=BUSY_TIMEOUT_MS / 1000.0,
        isolation_level=None,  # autocommit; transactions are explicit
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
    # WAL is persistent once set; doing it on every connection is harmless
    # and covers a freshly created database on the shared volume.
    _run_with_lock_retry(lambda: conn.execute("PRAGMA journal_mode = WAL"))
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA foreign_keys = ON")
    with _all_conns_lock:
        _live_conns.add(conn)
    return conn


def get_conn() -> sqlite3.Connection:
    conn = getattr(_local, "conn", None)
    # A reset_connections() call (e.g. test relocating the data dir)
    # closes the shared connection; every thread then reconnects.
    if conn is None or conn not in _live_conns:
        conn = _connect()
        _local.conn = conn
    return conn


def reset_connections() -> None:
    """Close every connection in every thread (test/relocation support)."""
    with _all_conns_lock:
        conns = list(_live_conns)
        _live_conns.clear()
    for conn in conns:
        conn.close()
    if hasattr(_local, "conn"):
        del _local.conn


def init_db() -> None:
    config.ensure_layout()
    conn = get_conn()
    _run_with_lock_retry(lambda: conn.executescript(_SCHEMA))
    for table, column, ddl in _MIGRATIONS:
        cols = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        if column not in cols:
            try:
                _run_with_lock_retry(lambda: conn.execute(ddl))
            except sqlite3.OperationalError as exc:
                # Two instances starting together can race the ALTER; the
                # loser sees the column already present, which is fine.
                if "duplicate column" not in str(exc).lower():
                    raise


@contextmanager
def transaction() -> Iterator[sqlite3.Connection]:
    """Serialize a unit of work with an explicit IMMEDIATE transaction.

    BEGIN IMMEDIATE takes the WAL write lock up front, so two concurrent
    instances uploading the same batch cannot both pass the "does this
    batch already exist?" check.
    """
    conn = get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
