"""Crash-recovery worker for the repair path.

Stages retained ZIP bytes and runs the crash-safe repair for one shared
original, hard-exiting (``os._exit``) at the configured checkpoint so no
cleanup can hide the interrupted state.  Invoked as a separate process by
``test_recovery.py`` to prove that an interrupted repair never serves a
half-repaired original and can simply be retried after a restart.

Usage: repair_worker.py <data_dir> <retained_zip> [hook]
"""
from __future__ import annotations

import hashlib
import os
import sys
from pathlib import Path

# Trusted project code: make the package importable when run as a file.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import config, db, storage  # noqa: E402


def main() -> int:
    data_dir = sys.argv[1]
    zip_path = sys.argv[2]
    hook = sys.argv[3] if len(sys.argv) > 3 else ""

    os.environ["ARCHIVE_DATA_DIR"] = data_dir
    config.DATA_DIR = Path(data_dir).resolve()
    config.BLOB_DIR = config.DATA_DIR / "blobs"
    config.QUARANTINE_DIR = config.DATA_DIR / "quarantine"
    config.TMP_DIR = config.DATA_DIR / "tmp"
    config.DB_PATH = config.DATA_DIR / "archives.db"
    config.ensure_layout()

    db.init_db()
    body = Path(zip_path).read_bytes()
    sha256 = hashlib.sha256(body).hexdigest()
    tmp = storage.stage_tmp(body)
    if hook:
        storage.REPAIR_CRASH_HOOK = hook
    storage.repair_blob(sha256, tmp, len(body))
    return 0


if __name__ == "__main__":
    sys.exit(main())
