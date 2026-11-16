"""Crash recovery tests: GC interrupted by a hard process exit.

Scenarios
---------
1. Crash in phase 1 (row flipped to 'deleting', bytes moved away) and
   in phase 2 (bytes in quarantine, row still present): on restart,
   reconciliation must finish removing the now-unreferenced original.
2. After the same crash, the content becomes referenced again by a new
   batch: reconciliation must restore the bytes instead of deleting them.
3. The upload API itself reclaims a blob stuck in the 'deleting' state.
4. A repair that crashes after publishing the replacement file but before
   committing the serving state must leave batches suspended (nothing
   unverified is served) and be retryable after a restart.
"""
from __future__ import annotations

import hashlib
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

from fastapi.testclient import TestClient

from app.api import app
from conftest import make_zip, upload

ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / "scripts" / "crash_worker.py"
REPAIR_WORKER = ROOT / "scripts" / "repair_worker.py"


def _run_worker(data_dir: Path, hook: str) -> subprocess.CompletedProcess:
    # -I keeps the child interpreter isolated from PYTHONPATH pollution.
    return subprocess.run(
        [sys.executable, "-I", str(WORKER), str(data_dir), hook],
        capture_output=True,
        text=True,
        timeout=60,
    )


def _stage_retired_without_gc(data_dir: Path, batch_id: str, body: bytes) -> str:
    """Create an active batch via the API, then retire it via raw SQL.

    This reproduces the exact pre-GC state (retired batch, live blob,
    gc_queue empty) that a crash worker can then interrupt.
    """
    digest = hashlib.sha256(body).hexdigest()
    with TestClient(app) as c:
        assert upload(c, batch_id, body).status_code == 200

    conn = sqlite3.connect(data_dir / "archives.db")
    conn.execute(
        "UPDATE batches SET status = 'retired', "
        "retired_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') "
        "WHERE batch_id = ?",
        (batch_id,),
    )
    conn.commit()
    conn.close()
    assert (data_dir / "blobs" / digest).exists()
    return digest


def test_recovery_finishes_reclamation_after_phase1_crash(data_dir):
    body = make_zip({"doomed": b"ephemeral"})
    digest = _stage_retired_without_gc(data_dir, "DOOMED", body)

    proc = _run_worker(data_dir, "phase1")
    assert proc.returncode == 2, proc.stderr  # hard exit code
    # Interrupted between the row flip and the file move: the blob row is
    # stuck in 'deleting' while its bytes still sit in blobs/.
    row = sqlite3.connect(data_dir / "archives.db").execute(
        "SELECT status FROM blobs WHERE sha256 = ?", (digest,)
    ).fetchone()
    assert row == ("deleting",)
    assert (data_dir / "blobs" / digest).exists()

    with TestClient(app) as c:  # lifespan -> reconcile() resumes the sweep
        assert not (data_dir / "blobs" / digest).exists()
        assert not (data_dir / "quarantine" / digest).exists()
        summary = c.get("/storage/summary").json()
        assert summary["blob_rows"] == 0
        assert summary["blob_files_on_disk"] == 0
        assert summary["queued_for_gc"] == 0
        assert summary["quarantined_files"] == 0


def test_recovery_finishes_reclamation_after_phase2_crash(data_dir):
    body = make_zip({"doomed2": b"ephemeral-too"})
    digest = _stage_retired_without_gc(data_dir, "DOOMED", body)

    proc = _run_worker(data_dir, "phase2")
    assert proc.returncode == 2, proc.stderr
    assert (data_dir / "quarantine" / digest).exists()

    with TestClient(app) as c:
        summary = c.get("/storage/summary").json()
        assert summary["blob_rows"] == summary["blob_files_on_disk"] == 0
        assert summary["quarantined_files"] == 0
        assert summary["queued_for_gc"] == 0


def test_referenced_again_after_crash_is_restored(data_dir):
    body = make_zip({"rescued": b"keep-me"})
    digest = _stage_retired_without_gc(data_dir, "OLD", body)

    proc = _run_worker(data_dir, "phase1")
    assert proc.returncode == 2, proc.stderr
    # Row stuck in 'deleting' before the bytes were moved.
    row = sqlite3.connect(data_dir / "archives.db").execute(
        "SELECT status FROM blobs WHERE sha256 = ?", (digest,)
    ).fetchone()
    assert row == ("deleting",)

    # While the API was down, a new batch starts referencing the same
    # content hash (a concurrent instance committing the same upload).
    conn = sqlite3.connect(data_dir / "archives.db")
    conn.execute(
        "INSERT INTO batches (batch_id, sha256, size_bytes, status) "
        "VALUES (?, ?, ?, 'active')",
        ("NEW", digest, len(body)),
    )
    conn.commit()
    conn.close()

    with TestClient(app) as c:  # reconcile must restore, not delete
        assert (data_dir / "blobs" / digest).exists()
        assert not (data_dir / "quarantine" / digest).exists()

        row = sqlite3.connect(data_dir / "archives.db").execute(
            "SELECT status FROM blobs WHERE sha256 = ?", (digest,)
        ).fetchone()
        assert row == ("ready",)

        got = c.get("/batches/NEW/archive")
        assert got.status_code == 200
        assert hashlib.sha256(got.content).hexdigest() == digest
        summary = c.get("/storage/summary").json()
        assert summary["blob_rows"] == summary["blob_files_on_disk"] == 1
        assert summary["queued_for_gc"] == 0


def test_upload_reclaims_blob_stuck_in_deleting(data_dir):
    body = make_zip({"mid-gc": b"rescued-by-upload"})
    digest = hashlib.sha256(body).hexdigest()

    with TestClient(app) as c:
        upload(c, "OLD", body)

        # Reproduce the exact post-phase-1 state without hard-exiting.
        from app import db, storage
        with db.transaction() as conn:
            conn.execute(
                "UPDATE blobs SET status = 'deleting', "
                "quarantine_path = ? WHERE sha256 = ?",
                (str(storage.quarantine_path(digest)), digest),
            )
        storage.config.QUARANTINE_DIR.mkdir(exist_ok=True)
        storage.os.replace(
            storage.blob_path(digest), storage.quarantine_path(digest)
        )
        assert not (data_dir / "blobs" / digest).exists()

        # A new batch uploads identical content: the API must claim the
        # deleting row and move the bytes back.
        r = upload(c, "NEW", body)
        assert r.status_code == 200, r.text
        assert (data_dir / "blobs" / digest).exists()
        assert not (data_dir / "quarantine" / digest).exists()
        assert c.get("/batches/NEW/archive").content == body


def test_orphan_tmp_parts_are_reaped_on_restart(data_dir):
    body = make_zip({"z": b"abc"})
    with TestClient(app) as c:
        upload(c, "T1", body)

    # Simulate an upload that hard-crashed after staging its temp part.
    (data_dir / "tmp").mkdir(exist_ok=True)
    orphan = data_dir / "tmp" / "part-deadbeef"
    orphan.write_bytes(b"stuck")

    with TestClient(app) as c:
        assert c.get("/healthz").status_code == 200
        assert not orphan.exists()
        # Existing data untouched.
        assert c.get("/batches/T1/archive").status_code == 200


def test_staged_part_rescues_missing_blob_after_crash(data_dir):
    """Crash between DB COMMIT and file publish must not strand a batch."""
    body = make_zip({"late": b"published-after-restart"})
    digest = hashlib.sha256(body).hexdigest()

    # Build the exact post-commit/pre-publish state via SQL + a tmp part.
    from app import config
    with TestClient(app):  # creates schema, runs reconcile on empty store
        pass
    conn = sqlite3.connect(data_dir / "archives.db")
    conn.execute(
        "INSERT INTO blobs (sha256, size_bytes, status) VALUES (?, ?, 'ready')",
        (digest, len(body)),
    )
    conn.execute(
        "INSERT INTO batches (batch_id, sha256, size_bytes, status) "
        "VALUES (?, ?, ?, 'active')",
        ("LATE", digest, len(body)),
    )
    conn.commit()
    conn.close()
    part = config.TMP_DIR / "part-crashed"
    part.write_bytes(body)
    # The blob row exists but its bytes do not.
    assert not (data_dir / "blobs" / digest).exists()

    with TestClient(app) as c:
        got = c.get("/batches/LATE/archive")
        assert got.status_code == 200
        assert hashlib.sha256(got.content).hexdigest() == digest
        assert (data_dir / "blobs" / digest).exists()
        assert not part.exists()
        summary = c.get("/storage/summary").json()
        assert summary["blob_rows"] == summary["blob_files_on_disk"] == 1


def test_repair_crash_after_publish_stays_suspended_then_retry(
    data_dir, tmp_path
):
    """Repair interrupted between the file swap and the state commit.

    After the crash and a restart the batches must stay suspended (only
    fully verified originals are served, no half-repaired state), and the
    retained ZIP can simply be re-uploaded to finish the repair.
    """
    body = make_zip({"crash": b"repair-crash"})
    digest = hashlib.sha256(body).hexdigest()
    with TestClient(app) as c:
        assert upload(c, "RC1", body).status_code == 200
        assert upload(c, "RC2", body).status_code == 200

    # Disk corruption, then inspection suspends both referencing batches.
    (data_dir / "blobs" / digest).write_bytes(os.urandom(len(body)))
    with TestClient(app) as c:
        r = c.post("/batches/RC1/inspect")
        assert sorted(r.json()["suspended_batches"]) == ["RC1", "RC2"]
        assert c.get("/batches/RC1/archive").status_code == 409

    # Repair from the retained ZIP, hard-crashing after the replacement
    # file is published but before the serving state is committed.
    retained = tmp_path / "retained.zip"
    retained.write_bytes(body)
    proc = subprocess.run(
        [sys.executable, "-I", str(REPAIR_WORKER),
         str(data_dir), str(retained), "after_publish"],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode == 2, proc.stderr
    # The good bytes are already on disk, but the state switch never
    # committed.
    blob_file = data_dir / "blobs" / digest
    assert hashlib.sha256(blob_file.read_bytes()).hexdigest() == digest

    with TestClient(app) as c:  # restart: reconcile runs
        # Nothing unverified is served: both batches are still suspended.
        assert c.get("/batches/RC1/archive").status_code == 409
        assert c.get("/batches/RC2/archive").status_code == 409
        summary = c.get("/storage/summary").json()
        assert summary["suspended_batches"] == 2

        # The interrupted repair can be retried; it completes the switch.
        r = c.post(
            "/batches/RC1/repair",
            files={"file": ("rc1.zip", body, "application/zip")},
        )
        assert r.status_code == 200
        assert sorted(r.json()["restored_batches"]) == ["RC1", "RC2"]
        assert c.get("/batches/RC1/archive").content == body
        assert c.get("/batches/RC2/archive").content == body
        summary = c.get("/storage/summary").json()
        assert summary["suspended_batches"] == 0


def test_repair_crash_before_publish_leaves_no_partial_file(
    data_dir, tmp_path
):
    """A repair dying while staging must not leave a half-written original
    or any served state change behind."""
    body = make_zip({"stage": b"stage-crash"})
    digest = hashlib.sha256(body).hexdigest()
    with TestClient(app) as c:
        assert upload(c, "SC1", body).status_code == 200

    (data_dir / "blobs" / digest).write_bytes(os.urandom(len(body)))
    with TestClient(app) as c:
        c.post("/batches/SC1/inspect")
        assert c.get("/batches/SC1/archive").status_code == 409

    # Simulate a repair that crashed after staging but before the atomic
    # rename: only an orphan tmp part survives.
    orphan = data_dir / "tmp" / "part-repair-crashed"
    orphan.write_bytes(body)

    with TestClient(app) as c:  # restart reaps the staged part
        assert not orphan.exists()
        # Still suspended (the corrupt file is untouched) - no half state.
        assert c.get("/batches/SC1/archive").status_code == 409
        # Retry finishes the repair.
        r = c.post(
            "/batches/SC1/repair",
            files={"file": ("sc1.zip", body, "application/zip")},
        )
        assert r.status_code == 200
        assert r.json()["restored_batches"] == ["SC1"]
        assert c.get("/batches/SC1/archive").content == body
