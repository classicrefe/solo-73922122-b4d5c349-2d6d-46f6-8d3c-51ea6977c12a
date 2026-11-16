"""Content-addressed blob storage and crash-safe garbage collection.

Layout on the shared volume::

    blobs/<sha256>            one physical copy per distinct archive
    quarantine/<sha256>       blob moved out of blobs/ while being removed
    tmp/part-<uuid>           in-flight upload bodies

Garbage collection is split into two phases so it is safe across two
instances and across crashes:

* **mark** - every blob with no active batch reference is recorded in
  ``gc_queue``; files on disk that no blob row knows about are reaped.
* **sweep** - each queued blob is re-checked.  If it has gained a
  reference again (a new upload reused the hash) it is kept/restored.
  Otherwise the row is flipped to ``deleting``, the bytes move to
  quarantine and the blob row is deleted inside a second transaction;
  the quarantined file is unlinked afterwards.  A crash between steps
  leaves a ``deleting`` row or a quarantine file that
  :func:`reconcile` resumes on restart.

Upload ordering (also crash-safe):

1. stream + validate + hash into a ``tmp/`` part,
2. BEGIN IMMEDIATE, validate batch rules, insert/claim the blob row,
3. COMMIT, then atomically rename the part into ``blobs/`` if missing.

The row always exists before the file appears, so the mark reaper can
never mistake a fresh upload for an orphan, and every upload guarantees
the physical bytes for the row it claims.
"""
from __future__ import annotations

import hashlib
import io
import os
import time
import uuid
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO

from . import config, db

# Test hook: when set, the GC process exits (os._exit) at this checkpoint.
# Used by the crash-recovery tests; never set in production.
CRASH_HOOK: str | None = None

# Test hook: when set, repair_blob() exits (os._exit) right after the
# replacement file is published but before the serving state is committed.
REPAIR_CRASH_HOOK: str | None = None

# Test hook: callable invoked by inspect_batch() after the verdict has been
# computed from the file bytes but before it is committed.  Lets tests
# interleave a repair (or a stall) at exactly the racy moment.
INSPECT_PAUSE_HOOK: object = None


class UploadError(ValueError):
    """Raised for client-side upload problems (size, ZIP validity)."""


def _fsync_dir(path: Path) -> None:
    """Durably persist a rename/unlink inside a directory."""
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def blob_path(sha256: str) -> Path:
    return config.BLOB_DIR / sha256


def quarantine_path(sha256: str) -> Path:
    return config.QUARANTINE_DIR / sha256


# --------------------------------------------------------------------------- #
# Upload intake
# --------------------------------------------------------------------------- #

def read_and_validate(stream: BinaryIO, max_bytes: int) -> tuple[bytes, str]:
    """Read an upload body, enforcing the size cap and ZIP validity.

    The body is buffered in memory: 2 MiB is small enough and keeps the
    hashing / zip checks simple before anything touches the shared volume.
    Archive entries are never extracted, so a malicious ZIP cannot write
    outside the volume.
    """
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = stream.read(64 * 1024)
        if not chunk:
            break
        total += len(chunk)
        if total > max_bytes:
            raise UploadError(f"upload exceeds {max_bytes} bytes")
        chunks.append(chunk)
    body = b"".join(chunks)
    if not body:
        raise UploadError("upload body is empty")
    if not zipfile.is_zipfile(io.BytesIO(body)):
        raise UploadError("uploaded content is not a valid ZIP archive")
    return body, hashlib.sha256(body).hexdigest()


def stage_tmp(body: bytes) -> Path:
    """Durably write the body to a temp part (never scanned by the reaper)."""
    config.TMP_DIR.mkdir(parents=True, exist_ok=True)
    tmp = config.TMP_DIR / f"part-{uuid.uuid4().hex}"
    with open(tmp, "wb") as fh:
        fh.write(body)
        fh.flush()
        os.fsync(fh.fileno())
    return tmp


def publish(tmp: Path, sha256: str) -> None:
    """Move a staged part into blobs/ if the bytes are not already there."""
    config.BLOB_DIR.mkdir(parents=True, exist_ok=True)
    target = blob_path(sha256)
    if not target.exists():
        os.replace(tmp, target)
        _fsync_dir(config.BLOB_DIR)
    else:
        tmp.unlink(missing_ok=True)


def publish_replace(tmp: Path, sha256: str) -> None:
    """Atomically replace the original with the staged (verified) bytes.

    ``os.replace`` swaps the directory entry atomically, so concurrent
    readers keep streaming the old inode and never observe a half-written
    file; the staged part was fsynced before the rename, so the new bytes
    are durable.
    """
    config.BLOB_DIR.mkdir(parents=True, exist_ok=True)
    os.replace(tmp, blob_path(sha256))
    _fsync_dir(config.BLOB_DIR)


def new_generation() -> str:
    """Fresh token marking a change of a blob's published bytes."""
    return uuid.uuid4().hex


def hash_file(path: Path) -> tuple[int, str]:
    """Stream a file once, returning (size_bytes, sha256 hexdigest)."""
    digest = hashlib.sha256()
    size = 0
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(64 * 1024), b""):
            size += len(chunk)
            digest.update(chunk)
    return size, digest.hexdigest()


# --------------------------------------------------------------------------- #
# Inspection (巡检) and repair (修复)
# --------------------------------------------------------------------------- #

@dataclass
class InspectResult:
    batch_id: str
    sha256: str
    registered_size_bytes: int
    observed_size_bytes: int | None
    observed_sha256: str | None
    # ok | missing | mismatch | superseded
    verdict: str
    detail: str
    suspended_batches: list[str] = field(default_factory=list)


def _inspect_pause() -> None:
    """Test-only stall point between hashing the file and committing the
    verdict.  Two mechanisms, both inert in production:

    * ``INSPECT_PAUSE_HOOK`` - an in-process callable (unit tests).
    * ``ARCHIVE_INSPECT_GATE_FILE`` - while this path exists, the inspection
      waits (cross-process gate for the two-instance tests).  A
      ``<gate>.reached`` marker is touched so the test knows the inspection
      is parked with its verdict in hand.
    """
    if INSPECT_PAUSE_HOOK is not None:
        INSPECT_PAUSE_HOOK()  # type: ignore[operator]
    gate = os.environ.get("ARCHIVE_INSPECT_GATE_FILE")
    if gate and os.path.exists(gate):
        try:
            Path(gate + ".reached").touch()
        except OSError:
            pass
        deadline = time.monotonic() + 30.0
        while os.path.exists(gate) and time.monotonic() < deadline:
            time.sleep(0.05)


def inspect_batch(batch_id: str) -> InspectResult | None:
    """Verify the shared original behind ``batch_id`` against its
    registered length and SHA-256, suspending every active referencing
    batch when the original is missing or does not match.

    Locking: the registration is read without a lock and the file is
    hashed without any database transaction, so a slow or paused
    inspection never blocks repairs or unrelated batches.  Only the
    verdict commit takes the (short) write lock - and it is applied only
    if the blob's generation is unchanged since the file was read.  A
    repair that landed in between refreshes the generation, so a stale
    "bad original" verdict can never suspend an already-repaired original.
    """
    conn = db.get_conn()
    batch = conn.execute(
        "SELECT sha256, size_bytes FROM batches WHERE batch_id = ?",
        (batch_id,),
    ).fetchone()
    if batch is None:
        return None
    sha256 = batch["sha256"]
    registered = batch["size_bytes"]
    row = conn.execute(
        "SELECT generation FROM blobs WHERE sha256 = ?", (sha256,)
    ).fetchone()
    g0 = row["generation"] if row is not None else None

    path = blob_path(sha256)
    if not path.exists():
        observed_size: int | None = None
        observed_hash: str | None = None
        verdict = "missing"
    else:
        observed_size, observed_hash = hash_file(path)
        verdict = (
            "ok"
            if observed_size == registered and observed_hash == sha256
            else "mismatch"
        )

    _inspect_pause()

    if verdict == "missing":
        detail = f"shared original {sha256} is missing from storage"
    elif verdict == "mismatch":
        detail = (
            f"shared original {sha256} failed integrity check: registered "
            f"size {registered} sha256 {sha256}, observed size "
            f"{observed_size} sha256 {observed_hash}"
        )
    else:
        detail = (
            f"shared original {sha256} matches registered size and sha256"
        )

    suspended: list[str] = []
    with db.transaction() as conn:
        row = conn.execute(
            "SELECT generation FROM blobs WHERE sha256 = ?", (sha256,)
        ).fetchone()
        g1 = row["generation"] if row is not None else None
        if g0 != g1:
            return InspectResult(
                batch_id=batch_id,
                sha256=sha256,
                registered_size_bytes=registered,
                observed_size_bytes=observed_size,
                observed_sha256=observed_hash,
                verdict="superseded",
                detail=(
                    "the original changed while the inspection was in "
                    "flight; the stale verdict was discarded - re-run the "
                    "inspection"
                ),
            )
        now = conn.execute(
            "SELECT strftime('%Y-%m-%dT%H:%M:%fZ', 'now')"
        ).fetchone()[0]
        conn.execute(
            "UPDATE blobs SET last_inspected_at = ?, "
            "last_inspect_verdict = ? WHERE sha256 = ?",
            (now, verdict, sha256),
        )
        if verdict != "ok":
            rows = conn.execute(
                "SELECT batch_id FROM batches WHERE sha256 = ? "
                "AND status = 'active' AND suspended_at IS NULL",
                (sha256,),
            ).fetchall()
            suspended = [r["batch_id"] for r in rows]
            conn.execute(
                "UPDATE batches SET suspended_at = ?, suspend_reason = ? "
                "WHERE sha256 = ? AND status = 'active' "
                "AND suspended_at IS NULL",
                (now, detail, sha256),
            )

    return InspectResult(
        batch_id=batch_id,
        sha256=sha256,
        registered_size_bytes=registered,
        observed_size_bytes=observed_size,
        observed_sha256=observed_hash,
        verdict=verdict,
        detail=detail,
        suspended_batches=suspended,
    )


def repair_blob(sha256: str, tmp: Path, size_bytes: int) -> list[str]:
    """Restore a shared original from verified bytes and resume serving.

    The caller guarantees the staged bytes hash to ``sha256`` (the
    registered digest).  Ordering is crash-safe:

    1. the replacement file is atomically renamed into ``blobs/`` -
       readers never see a half-written file;
    2. one transaction refreshes the blob generation (invalidating any
       inspection verdict computed from the old bytes) and lifts the
       suspension of every *active* batch referencing the original.

    A crash or write failure between the steps leaves batches suspended
    (safe: nothing unverified is served) and the repair can simply be
    retried.  Retired batches are never touched: a retired number stays
    dead and is never revived.
    """
    publish_replace(tmp, sha256)
    if REPAIR_CRASH_HOOK == "after_publish":
        os._exit(2)
    with db.transaction() as conn:
        row = conn.execute(
            "SELECT status FROM blobs WHERE sha256 = ?", (sha256,)
        ).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO blobs (sha256, size_bytes, status, generation) "
                "VALUES (?, ?, 'ready', ?)",
                (sha256, size_bytes, new_generation()),
            )
        elif row["status"] == "deleting":
            # Reclaim from an in-flight GC, as uploads do.
            conn.execute(
                "UPDATE blobs SET status = 'ready', quarantine_path = NULL, "
                "generation = ? WHERE sha256 = ?",
                (new_generation(), sha256),
            )
            conn.execute("DELETE FROM gc_queue WHERE sha256 = ?", (sha256,))
        else:
            conn.execute(
                "UPDATE blobs SET generation = ? WHERE sha256 = ?",
                (new_generation(), sha256),
            )
        rows = conn.execute(
            "SELECT batch_id FROM batches WHERE sha256 = ? "
            "AND status = 'active' AND suspended_at IS NOT NULL",
            (sha256,),
        ).fetchall()
        restored = [r["batch_id"] for r in rows]
        conn.execute(
            "UPDATE batches SET suspended_at = NULL, suspend_reason = NULL "
            "WHERE sha256 = ? AND status = 'active' "
            "AND suspended_at IS NOT NULL",
            (sha256,),
        )
    # Defensive: drop a stale quarantined copy if one survived.
    move_back_from_quarantine(sha256)
    return restored


# --------------------------------------------------------------------------- #
# Garbage collection
# --------------------------------------------------------------------------- #

@dataclass
class GcReport:
    reaped_unknown_files: list[str] = field(default_factory=list)
    marked: list[str] = field(default_factory=list)
    deleted_blobs: list[str] = field(default_factory=list)
    restored_blobs: list[str] = field(default_factory=list)
    swept_queued: int = 0


def move_back_from_quarantine(sha256: str) -> None:
    """Return quarantined bytes to blobs/ (best effort, idempotent)."""
    qpath = quarantine_path(sha256)
    bpath = blob_path(sha256)
    if qpath.exists():
        if bpath.exists():
            # An upload already put fresh identical bytes back.
            qpath.unlink(missing_ok=True)
        else:
            os.replace(qpath, bpath)
            _fsync_dir(config.BLOB_DIR)


def _mark(report: GcReport) -> None:
    conn = db.get_conn()

    # Reap physical files that no blob row accounts for.  With the upload
    # ordering (row committed before the rename) these can only be crash
    # leftovers from older releases or manually placed junk.
    known = {row["sha256"] for row in conn.execute("SELECT sha256 FROM blobs")}
    on_disk = {p.name for p in config.BLOB_DIR.iterdir() if p.is_file()}
    for name in sorted(on_disk - known):
        (config.BLOB_DIR / name).unlink(missing_ok=True)
        report.reaped_unknown_files.append(name)

    # Queue every ready blob without an active batch reference.
    rows = conn.execute(
        """
        SELECT b.sha256
          FROM blobs b
         WHERE b.status = 'ready'
           AND NOT EXISTS (
                 SELECT 1 FROM batches t
                  WHERE t.sha256 = b.sha256 AND t.status = 'active'
               )
        """
    ).fetchall()
    if rows:
        with db.transaction():
            conn.executemany(
                "INSERT OR IGNORE INTO gc_queue (sha256) VALUES (?)",
                [(row["sha256"],) for row in rows],
            )
        report.marked = [row["sha256"] for row in rows]


def _finish_phase_two(sha256: str, report: GcReport) -> None:
    """Delete the blob row under the write lock, then remove its bytes.

    References are re-checked inside the transaction: a concurrently
    committed upload reusing this hash forces a restore instead of
    deletion.  If an upload re-created a *new* ``ready`` row while the
    old one sat in ``deleting``, the bytes are kept in quarantine for
    that upload to republish rather than unlinked.
    """
    conn = db.get_conn()
    with db.transaction():
        referenced = conn.execute(
            "SELECT 1 FROM batches WHERE sha256 = ? AND status = 'active' "
            "LIMIT 1",
            (sha256,),
        ).fetchone()
        if referenced:
            conn.execute(
                "UPDATE blobs SET status = 'ready', quarantine_path = NULL, "
                "generation = ? WHERE sha256 = ?",
                (new_generation(), sha256),
            )
            conn.execute("DELETE FROM gc_queue WHERE sha256 = ?", (sha256,))
            restored = True
        else:
            live_ready = conn.execute(
                "SELECT 1 FROM blobs WHERE sha256 = ? AND status = 'ready' "
                "LIMIT 1",
                (sha256,),
            ).fetchone()
            conn.execute("DELETE FROM blobs WHERE sha256 = ?", (sha256,))
            conn.execute("DELETE FROM gc_queue WHERE sha256 = ?", (sha256,))
            if live_ready:
                # An upload racing us already committed a replacement row.
                # Re-insert it so the table still names the bytes; the
                # upload's publish/move_back step will materialize them.
                size_row = conn.execute(
                    "SELECT size_bytes FROM batches WHERE sha256 = ? "
                    "LIMIT 1",
                    (sha256,),
                ).fetchone()
                qfile = quarantine_path(sha256)
                size_bytes = size_row["size_bytes"] if size_row else (
                    qfile.stat().st_size if qfile.exists() else 0
                )
                conn.execute(
                    "INSERT INTO blobs (sha256, size_bytes, status, "
                    "generation) VALUES (?, ?, 'ready', ?)",
                    (sha256, size_bytes, new_generation()),
                )
            restored = False

    if restored:
        move_back_from_quarantine(sha256)
        report.restored_blobs.append(sha256)
        return

    qpath = quarantine_path(sha256)
    if live_ready:
        # Keep the quarantined bytes: the racing upload moves them back
        # (or republishes identical bytes from its own staged part).
        if blob_path(sha256).exists():
            qpath.unlink(missing_ok=True)
        # The old 'deleting' row was replaced by a live 'ready' row.
        report.restored_blobs.append(sha256)
        return
    qpath.unlink(missing_ok=True)

    # A same-hash upload committing after our row delete recreates the row
    # before publishing the file.  Only remove an orphan without a live row.
    bpath = blob_path(sha256)
    if bpath.exists():
        live = conn.execute(
            "SELECT 1 FROM blobs WHERE sha256 = ?", (sha256,)
        ).fetchone()
        if live is None:
            bpath.unlink(missing_ok=True)
    _fsync_dir(config.QUARANTINE_DIR)
    report.deleted_blobs.append(sha256)


def _sweep_one(sha256: str, report: GcReport) -> None:
    conn = db.get_conn()
    row = conn.execute(
        "SELECT status, quarantine_path FROM blobs WHERE sha256 = ?",
        (sha256,),
    ).fetchone()

    if row is None:
        # Another instance already finished phase two.
        with db.transaction():
            conn.execute("DELETE FROM gc_queue WHERE sha256 = ?", (sha256,))
        report.swept_queued += 1
        return

    if row["status"] == "deleting":
        # Resume after a crash between the move and the row deletion.
        if CRASH_HOOK == "phase2":
            os._exit(2)
        _finish_phase_two(sha256, report)
        report.swept_queued += 1
        return

    # status == 'ready': re-check references before touching the bytes.
    referenced = conn.execute(
        "SELECT 1 FROM batches WHERE sha256 = ? AND status = 'active' LIMIT 1",
        (sha256,),
    ).fetchone()
    if referenced:
        with db.transaction():
            conn.execute("DELETE FROM gc_queue WHERE sha256 = ?", (sha256,))
        report.restored_blobs.append(sha256)
        return

    bpath = blob_path(sha256)
    if not bpath.exists():
        # Bytes already gone (crash/reaper); finish the bookkeeping.
        _finish_phase_two(sha256, report)
        report.swept_queued += 1
        return

    # Phase one: flip the row to 'deleting', then move bytes to quarantine.
    with db.transaction():
        conn.execute(
            "UPDATE blobs SET status = 'deleting', quarantine_path = ? "
            "WHERE sha256 = ? AND status = 'ready'",
            (str(quarantine_path(sha256)), sha256),
        )
    if CRASH_HOOK == "phase1":
        os._exit(2)
    qpath = quarantine_path(sha256)
    try:
        os.replace(bpath, qpath)
    except FileNotFoundError:
        # Another instance won the race and moved the bytes first.
        # Re-read the row and let the idempotent branches below decide
        # whether to finish the deletion or restore a re-referenced blob.
        row = conn.execute(
            "SELECT status FROM blobs WHERE sha256 = ?", (sha256,)
        ).fetchone()
        if row is None:
            with db.transaction():
                conn.execute(
                    "DELETE FROM gc_queue WHERE sha256 = ?", (sha256,)
                )
            report.swept_queued += 1
            return
        _sweep_one(sha256, report)
        return
    _fsync_dir(config.QUARANTINE_DIR)

    if CRASH_HOOK == "phase2":
        os._exit(2)
    _finish_phase_two(sha256, report)
    report.swept_queued += 1


def run_gc() -> GcReport:
    """Mark unreferenced originals and sweep everything queued."""
    report = GcReport()
    config.ensure_layout()
    _mark(report)

    conn = db.get_conn()
    queued = [r["sha256"] for r in conn.execute("SELECT sha256 FROM gc_queue")]
    for sha256 in queued:
        _sweep_one(sha256, report)
    return report


# --------------------------------------------------------------------------- #
# Startup reconciliation (crash recovery)
# --------------------------------------------------------------------------- #

def _restore_blob_row(sha256: str) -> None:
    """Mark a deleting blob live again and return its bytes."""
    conn = db.get_conn()
    with db.transaction():
        conn.execute(
            "UPDATE blobs SET status = 'ready', quarantine_path = NULL, "
            "generation = ? WHERE sha256 = ?",
            (new_generation(), sha256),
        )
        conn.execute("DELETE FROM gc_queue WHERE sha256 = ?", (sha256,))
    move_back_from_quarantine(sha256)


def reconcile() -> GcReport:
    """Resume interrupted GC work after a process exit/restart."""
    config.ensure_layout()

    # 1. In-flight upload parts never became rows; drop them - unless a
    #    referenced blob is missing its bytes (crash between COMMIT and
    #    publish), in which case the part is the only surviving copy and
    #    must be moved into place so an active batch never lacks its file.
    tmp_parts = list(config.TMP_DIR.glob("part-*"))
    if tmp_parts:
        missing = {
            r["sha256"]
            for r in db.get_conn().execute(
                "SELECT b.sha256 FROM blobs b "
                "WHERE b.status = 'ready' "
                "AND EXISTS (SELECT 1 FROM batches t "
                "            WHERE t.sha256 = b.sha256 AND t.status = 'active')"
            )
        }
        missing = {h for h in missing if not blob_path(h).exists()}
        for part in tmp_parts:
            rescued = False
            if missing:
                digest = hashlib.sha256(part.read_bytes()).hexdigest()
                if digest in missing:
                    os.replace(part, blob_path(digest))
                    _fsync_dir(config.BLOB_DIR)
                    # The original's bytes changed on disk: refresh the
                    # generation so an in-flight inspection on the other
                    # instance cannot act on a stale "missing" verdict.
                    with db.transaction() as conn:
                        conn.execute(
                            "UPDATE blobs SET generation = ? "
                            "WHERE sha256 = ?",
                            (new_generation(), digest),
                        )
                    missing.discard(digest)
                    rescued = True
            if not rescued:
                part.unlink(missing_ok=True)

    conn = db.get_conn()

    # 2. Blobs stuck in 'deleting': requeue; restore any that are
    #    referenced again, otherwise let the sweep finish them.
    deleting = [
        r["sha256"]
        for r in conn.execute(
            "SELECT sha256 FROM blobs WHERE status = 'deleting'"
        )
    ]
    for sha256 in deleting:
        conn.execute(
            "INSERT OR IGNORE INTO gc_queue (sha256) VALUES (?)", (sha256,)
        )
        referenced = conn.execute(
            "SELECT 1 FROM batches WHERE sha256 = ? AND status = 'active' "
            "LIMIT 1",
            (sha256,),
        ).fetchone()
        if referenced:
            _restore_blob_row(sha256)

    # 3. Quarantine files without a deleting row (unlink missed by crash).
    tracked_q = {
        r["sha256"]
        for r in conn.execute(
            "SELECT sha256 FROM blobs WHERE quarantine_path IS NOT NULL"
        )
    }
    for qfile in config.QUARANTINE_DIR.iterdir():
        if qfile.is_file() and qfile.name not in tracked_q:
            qfile.unlink(missing_ok=True)

    # 4. Continue the normal mark/sweep pipeline.
    return run_gc()
