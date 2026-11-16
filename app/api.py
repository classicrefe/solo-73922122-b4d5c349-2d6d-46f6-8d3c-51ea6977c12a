"""FastAPI application: batch archive intake, download, retirement, GC.

Concurrency model
-----------------
All mutating endpoints run their checks and writes inside one
``BEGIN IMMEDIATE`` transaction, so two instances sharing the volume
serialize on SQLite's write lock and cannot observe each other's
half-committed state.  Downloads open the content file and stream it
outside any database transaction; on POSIX an unlinked (or quarantined)
file stays readable through the already-open file descriptor, so a slow
download always finishes and never blocks another batch's retirement.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel

from . import config, db, storage


# --------------------------------------------------------------------------- #
# Response models
# --------------------------------------------------------------------------- #

class BatchSummary(BaseModel):
    batch_id: str
    sha256: str
    size_bytes: int
    # Stored state: 'active' or 'retired'.  A suspended batch is still
    # 'active' - it keeps its number and its reference - but is not served.
    status: str
    # False while the batch is suspended by an inspection.
    serving: bool
    created_at: str
    retired_at: str | None
    suspended_at: str | None
    suspend_reason: str | None


class BatchListResponse(BaseModel):
    batches: list[BatchSummary]
    total: int


class StorageSummary(BaseModel):
    active_batches: int
    suspended_batches: int
    retired_batches: int
    # Rows in the blobs table - the number of originals that must exist.
    blob_rows: int
    # Physical files actually present in blobs/ - compare with blob_rows
    # to verify garbage collection finished its work.
    blob_files_on_disk: int
    quarantined_files: int
    queued_for_gc: int
    bytes_used_by_blobs: int


class GcResponse(BaseModel):
    marked: list[str]
    deleted_blobs: list[str]
    restored_blobs: list[str]
    reaped_unknown_files: list[str]
    swept_queued: int


class UploadResponse(BaseModel):
    batch_id: str
    sha256: str
    size_bytes: int
    reused_existing_content: bool
    # Batches whose suspension was lifted because this upload restored the
    # shared original (empty for ordinary uploads).
    restored_batches: list[str] = []


class InspectResponse(BaseModel):
    batch_id: str
    sha256: str
    registered_size_bytes: int
    observed_size_bytes: int | None
    observed_sha256: str | None
    # ok | missing | mismatch | superseded
    verdict: str
    detail: str
    suspended_batches: list[str]


class RepairResponse(BaseModel):
    batch_id: str
    sha256: str
    size_bytes: int
    # Active batches whose suspension was lifted by the repair.
    restored_batches: list[str]


# --------------------------------------------------------------------------- #
# Lifespan
# --------------------------------------------------------------------------- #

@asynccontextmanager
async def lifespan(app: FastAPI) -> Any:
    db.init_db()
    # Resume any GC work interrupted by a previous process exit.
    storage.reconcile()
    yield


app = FastAPI(
    title="Sequencing Center Archive Reclamation API",
    version="1.1.0",
    description=(
        "Upload per-batch ZIP archives (deduplicated by SHA-256), "
        "download original bytes, retire batches and garbage-collect "
        "originals that no batch references anymore.  Inspection "
        "(巡检) verifies each shared original against its registered "
        "length/SHA-256 and suspends affected batches; repair (修复) "
        "restores the original from a retained ZIP and resumes serving."
    ),
    lifespan=lifespan,
)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def _batch_to_model(row: Any) -> BatchSummary:
    return BatchSummary(
        batch_id=row["batch_id"],
        sha256=row["sha256"],
        size_bytes=row["size_bytes"],
        status=row["status"],
        serving=row["status"] == "active" and row["suspended_at"] is None,
        created_at=row["created_at"],
        retired_at=row["retired_at"],
        suspended_at=row["suspended_at"],
        suspend_reason=row["suspend_reason"],
    )


def _validate_batch_id(batch_id: str) -> None:
    """Keep batch identifiers to a sane, path-safe charset."""
    if not batch_id or len(batch_id) > 128:
        raise HTTPException(
            status_code=400,
            detail="batch_id must be 1..128 characters",
        )
    if any(ch not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._"
           for ch in batch_id):
        raise HTTPException(
            status_code=400,
            detail="batch_id may only contain letters, digits, '-', '.', '_'",
        )


# --------------------------------------------------------------------------- #
# Endpoints
# --------------------------------------------------------------------------- #

@app.post("/batches/{batch_id}/archive", response_model=UploadResponse)
def upload_archive(batch_id: str, file: UploadFile = File(...)) -> UploadResponse:
    """Store a batch's ZIP archive (max 2 MiB).

    * Same batch + same SHA-256 re-upload is a no-op success (reuse); if
      the shared original is missing or its batches are suspended, the
      re-upload doubles as a repair and resumes serving.
    * Same batch + different SHA-256 is rejected with 409.
    * Retired batch numbers can never be reused (409).
    * Identical SHA-256 content is stored exactly once on disk.
    """
    _validate_batch_id(batch_id)
    try:
        body, sha256 = storage.read_and_validate(file.file, config.MAX_UPLOAD_BYTES)
    except storage.UploadError as exc:
        raise HTTPException(status_code=413 if "exceeds" in str(exc) else 400,
                            detail=str(exc)) from exc

    # Stage bytes before taking the write lock so the critical section
    # only contains database work and a fast rename.
    tmp = storage.stage_tmp(body)

    try:
        reused = False
        restored: list[str] = []
        with db.transaction() as conn:
            batch = conn.execute(
                "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()

            if batch is not None:
                if batch["status"] == "retired":
                    raise HTTPException(
                        status_code=409,
                        detail=f"batch '{batch_id}' was retired and its number "
                               "can never be reused",
                    )
                if batch["sha256"] != sha256:
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            f"batch '{batch_id}' already holds content "
                            f"{batch['sha256']}; refusing to overwrite with "
                            f"different content {sha256}"
                        ),
                    )
                reused = True

            blob = conn.execute(
                "SELECT * FROM blobs WHERE sha256 = ?", (sha256,)
            ).fetchone()
            if blob is None:
                conn.execute(
                    "INSERT INTO blobs (sha256, size_bytes, status, "
                    "generation) VALUES (?, ?, 'ready', ?)",
                    (sha256, len(body), storage.new_generation()),
                )
            elif blob["status"] == "deleting":
                # GC had marked this content for removal, but a batch now
                # references it: reclaim it.
                conn.execute(
                    "UPDATE blobs SET status = 'ready', quarantine_path = "
                    "NULL, generation = ? WHERE sha256 = ?",
                    (storage.new_generation(), sha256),
                )
                conn.execute(
                    "DELETE FROM gc_queue WHERE sha256 = ?", (sha256,)
                )

            if batch is None:
                conn.execute(
                    "INSERT INTO batches (batch_id, sha256, size_bytes, status) "
                    "VALUES (?, ?, ?, 'active')",
                    (batch_id, sha256, len(body)),
                )

            # The committed rows above pin the original (GC cannot reclaim
            # a referenced blob).  If the original's bytes are gone or
            # active referrers are suspended, this upload is also a repair:
            # republish the verified bytes and lift the suspensions.
            need_restore = (
                not storage.blob_path(sha256).exists()
                or conn.execute(
                    "SELECT 1 FROM batches WHERE sha256 = ? "
                    "AND status = 'active' AND suspended_at IS NOT NULL "
                    "LIMIT 1",
                    (sha256,),
                ).fetchone() is not None
            )

        if need_restore:
            # Crash-safe order: the verified bytes land first, then one
            # transaction refreshes the generation and resumes serving.
            restored = storage.repair_blob(sha256, tmp, len(body))
            tmp = None
        else:
            # Transaction committed: publish the bytes (idempotent).  If GC
            # moved them to quarantine in parallel, move them back; if
            # another instance already published identical bytes, nothing
            # to do.
            storage.publish(tmp, sha256)
            if storage.quarantine_path(sha256).exists():
                storage.move_back_from_quarantine(sha256)
    except BaseException:
        if tmp is not None:
            tmp.unlink(missing_ok=True)
        raise

    return UploadResponse(
        batch_id=batch_id,
        sha256=sha256,
        size_bytes=len(body),
        reused_existing_content=reused,
        restored_batches=restored,
    )


@app.get("/batches/{batch_id}/archive")
def download_archive(batch_id: str) -> FileResponse:
    """Download the original uploaded bytes for an active batch.

    Returns 404 for unknown or retired batches (retired numbers stay
    dead) and 409 for batches suspended by an inspection (the reason is
    included).  Once the response starts, it streams from an open file
    descriptor, so concurrent garbage collection cannot interrupt it.
    """
    _validate_batch_id(batch_id)
    conn = db.get_conn()
    batch = conn.execute(
        "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
    ).fetchone()
    if batch is None or batch["status"] != "active":
        raise HTTPException(
            status_code=404,
            detail=f"no active batch '{batch_id}'",
        )
    if batch["suspended_at"] is not None:
        # Serving was stopped by an inspection; the batch number and its
        # reference are kept and a repair can resume serving.
        raise HTTPException(
            status_code=409,
            detail=(
                f"batch '{batch_id}' is suspended since "
                f"{batch['suspended_at']}: {batch['suspend_reason']}"
            ),
        )

    path = Path(config.BLOB_DIR) / batch["sha256"]
    if not path.exists():
        # Transient crash-recovery window: the row is live but bytes are
        # being restored.  Report missing rather than stream a broken file.
        raise HTTPException(
            status_code=404,
            detail=f"original for batch '{batch_id}' is currently unavailable",
        )

    # FileResponse opens the descriptor now and keeps it for the whole
    # (possibly slow) transfer.  No DB transaction is held meanwhile.
    return FileResponse(
        path,
        media_type="application/zip",
        filename=f"{batch_id}.zip",
    )


@app.post("/batches/{batch_id}/inspect", response_model=InspectResponse)
def inspect_batch(batch_id: str) -> InspectResponse:
    """巡检: verify the shared original behind a batch.

    The original's bytes are checked against the registered length and
    SHA-256.  If the file is missing or does not match, *every* active
    batch referencing it is suspended: queries show the reason, new
    downloads are rejected, and batch numbers/references are kept.  The
    file hashing runs outside any database lock, so a slow inspection
    never blocks repairs or unrelated batches; a verdict is committed
    only if the original has not changed since it was read (a verdict
    that lost the race to a repair is reported as ``superseded``).
    """
    _validate_batch_id(batch_id)
    result = storage.inspect_batch(batch_id)
    if result is None:
        raise HTTPException(
            status_code=404, detail=f"unknown batch '{batch_id}'"
        )
    return InspectResponse(**result.__dict__)


@app.post("/batches/{batch_id}/repair", response_model=RepairResponse)
def repair_batch(batch_id: str, file: UploadFile = File(...)) -> RepairResponse:
    """修复: restore the shared original from a retained ZIP.

    The uploaded ZIP must be byte-identical to the batch's registered
    content (same SHA-256); different content is rejected with 409.
    The verified bytes are atomically swapped in, then serving resumes
    for *all* active batches referencing the original.  Retired batches
    are never revived (409).  If the repair is interrupted (write
    failure, process exit) nothing unverified is served and the repair
    can simply be retried.
    """
    _validate_batch_id(batch_id)
    try:
        body, sha256 = storage.read_and_validate(file.file, config.MAX_UPLOAD_BYTES)
    except storage.UploadError as exc:
        raise HTTPException(status_code=413 if "exceeds" in str(exc) else 400,
                            detail=str(exc)) from exc

    batch = db.get_conn().execute(
        "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
    ).fetchone()
    if batch is None:
        raise HTTPException(
            status_code=404, detail=f"unknown batch '{batch_id}'"
        )
    if batch["status"] == "retired":
        raise HTTPException(
            status_code=409,
            detail=f"batch '{batch_id}' was retired and can never be revived",
        )
    if batch["sha256"] != sha256:
        raise HTTPException(
            status_code=409,
            detail=(
                f"uploaded content {sha256} does not match the registered "
                f"original {batch['sha256']} of batch '{batch_id}'; "
                "refusing to repair with different content"
            ),
        )

    tmp = storage.stage_tmp(body)
    try:
        restored = storage.repair_blob(sha256, tmp, len(body))
        tmp = None
    finally:
        if tmp is not None:
            tmp.unlink(missing_ok=True)

    return RepairResponse(
        batch_id=batch_id,
        sha256=sha256,
        size_bytes=len(body),
        restored_batches=restored,
    )


@app.post("/batches/{batch_id}/retire", status_code=200)
def retire_batch(batch_id: str) -> dict[str, Any]:
    """Retire a batch. Its number becomes permanently unusable.

    Retirement only flips batch state; the attached original is reclaimed
    by GC once no *active* batch references it.  GC runs inline after the
    state change so the space is released promptly.  The endpoint is
    idempotent: retiring an already-retired batch returns 200.
    """
    _validate_batch_id(batch_id)
    with db.transaction() as conn:
        batch = conn.execute(
            "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
        ).fetchone()
        if batch is None:
            raise HTTPException(
                status_code=404, detail=f"unknown batch '{batch_id}'"
            )
        already = batch["status"] == "retired"
        if not already:
            conn.execute(
                "UPDATE batches SET status = 'retired', "
                "retired_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') "
                "WHERE batch_id = ?",
                (batch_id,),
            )

    # Outside the transaction: GC takes its own short locks per blob,
    # keeping a slow download (which holds no DB lock) completely out of
    # the way of other batches' retirement.
    report = storage.run_gc()
    return {
        "batch_id": batch_id,
        "status": "retired",
        "already_retired": already,
        "gc": report.__dict__,
    }


@app.post("/gc/run", response_model=GcResponse)
def trigger_gc() -> GcResponse:
    """Manually run the mark-and-sweep garbage collection."""
    report = storage.run_gc()
    return GcResponse(**report.__dict__)


@app.get("/batches", response_model=BatchListResponse)
def list_batches(status: str | None = None) -> BatchListResponse:
    """List batch summaries, optionally filtered by serving state.

    ``status=active`` lists batches currently being served,
    ``status=suspended`` lists active batches stopped by an inspection
    (with the reason), ``status=retired`` lists retired numbers.
    """
    conn = db.get_conn()
    if status is not None:
        filters = {
            "active": "status = 'active' AND suspended_at IS NULL",
            "suspended": "status = 'active' AND suspended_at IS NOT NULL",
            "retired": "status = 'retired'",
        }
        where = filters.get(status)
        if where is None:
            raise HTTPException(
                status_code=400,
                detail="status filter must be 'active', 'suspended' or "
                       "'retired'",
            )
        rows = conn.execute(
            f"SELECT * FROM batches WHERE {where} ORDER BY batch_id",
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM batches ORDER BY batch_id"
        ).fetchall()
    return BatchListResponse(
        batches=[_batch_to_model(row) for row in rows], total=len(rows)
    )


@app.get("/batches/{batch_id}", response_model=BatchSummary)
def get_batch(batch_id: str) -> BatchSummary:
    """Return one batch's summary."""
    _validate_batch_id(batch_id)
    row = db.get_conn().execute(
        "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
    ).fetchone()
    if row is None:
        raise HTTPException(
            status_code=404, detail=f"unknown batch '{batch_id}'"
        )
    return _batch_to_model(row)


@app.get("/storage/summary", response_model=StorageSummary)
def storage_summary() -> StorageSummary:
    """Inventory view for reconciling reclamation results."""
    conn = db.get_conn()
    active = conn.execute(
        "SELECT COUNT(*) FROM batches WHERE status = 'active'"
    ).fetchone()[0]
    suspended = conn.execute(
        "SELECT COUNT(*) FROM batches WHERE status = 'active' "
        "AND suspended_at IS NOT NULL"
    ).fetchone()[0]
    retired = conn.execute(
        "SELECT COUNT(*) FROM batches WHERE status = 'retired'"
    ).fetchone()[0]
    blob_rows = conn.execute("SELECT COUNT(*) FROM blobs").fetchone()[0]
    queued = conn.execute("SELECT COUNT(*) FROM gc_queue").fetchone()[0]

    blob_files = [p for p in config.BLOB_DIR.iterdir() if p.is_file()]
    quarantined = [p for p in config.QUARANTINE_DIR.iterdir() if p.is_file()]
    return StorageSummary(
        active_batches=active,
        suspended_batches=suspended,
        retired_batches=retired,
        blob_rows=blob_rows,
        blob_files_on_disk=len(blob_files),
        quarantined_files=len(quarantined),
        queued_for_gc=queued,
        bytes_used_by_blobs=sum(p.stat().st_size for p in blob_files),
    )


@app.get("/healthz")
async def healthz() -> Response:
    db.get_conn().execute("SELECT 1")
    return Response(content="ok", media_type="text/plain")


@app.post("/internal/shutdown", include_in_schema=False)
def shutdown() -> Response:
    """Cooperative process shutdown for tests and orchestration.

    Disabled unless ``ENABLE_SHUTDOWN_ENDPOINT=1``: production containers
    are stopped with SIGTERM, and this endpoint must not be reachable by
    callers of the archive API.  When enabled it schedules a hard but
    orderly exit from a background thread (the response is flushed first)
    for runtimes whose seccomp policy forbids signaling child processes.
    """
    import os

    if os.environ.get("ENABLE_SHUTDOWN_ENDPOINT") != "1":
        raise HTTPException(status_code=404, detail="not found")

    import threading

    def _stop() -> None:
        import time

        time.sleep(0.2)  # let the HTTP response leave the socket
        os._exit(0)

    threading.Thread(target=_stop, daemon=True).start()
    return Response(content="shutting down", media_type="text/plain")
