"""Functional tests for inspection (巡检) and repair (修复).

Covers: per-batch verification of the shared original against its
registered length/SHA-256, suspension of all active referencing batches
with a visible reason, rejection of new downloads while numbers and
references are kept, repair by re-uploading the byte-identical ZIP
(restoring every active reference), rejection of different content, and
the rule that retired batches are never revived.
"""
from __future__ import annotations

import hashlib
import os

from conftest import make_zip, upload


def _corrupt(data_dir, body: bytes) -> str:
    """Overwrite the shared original with same-size garbage (bad sectors)."""
    digest = hashlib.sha256(body).hexdigest()
    (data_dir / "blobs" / digest).write_bytes(os.urandom(len(body)))
    return digest


def _repair(client, batch_id: str, body: bytes):
    return client.post(
        f"/batches/{batch_id}/repair",
        files={"file": (f"{batch_id}.zip", body, "application/zip")},
    )


def test_inspect_healthy_original(client):
    body = make_zip({"reads": b"ACGT"})
    upload(client, "I001", body)
    r = client.post("/batches/I001/inspect")
    assert r.status_code == 200, r.text
    payload = r.json()
    assert payload["verdict"] == "ok"
    assert payload["observed_sha256"] == hashlib.sha256(body).hexdigest()
    assert payload["observed_size_bytes"] == len(body)
    assert payload["suspended_batches"] == []
    # A healthy inspection changes nothing.
    assert client.get("/batches/I001/archive").status_code == 200


def test_inspect_missing_original_suspends_all_active_referrers(
    client, data_dir
):
    body = make_zip({"shared": b"data"})
    upload(client, "M1", body)
    upload(client, "M2", body)
    digest = hashlib.sha256(body).hexdigest()
    (data_dir / "blobs" / digest).unlink()

    r = client.post("/batches/M1/inspect")
    assert r.status_code == 200
    payload = r.json()
    assert payload["verdict"] == "missing"
    assert payload["observed_sha256"] is None
    assert sorted(payload["suspended_batches"]) == ["M1", "M2"]

    for b in ("M1", "M2"):
        # Queries show the reason.
        info = client.get(f"/batches/{b}").json()
        assert info["serving"] is False
        assert info["suspended_at"] is not None
        assert "missing" in info["suspend_reason"]
        # New downloads are rejected.
        r = client.get(f"/batches/{b}/archive")
        assert r.status_code == 409
        assert "missing" in r.json()["detail"]

    # Batch numbers and references are kept.
    listing = client.get("/batches").json()
    assert listing["total"] == 2
    summary = client.get("/storage/summary").json()
    assert summary["blob_rows"] == 1
    assert summary["suspended_batches"] == 2


def test_inspect_checksum_mismatch_suspends(client, data_dir):
    body = make_zip({"x": b"original-bytes"})
    upload(client, "C1", body)
    _corrupt(data_dir, body)
    r = client.post("/batches/C1/inspect")
    payload = r.json()
    assert payload["verdict"] == "mismatch"
    assert payload["suspended_batches"] == ["C1"]
    info = client.get("/batches/C1").json()
    assert "failed integrity check" in info["suspend_reason"]
    assert client.get("/batches/C1/archive").status_code == 409


def test_repair_restores_all_active_referrers(client, data_dir):
    body = make_zip({"shared": b"repair-me"})
    upload(client, "R1", body)
    upload(client, "R2", body)
    upload(client, "R3", make_zip({"other": b"untouched"}))
    _corrupt(data_dir, body)
    client.post("/batches/R1/inspect")
    assert client.get("/batches/R1/archive").status_code == 409
    assert client.get("/batches/R2/archive").status_code == 409

    # Re-uploading the byte-identical ZIP to an active batch repairs the
    # shared original.
    r = _repair(client, "R2", body)
    assert r.status_code == 200, r.text
    assert sorted(r.json()["restored_batches"]) == ["R1", "R2"]

    # Serving resumes for every active reference.
    for b in ("R1", "R2"):
        got = client.get(f"/batches/{b}/archive")
        assert got.status_code == 200
        assert got.content == body
        info = client.get(f"/batches/{b}").json()
        assert info["serving"] is True
        assert info["suspend_reason"] is None
        assert info["suspended_at"] is None
    # The unrelated batch was never affected.
    assert client.get("/batches/R3/archive").status_code == 200


def test_repair_with_different_content_rejected(client, data_dir):
    body = make_zip({"a": b"registered"})
    upload(client, "D1", body)
    _corrupt(data_dir, body)
    client.post("/batches/D1/inspect")

    r = _repair(client, "D1", make_zip({"a": b"different"}))
    assert r.status_code == 409
    # Still suspended afterwards; nothing was changed.
    assert client.get("/batches/D1/archive").status_code == 409


def test_repair_retired_batch_never_revived(client, data_dir):
    body = make_zip({"shared": b"no-resurrection"})
    upload(client, "T1", body)
    upload(client, "T2", body)
    _corrupt(data_dir, body)
    client.post("/batches/T1/inspect")       # T1 and T2 both suspended
    client.post("/batches/T2/retire")        # retire while suspended

    # A retired batch cannot be repaired/revived.
    assert _repair(client, "T2", body).status_code == 409

    # Repair through the still-active T1: only active references resume.
    r = _repair(client, "T1", body)
    assert r.status_code == 200
    assert r.json()["restored_batches"] == ["T1"]
    assert client.get("/batches/T1/archive").status_code == 200

    # T2 stays retired: download 404, number unusable, not revived.
    assert client.get("/batches/T2/archive").status_code == 404
    info = client.get("/batches/T2").json()
    assert info["status"] == "retired"
    assert info["serving"] is False
    assert upload(client, "T2", body).status_code == 409


def test_reupload_same_zip_to_active_batch_repairs(client, data_dir):
    """Re-uploading identical content via the plain upload endpoint also
    repairs the shared original (重传即修复)."""
    body = make_zip({"lane": b"reupload-repair"})
    upload(client, "U1", body)
    upload(client, "U2", body)
    _corrupt(data_dir, body)
    client.post("/batches/U1/inspect")
    assert client.get("/batches/U2/archive").status_code == 409

    r = upload(client, "U1", body)
    assert r.status_code == 200
    assert r.json()["reused_existing_content"] is True
    assert sorted(r.json()["restored_batches"]) == ["U1", "U2"]
    assert client.get("/batches/U1/archive").content == body
    assert client.get("/batches/U2/archive").content == body


def test_upload_different_content_to_suspended_batch_rejected(
    client, data_dir
):
    body = make_zip({"s": b"orig"})
    upload(client, "V1", body)
    _corrupt(data_dir, body)
    client.post("/batches/V1/inspect")
    r = upload(client, "V1", make_zip({"s": b"different"}))
    assert r.status_code == 409
    assert client.get("/batches/V1/archive").status_code == 409


def test_suspended_blob_survives_gc(client, data_dir):
    """A suspended batch is still a live reference: unreferenced-only GC
    must not reclaim the original it points to."""
    body = make_zip({"gc": b"keep"})
    upload(client, "G1", body)
    digest = _corrupt(data_dir, body)
    client.post("/batches/G1/inspect")
    r = client.post("/gc/run")
    assert r.status_code == 200
    assert digest not in r.json()["deleted_blobs"]
    assert (data_dir / "blobs" / digest).exists()
    summary = client.get("/storage/summary").json()
    assert summary["blob_rows"] == 1
    assert summary["suspended_batches"] == 1


def test_inspect_unknown_batch_404(client):
    assert client.post("/batches/NOPE/inspect").status_code == 404


def test_repair_unknown_batch_404(client):
    r = _repair(client, "NOPE", make_zip({"a": b"1"}))
    assert r.status_code == 404


def test_repair_rejects_non_zip_and_oversize(client):
    upload(client, "Z1", make_zip({"a": b"1"}))
    r = client.post(
        "/batches/Z1/repair",
        files={"file": ("x.bin", b"not a zip", "application/zip")},
    )
    assert r.status_code == 400
    big = make_zip({"big": os.urandom(2 * 1024 * 1024 + 1024)})
    r = client.post(
        "/batches/Z1/repair",
        files={"file": ("big.zip", big, "application/zip")},
    )
    assert r.status_code == 413


def test_batch_list_shows_suspended_with_reason(client, data_dir):
    body = make_zip({"l": b"list"})
    upload(client, "L1", body)
    upload(client, "L2", make_zip({"l": b"other"}))
    _corrupt(data_dir, body)
    client.post("/batches/L1/inspect")

    suspended = client.get("/batches", params={"status": "suspended"}).json()
    assert [b["batch_id"] for b in suspended["batches"]] == ["L1"]
    assert "failed integrity check" in suspended["batches"][0]["suspend_reason"]
    active = client.get("/batches", params={"status": "active"}).json()
    assert [b["batch_id"] for b in active["batches"]] == ["L2"]
    assert client.get("/batches", params={"status": "bogus"}).status_code == 400


def test_stale_inspection_cannot_suspend_repaired_original(client, data_dir):
    """A late inspection verdict computed from the old corrupt bytes must
    not suspend an original that was repaired in the meantime."""
    from app import storage

    body = make_zip({"race": b"stale-inspect"})
    upload(client, "S1", body)
    digest = _corrupt(data_dir, body)

    # The inspection reads the corrupt file; before it commits its
    # verdict, a repair lands (replacing the bytes and refreshing the
    # blob generation).
    def repair_first():
        storage.INSPECT_PAUSE_HOOK = None
        tmp = storage.stage_tmp(body)
        storage.repair_blob(digest, tmp, len(body))

    storage.INSPECT_PAUSE_HOOK = repair_first
    try:
        r = client.post("/batches/S1/inspect")
    finally:
        storage.INSPECT_PAUSE_HOOK = None
    assert r.status_code == 200
    payload = r.json()
    assert payload["verdict"] == "superseded"
    assert payload["suspended_batches"] == []

    # The repaired original keeps serving.
    got = client.get("/batches/S1/archive")
    assert got.status_code == 200
    assert got.content == body


def test_repair_is_idempotent_when_healthy(client):
    body = make_zip({"idem": b"healthy"})
    upload(client, "H1", body)
    for _ in range(2):
        r = _repair(client, "H1", body)
        assert r.status_code == 200
        assert r.json()["restored_batches"] == []
    assert client.get("/batches/H1/archive").content == body
