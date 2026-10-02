"""Offline synthetic verification; never pull images or use registry credentials."""

from __future__ import annotations

import hashlib
import json
import tarfile
from pathlib import Path

import pytest

from scripts import export_runtime_schema_runner as exporter

MANIFEST = "application/vnd.docker.distribution.manifest.v2+json"
CONFIG = "application/vnd.docker.container.image.v1+json"
LAYER = "application/vnd.docker.image.rootfs.diff.tar.gzip"


def _payload(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _put(source: Path, payload: bytes, media_type: str) -> dict:
    hexadecimal = hashlib.sha256(payload).hexdigest()
    (source / hexadecimal).write_bytes(payload)
    return {"digest": "sha256:" + hexadecimal, "size": len(payload), "mediaType": media_type}


def _source(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, change: str = "") -> Path:
    source = tmp_path / "source"
    source.mkdir()
    (source / "version").write_bytes(b"Directory Transport Version: 1.1\n")
    config = {
        "os": "linux",
        "architecture": "amd64",
        "config": {
            "User": exporter.REVIEWED_USER,
            "Labels": {
                "org.opencontainers.image.source": exporter.REVIEWED_SOURCE,
                "org.opencontainers.image.revision": exporter.REVIEWED_REVISION,
            },
        },
    }
    if change == "user":
        config["config"]["User"] = "root"
    if change == "revision":
        config["config"]["Labels"]["org.opencontainers.image.revision"] = "0" * 40
    configuration = _put(source, _payload(config), CONFIG)
    layer = _put(source, b"synthetic layer bytes", LAYER)
    if change == "traversal":
        layer["digest"] = "sha256:../../escape"
    if change == "external":
        layer["urls"] = ["https://example.invalid/credential-bearing-layer"]
    if change == "boolean_size":
        layer["size"] = True
    root = _payload({"schemaVersion": 2, "mediaType": MANIFEST, "config": configuration, "layers": [layer]})
    (source / "manifest.json").write_bytes(root)
    digest = "sha256:" + hashlib.sha256(root).hexdigest()
    # Only synthetic test roots are substituted; the operator CLI has no such switches.
    monkeypatch.setattr(exporter, "REVIEWED_DIGEST", digest)
    monkeypatch.setattr(exporter, "REVIEWED_MANIFEST_BYTES", len(root))
    monkeypatch.setattr(exporter, "REVIEWED_REFERENCE", f"{exporter.REVIEWED_REPOSITORY}@{digest}")
    return source


def _export(source: Path, output: Path) -> dict:
    return exporter.export_archive(
        source,
        output,
        workflow_repository=exporter.WORKFLOW_REPOSITORY,
        workflow_run_id="12345",
        workflow_commit="a" * 40,
    )


def test_export_preserves_original_graph_names_and_allowlist(tmp_path: Path, monkeypatch) -> None:
    source = _source(tmp_path, monkeypatch)
    output = tmp_path / "artifact"
    receipt = _export(source, output)
    assert {path.name for path in output.iterdir()} == set(exporter.ARTIFACT_FILES)
    assert receipt["image_reference"] == exporter.REVIEWED_REFERENCE
    assert receipt["workflow_repository"] == exporter.WORKFLOW_REPOSITORY
    assert receipt["credentials_exported"] is False
    assert receipt["image_bytes_rewritten"] is False
    with tarfile.open(output / exporter.ARTIFACT_FILES[0]) as archive:
        index = json.load(archive.extractfile("index.json"))
        root = index["manifests"][0]
        assert root["digest"] == exporter.REVIEWED_DIGEST
        assert root["mediaType"] == MANIFEST
        assert root["annotations"]["org.opencontainers.image.ref.name"] == exporter.REVIEWED_REFERENCE
        assert root["annotations"]["io.containerd.image.name"] == exporter.REVIEWED_REFERENCE
        for member in archive.getmembers():
            assert member.isfile()
            assert ".." not in member.name and not member.name.startswith("/")
            if member.name.startswith("blobs/"):
                payload = archive.extractfile(member).read()
                assert hashlib.sha256(payload).hexdigest() == member.name.split("/")[-1]
    sums = (output / "SHA256SUMS").read_text()
    assert receipt["archive_sha256"] in sums
    assert "credential" not in sums


@pytest.mark.parametrize("name", ["index.json", "oci-layout"])
@pytest.mark.parametrize("tamper", ["same_size", "append"])
def test_finished_archive_metadata_tampering_blocks_receipt(
    tmp_path: Path, monkeypatch, name, tamper
) -> None:
    source = _source(tmp_path, monkeypatch)
    output = tmp_path / "artifact"
    original = exporter._tar_bytes

    def altered_metadata(archive, member_name, payload):
        if member_name == name:
            if tamper == "append":
                payload += b" "
            elif name == "index.json":
                payload = payload.replace(b"ghcr.io", b"bait.io")
            else:
                payload = payload.replace(b"1.0.0", b"2.0.0")
        original(archive, member_name, payload)

    monkeypatch.setattr(exporter, "_tar_bytes", altered_metadata)
    with pytest.raises(ValueError, match="metadata bytes/size"):
        _export(source, output)
    assert not (output / "transport-receipt.json").exists()
    assert not (output / "SHA256SUMS").exists()


def test_wrong_workflow_repository_is_rejected_before_any_output(tmp_path: Path, monkeypatch) -> None:
    source = _source(tmp_path, monkeypatch)
    output = tmp_path / "artifact"
    with pytest.raises(ValueError, match="owning workflow repository"):
        exporter.export_archive(
            source,
            output,
            workflow_repository="fork-owner/kairos-persistence",
            workflow_run_id="12345",
            workflow_commit="a" * 40,
        )
    assert not output.exists()


def test_directory_entry_count_is_bounded(tmp_path: Path, monkeypatch) -> None:
    source = _source(tmp_path, monkeypatch)
    monkeypatch.setattr(exporter, "MAX_SOURCE_ENTRIES", 3)
    with pytest.raises(ValueError, match="directory exceeds the entry limit"):
        _export(source, tmp_path / "artifact")


@pytest.mark.parametrize("change", ["traversal", "external", "boolean_size", "user", "revision"])
def test_rejects_unsafe_or_unreviewed_graph(tmp_path: Path, monkeypatch, change: str) -> None:
    source = _source(tmp_path, monkeypatch, change=change)
    with pytest.raises(ValueError):
        _export(source, tmp_path / "artifact")
    assert not (tmp_path / "artifact").exists()


def test_rejects_root_hash_mismatch(tmp_path: Path, monkeypatch) -> None:
    source = _source(tmp_path, monkeypatch)
    root = source / "manifest.json"
    root.write_bytes(root.read_bytes().replace(b'"schemaVersion":2', b'"schemaVersion":3'))
    with pytest.raises(ValueError, match="root manifest"):
        _export(source, tmp_path / "artifact")


def test_rejects_layer_hash_mismatch(tmp_path: Path, monkeypatch) -> None:
    source = _source(tmp_path, monkeypatch)
    root = json.loads((source / "manifest.json").read_bytes())
    layer = source / root["layers"][0]["digest"].split(":")[1]
    layer.write_bytes(b"damaged layer bytes!!")
    with pytest.raises(ValueError, match="blob size/hash"):
        _export(source, tmp_path / "artifact")


@pytest.mark.parametrize("filename", ["auth.json", "../outside", "unreferenced"])
def test_rejects_non_graph_or_credential_files(tmp_path: Path, monkeypatch, filename: str) -> None:
    source = _source(tmp_path, monkeypatch)
    if filename == "../outside":
        (tmp_path / "outside").write_bytes(b"not referenced")
        # Traversal in descriptors, not merely an unrelated sibling file, must be rejected.
        root = json.loads((source / "manifest.json").read_bytes())
        root["layers"][0]["digest"] = "sha256:../outside"
        payload = _payload(root)
        (source / "manifest.json").write_bytes(payload)
        monkeypatch.setattr(exporter, "REVIEWED_DIGEST", "sha256:" + hashlib.sha256(payload).hexdigest())
        monkeypatch.setattr(exporter, "REVIEWED_MANIFEST_BYTES", len(payload))
    elif filename == "unreferenced":
        _put(source, b"unreferenced blob", LAYER)
    else:
        (source / filename).write_bytes(b"a credential must never enter the artifact")
    with pytest.raises(ValueError):
        _export(source, tmp_path / "artifact")


def test_refuses_existing_output(tmp_path: Path, monkeypatch) -> None:
    source = _source(tmp_path, monkeypatch)
    output = tmp_path / "artifact"
    output.mkdir()
    sentinel = output / "existing.txt"
    sentinel.write_text("keep", encoding="utf-8")
    with pytest.raises(ValueError, match="overwrite"):
        _export(source, output)
    assert sentinel.read_text() == "keep"


def test_rejects_symlink_source(tmp_path: Path, monkeypatch) -> None:
    source = _source(tmp_path, monkeypatch)
    link = tmp_path / "linked-source"
    try:
        link.symlink_to(source, target_is_directory=True)
    except OSError:
        pytest.skip("symlink creation is unavailable")
    with pytest.raises(ValueError, match="real directory"):
        _export(link, tmp_path / "artifact")


def test_workflow_has_read_only_dispatch_and_artifact_allowlist() -> None:
    workflow = (Path(__file__).parents[1] / ".github/workflows/export-runtime-schema-runner.yml").read_text()
    assert "workflow_dispatch:" in workflow and "packages: read" in workflow
    assert "github.repository == 'Kairos-cryptoAI/kairos-persistence'" in workflow
    assert "github.ref == 'refs/heads/main'" in workflow
    assert '--workflow-repository "$GITHUB_REPOSITORY"' in workflow
    assert "packages: write" not in workflow and "push:" not in workflow and "pull_request:" not in workflow
    assert "--all --preserve-digests" in workflow and "docker build" not in workflow
    assert 'trap \'rm -f -- "$auth_file"; rmdir -- "$auth_directory"\' EXIT' in workflow
    for name in exporter.ARTIFACT_FILES:
        assert f"kairos-runner-artifact/{name}" in workflow
    assert "kairos-runner-artifact/*" not in workflow
