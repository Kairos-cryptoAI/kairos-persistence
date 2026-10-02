"""Package only the already published, reviewed image bytes for offline delivery.

This script never builds an image, accesses credentials, calls a registry, or
changes a database. Skopeo must first copy the fixed digest to a fresh ``dir:``
transport. Docker/containerd can load the resulting OCI archive while retaining
the original manifest digest and repository name. No image bytes are rewritten.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import tarfile
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REVIEWED_REPOSITORY = "ghcr.io/kairos-cryptoai/kairos-runtime-schema-profile-runner"
REVIEWED_DIGEST = "sha256:2e10e9e936eae3a4a411f65d8b0bd14670ba808368eeff94b4e24021aa291077"
REVIEWED_REFERENCE = f"{REVIEWED_REPOSITORY}@{REVIEWED_DIGEST}"
REVIEWED_MANIFEST_BYTES = 2_199
REVIEWED_SOURCE = "https://github.com/Kairos-cryptoAI/kairos-persistence"
REVIEWED_REVISION = "1ca8bf38d265ece7a95f749a268075549f80c043"
REVIEWED_USER = "10001:10001"
WORKFLOW_REPOSITORY = "Kairos-cryptoAI/kairos-persistence"
ARTIFACT_FILES = ("runtime-schema-runner.oci.tar", "transport-receipt.json", "SHA256SUMS")
MAX_JSON_BYTES = 16 * 1024 * 1024
MAX_BLOB_BYTES = 1024 * 1024 * 1024
MAX_IMAGE_BYTES = 2 * MAX_BLOB_BYTES
MAX_BLOBS = 1_024
MAX_SOURCE_ENTRIES = MAX_BLOBS + 2
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_SOURCE_FILE = re.compile(r"[0-9a-f]{64}(?:\.manifest\.json)?\Z")
_MANIFEST_TYPES = {
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
}
_INDEX_TYPES = {
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
}
_CONFIG_TYPES = {
    "application/vnd.oci.image.config.v1+json",
    "application/vnd.docker.container.image.v1+json",
}
_LAYER_TYPES = {
    "application/vnd.oci.image.layer.v1.tar",
    "application/vnd.oci.image.layer.v1.tar+gzip",
    "application/vnd.oci.image.layer.v1.tar+zstd",
    "application/vnd.docker.image.rootfs.diff.tar.gzip",
}


def _hash_file(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    if path.stat().st_size > MAX_JSON_BYTES:
        raise ValueError("image JSON exceeds the allowed size")
    value = json.loads(path.read_bytes())
    if not isinstance(value, dict):
        raise ValueError("image JSON must be an object")
    return value


def _descriptor(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or "urls" in value:
        raise ValueError("image descriptors must be objects without external URLs")
    digest, size, media_type = value.get("digest"), value.get("size"), value.get("mediaType")
    if not isinstance(digest, str) or not _DIGEST.fullmatch(digest):
        raise ValueError("image descriptor requires a canonical SHA-256 digest")
    if type(size) is not int or size <= 0 or size > MAX_BLOB_BYTES:
        raise ValueError("image descriptor size is invalid")
    if not isinstance(media_type, str) or media_type not in (
        _MANIFEST_TYPES | _INDEX_TYPES | _CONFIG_TYPES | _LAYER_TYPES
    ):
        raise ValueError("unreviewed image descriptor media type")
    if "artifactType" in value or "subject" in value:
        raise ValueError("image artifacts/referrers are not runtime image blobs")
    return {"digest": digest, "size": size, "mediaType": media_type}


def _verify_config(value: dict[str, Any]) -> None:
    if value.get("os") != "linux" or value.get("architecture") != "amd64":
        raise ValueError("reviewed runner must be linux/amd64")
    config = value.get("config")
    if not isinstance(config, dict) or config.get("User") != REVIEWED_USER:
        raise ValueError("reviewed runner user differs")
    labels = config.get("Labels")
    if not isinstance(labels, dict) or (
        labels.get("org.opencontainers.image.source") != REVIEWED_SOURCE
        or labels.get("org.opencontainers.image.revision") != REVIEWED_REVISION
    ):
        raise ValueError("reviewed runner source/revision labels differ")


def verify_source(source_directory: Path) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    """Validate the fixed root and the complete referenced, content-addressed graph."""
    if source_directory.is_symlink() or not source_directory.is_dir():
        raise ValueError("source must be a real directory")
    source = source_directory.resolve(strict=True)
    entries: dict[str, Path] = {}
    for entry in source.iterdir():
        if len(entries) >= MAX_SOURCE_ENTRIES:
            raise ValueError("source directory exceeds the entry limit")
        if (
            entry.is_symlink()
            or not entry.is_file()
            or not (entry.name in {"version", "manifest.json"} or _SOURCE_FILE.fullmatch(entry.name))
        ):
            raise ValueError("source contains a non-allowlisted file or symlink")
        if entry.stat().st_size > MAX_BLOB_BYTES:
            raise ValueError("source blob exceeds allowed size")
        entries[entry.name] = entry
    if "manifest.json" not in entries or "version" not in entries:
        raise ValueError("source is not the expected Skopeo directory transport")
    version = b"Directory Transport Version: 1.1\n"
    if entries["version"].stat().st_size != len(version) or entries["version"].read_bytes() != version:
        raise ValueError("source directory transport version differs")
    root_path = entries["manifest.json"]
    if root_path.stat().st_size != REVIEWED_MANIFEST_BYTES or (
        "sha256:" + _hash_file(root_path) != REVIEWED_DIGEST
    ):
        raise ValueError("root manifest differs from the exact published runner")
    root_value = _read_json(root_path)
    root = _descriptor(
        {"digest": REVIEWED_DIGEST, "size": REVIEWED_MANIFEST_BYTES, "mediaType": root_value.get("mediaType")}
    )
    if root["mediaType"] not in _MANIFEST_TYPES | _INDEX_TYPES:
        raise ValueError("root must be an image manifest or index")
    pending = [root]
    graph: dict[str, dict[str, Any]] = {}
    used_names = {"version", "manifest.json"}
    total_bytes = 0
    while pending:
        descriptor = _descriptor(pending.pop())
        digest = descriptor["digest"]
        if digest in graph:
            if {key: graph[digest][key] for key in descriptor} != descriptor:
                raise ValueError("conflicting image descriptors")
            continue
        if len(graph) >= MAX_BLOBS:
            raise ValueError("image graph exceeds the blob limit")
        hexadecimal = digest.removeprefix("sha256:")
        candidates = (
            ["manifest.json"]
            if digest == REVIEWED_DIGEST
            else [
                hexadecimal,
                f"{hexadecimal}.manifest.json",
            ]
        )
        matches = [entries[name] for name in candidates if name in entries]
        if len(matches) != 1:
            raise ValueError("referenced image blob is missing or ambiguous")
        path = matches[0]
        if path.stat().st_size != descriptor["size"] or "sha256:" + _hash_file(path) != digest:
            raise ValueError("referenced image blob size/hash mismatch")
        total_bytes += descriptor["size"]
        if total_bytes > MAX_IMAGE_BYTES:
            raise ValueError("image graph exceeds the total size limit")
        graph[digest] = {**descriptor, "path": path}
        used_names.add(path.name)
        media_type = descriptor["mediaType"]
        if media_type in _CONFIG_TYPES:
            _verify_config(_read_json(path))
        elif media_type in _MANIFEST_TYPES | _INDEX_TYPES:
            value = _read_json(path)
            if value.get("schemaVersion") != 2 or value.get("mediaType") != media_type:
                raise ValueError("manifest identity differs from its descriptor")
            if "subject" in value or "artifactType" in value:
                raise ValueError("image manifest cannot contain artifacts/referrers")
            if media_type in _INDEX_TYPES:
                children = value.get("manifests")
                if not isinstance(children, list) or not children or len(children) > MAX_BLOBS:
                    raise ValueError("manifest index must contain image manifests")
                for child in children:
                    if _descriptor(child)["mediaType"] not in _MANIFEST_TYPES | _INDEX_TYPES:
                        raise ValueError("index contains a non-manifest descriptor")
            else:
                config = _descriptor(value.get("config"))
                if config["mediaType"] not in _CONFIG_TYPES:
                    raise ValueError("manifest configuration media type differs")
                layers = value.get("layers")
                if not isinstance(layers, list) or not layers or len(layers) > MAX_BLOBS:
                    raise ValueError("runtime image must contain filesystem layers")
                if any(_descriptor(layer)["mediaType"] not in _LAYER_TYPES for layer in layers):
                    raise ValueError("manifest contains a non-layer descriptor")
                children = [config, *layers]
            pending.extend(children)
    if set(entries) != used_names:
        raise ValueError("source contains unreferenced files; refusing to export them")
    return root, graph


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def _tar_bytes(archive: tarfile.TarFile, name: str, payload: bytes) -> None:
    info = tarfile.TarInfo(name)
    info.size, info.mode = len(payload), 0o644
    archive.addfile(info, io.BytesIO(payload))


def _verify_archive(
    path: Path, graph: dict[str, dict[str, Any]], *, index_bytes: bytes, layout_bytes: bytes
) -> None:
    """Re-read every archive member, including the repository-binding metadata."""
    metadata = {"oci-layout": layout_bytes, "index.json": index_bytes}
    with tarfile.open(path, mode="r:") as archive:
        members = archive.getmembers()
        expected = set(metadata) | {"blobs/sha256/" + digest.removeprefix("sha256:") for digest in graph}
        if len(members) != len(expected) or {member.name for member in members} != expected:
            raise ValueError("archive member allowlist differs")
        for member in members:
            if not member.isfile():
                raise ValueError("archive may contain only regular files")
            stream = archive.extractfile(member)
            if stream is None:
                raise ValueError("archive member cannot be read")
            with stream:
                if member.name in metadata:
                    expected_bytes = metadata[member.name]
                    if (
                        member.size != len(expected_bytes)
                        or stream.read(len(expected_bytes) + 1) != expected_bytes
                    ):
                        raise ValueError("finished archive metadata bytes/size differ")
                else:
                    actual = hashlib.file_digest(stream, "sha256").hexdigest()
                    digest = "sha256:" + member.name.removeprefix("blobs/sha256/")
                    if (
                        digest not in graph
                        or actual != digest.removeprefix("sha256:")
                        or member.size != graph[digest]["size"]
                    ):
                        raise ValueError("finished archive blob size/hash differs")


def export_archive(
    source: Path, output: Path, *, workflow_repository: str, workflow_run_id: str, workflow_commit: str
) -> dict:
    """Create the three-file artifact only after all input bytes have been verified."""
    if workflow_repository != WORKFLOW_REPOSITORY:
        raise ValueError("export requires the exact owning workflow repository")
    if not re.fullmatch(r"[1-9][0-9]{0,19}", workflow_run_id) or not re.fullmatch(
        r"[0-9a-f]{40}", workflow_commit
    ):
        raise ValueError("export must be bound to a concrete workflow run and commit")
    root, graph = verify_source(source)
    if output.is_symlink() or output.exists():
        raise ValueError("refusing to overwrite any existing output/evidence directory")
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    index = {
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "manifests": [
            {
                **root,
                "annotations": {
                    "org.opencontainers.image.ref.name": REVIEWED_REFERENCE,
                    "io.containerd.image.name": REVIEWED_REFERENCE,
                },
            }
        ],
    }
    layout_bytes = b'{"imageLayoutVersion":"1.0.0"}\n'
    index_bytes = _json_bytes(index)
    archive_path = output / ARTIFACT_FILES[0]
    with archive_path.open("xb") as handle, tarfile.open(fileobj=handle, mode="w") as archive:
        _tar_bytes(archive, "oci-layout", layout_bytes)
        _tar_bytes(archive, "index.json", index_bytes)
        for digest, item in sorted(graph.items()):
            path = item["path"]
            if path.is_symlink() or "sha256:" + _hash_file(path) != digest:
                raise ValueError("verified source changed before packaging")
            info = tarfile.TarInfo("blobs/sha256/" + digest.removeprefix("sha256:"))
            info.size, info.mode = item["size"], 0o644
            with path.open("rb") as blob:
                archive.addfile(info, blob)
        handle.flush()
        os.fsync(handle.fileno())
    _verify_archive(archive_path, graph, index_bytes=index_bytes, layout_bytes=layout_bytes)
    receipt = {
        "schema_version": "kairos.runtime-schema-runner-transport.v1",
        "classification": "EXACT_PUBLISHED_IMAGE_TRANSPORT_ONLY",
        "created_at_utc": datetime.now(UTC).isoformat(),
        "workflow_repository": workflow_repository,
        "workflow_file": "export-runtime-schema-runner.yml",
        "workflow_run_id": workflow_run_id,
        "workflow_commit": workflow_commit,
        "image_reference": REVIEWED_REFERENCE,
        "original_manifest": root,
        "persistence_revision": REVIEWED_REVISION,
        "runner_user": REVIEWED_USER,
        "blob_inventory": [{key: item[key] for key in root} for _, item in sorted(graph.items())],
        "archive_file": archive_path.name,
        "archive_bytes": archive_path.stat().st_size,
        "archive_sha256": _hash_file(archive_path),
        "artifact_files": list(ARTIFACT_FILES),
        "rebuild_performed": False,
        "image_bytes_rewritten": False,
        "credentials_exported": False,
        "database_contacted": False,
        "readiness_changed": False,
    }
    receipt_path = output / ARTIFACT_FILES[1]
    with receipt_path.open("xb") as handle:
        handle.write(_json_bytes(receipt))
    sums = (
        f"{receipt['archive_sha256']}  {archive_path.name}\n{_hash_file(receipt_path)}  {receipt_path.name}\n"
    )
    with (output / ARTIFACT_FILES[2]).open("x", encoding="ascii", newline="\n") as handle:
        handle.write(sums)
    artifact_entries = list(output.iterdir())
    if {path.name for path in artifact_entries} != set(ARTIFACT_FILES) or any(
        path.is_symlink() or not path.is_file() for path in artifact_entries
    ):
        raise ValueError("artifact output allowlist differs")
    return receipt


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-directory", required=True, type=Path)
    parser.add_argument("--output-directory", required=True, type=Path)
    parser.add_argument("--workflow-repository", required=True)
    parser.add_argument("--workflow-run-id", required=True)
    parser.add_argument("--workflow-commit", required=True)
    args = parser.parse_args(argv)
    receipt = export_archive(
        args.source_directory,
        args.output_directory,
        workflow_repository=args.workflow_repository,
        workflow_run_id=args.workflow_run_id,
        workflow_commit=args.workflow_commit,
    )
    print(json.dumps({"classification": receipt["classification"], "image_reference": REVIEWED_REFERENCE}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
