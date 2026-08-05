"""Tests for landline.media.extract — external archive-extractor wrapper.

Never depends on the real safe-unzip binary. Every exit-code branch is
driven by a tiny fake extractor script written into ``tmp_path``:
prints a canned JSON manifest to stdout and exits with a chosen code.
Mirrors the ``test_voice_transcribe.py`` "fake subprocess" style.

Also covers the privacy invariant (metadata-only log lines) and the
never-raise contract that keeps the dispatch loop from wedging.
"""

import json
import os
import stat
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from landline.media.extract import ExtractionResult, extract_archive


# ---------------------------------------------------------------------------
# Fake-extractor helpers
# ---------------------------------------------------------------------------


def _write_fake_extractor(tmp_path: Path, body: str) -> str:
    """Write an executable python script that runs ``body`` and return its path.

    The script executes ``body`` via ``exec()`` inside a small preamble
    that exposes ``sys`` and parses --dest so tests can echo it back.
    """
    script = tmp_path / "fake-extractor.py"
    header = (
        "#!/usr/bin/env python3\n"
        "import sys, json, os, time\n"
        "argv = sys.argv[1:]\n"
        "dest = None\n"
        "for i, a in enumerate(argv):\n"
        "    if a == '--dest' and i + 1 < len(argv):\n"
        "        dest = argv[i + 1]\n"
    )
    script.write_text(header + body, encoding="utf-8")
    script.chmod(
        script.stat().st_mode
        | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH,
    )
    return str(script)


def _make_dummy_archive(tmp_path: Path, name: str = "fake.zip") -> Path:
    """Create an empty file we can pass as the archive positional arg.

    The fake extractor never opens it; the wrapper only reads exit code +
    stdout, so the file exists purely to satisfy any deployer that logs
    argv or expects a real path.
    """
    p = tmp_path / name
    p.write_bytes(b"")
    return p


def _make_dest(tmp_path: Path, name: str = "dest") -> Path:
    """Fresh 0o700 destination dir for the extractor."""
    d = tmp_path / name
    d.mkdir(mode=0o700)
    return d


# ---------------------------------------------------------------------------
# Success branches
# ---------------------------------------------------------------------------


class TestExtractArchiveSuccess:
    def test_exit_0_with_extracted_paths_returns_ok(self, tmp_path):
        """The canonical happy path: exit 0 + manifest carrying two
        entries → ok=True, absolute paths joined from dest + name."""
        extractor = _write_fake_extractor(
            tmp_path,
            "print(json.dumps({\n"
            "    'verdict': 'extracted',\n"
            "    'archive': argv[0],\n"
            "    'dest': dest,\n"
            "    'reject_reason': None,\n"
            "    'extracted': [\n"
            "        {'name': 'hello.txt', 'size': 5, 'type': 'text'},\n"
            "        {'name': 'nested/inner.pdf', 'size': 12, 'type': 'pdf'},\n"
            "    ],\n"
            "    'skipped': [],\n"
            "    'totals': {'extracted': 2, 'skipped': 0}\n"
            "}))\n"
            "sys.exit(0)\n",
        )
        archive = _make_dummy_archive(tmp_path)
        dest = _make_dest(tmp_path)
        result = extract_archive(archive, extractor, dest, timeout_seconds=10)
        assert result.ok is True
        assert result.error is None
        assert result.reject_reason is None
        assert result.skipped_count == 0
        # Paths anchored on the dest that the manifest reported.
        assert result.extracted_paths == [
            os.path.join(str(dest), "hello.txt"),
            os.path.join(str(dest), "nested/inner.pdf"),
        ]

    def test_exit_0_with_empty_extracted_still_ok(self, tmp_path):
        """Safe-unzip returns 0 when EVERYTHING was skipped — the caller
        turns this into an "empty archive" notice, not a failure."""
        extractor = _write_fake_extractor(
            tmp_path,
            "print(json.dumps({\n"
            "    'verdict': 'extracted',\n"
            "    'archive': argv[0],\n"
            "    'dest': dest,\n"
            "    'reject_reason': None,\n"
            "    'extracted': [],\n"
            "    'skipped': [\n"
            "        {'name': 'evil.exe', 'reason': 'extension_not_allowed'},\n"
            "    ],\n"
            "    'totals': {'extracted': 0, 'skipped': 1}\n"
            "}))\n"
            "sys.exit(0)\n",
        )
        archive = _make_dummy_archive(tmp_path)
        dest = _make_dest(tmp_path)
        result = extract_archive(archive, extractor, dest, timeout_seconds=10)
        assert result.ok is True
        assert result.extracted_paths == []
        assert result.skipped_count == 1
        assert result.error is None

    def test_paths_use_manifest_dest_when_dest_is_under_caller(self, tmp_path):
        """A deployer's extractor may rewrite --dest into a subdir of the
        caller's dest (hash-into-subdir pattern). The wrapper anchors on
        ``manifest["dest"]`` for path building, but the containment check
        (fix for HIGH-2 / CRIT-1 layer b) requires the manifest dest be
        under the caller's dest_dir. Sub-dest → allowed, paths anchored
        on the manifest dest so they actually point at the files on disk.
        """
        dest = _make_dest(tmp_path)
        sub_dest = dest / "hash-subdir"
        sub_dest.mkdir(mode=0o700)
        rewritten = str(sub_dest)
        extractor = _write_fake_extractor(
            tmp_path,
            "print(json.dumps({\n"
            "    'verdict': 'extracted',\n"
            "    'dest': %r,\n"
            "    'reject_reason': None,\n"
            "    'extracted': [{'name': 'a.txt', 'size': 1, 'type': 'text'}],\n"
            "    'skipped': []\n"
            "}))\n"
            "sys.exit(0)\n" % rewritten,
        )
        archive = _make_dummy_archive(tmp_path)
        # Materialize the extracted file so realpath containment succeeds.
        (sub_dest / "a.txt").write_text("x", encoding="utf-8")
        result = extract_archive(archive, extractor, dest, timeout_seconds=10)
        assert result.ok is True
        assert result.extracted_paths == [
            os.path.join(rewritten, "a.txt"),
        ]

    def test_lying_dest_outside_caller_treated_as_bad_manifest(self, tmp_path):
        """A compromised extractor that rewrites ``dest`` to somewhere
        OUTSIDE the caller's dest_dir (e.g. ``/etc``) is a critical
        containment violation. Fix for CRIT-1 layer (b) / HIGH-2:
        surfaces as ``bad_manifest`` — no paths escape, generic
        couldn't-open notice at the caller.
        """
        dest = _make_dest(tmp_path)
        lying = "/etc"
        extractor = _write_fake_extractor(
            tmp_path,
            "print(json.dumps({\n"
            "    'verdict': 'extracted',\n"
            "    'dest': %r,\n"
            "    'reject_reason': None,\n"
            "    'extracted': [{'name': 'passwd', 'size': 1, 'type': 'text'}],\n"
            "    'skipped': []\n"
            "}))\n"
            "sys.exit(0)\n" % lying,
        )
        archive = _make_dummy_archive(tmp_path)
        result = extract_archive(archive, extractor, dest, timeout_seconds=10)
        assert result.ok is False
        assert result.error == "bad_manifest"
        assert result.extracted_paths == []


# ---------------------------------------------------------------------------
# Rejection / failure branches
# ---------------------------------------------------------------------------


class TestExtractArchiveRejected:
    def test_exit_77_with_reject_reason_captures_slug(self, tmp_path):
        """Rejection manifest's ``reject_reason`` slug rides through as
        ``result.reject_reason`` so callers CAN log the class if they
        want. Notice mapping intentionally ignores it."""
        extractor = _write_fake_extractor(
            tmp_path,
            "print(json.dumps({\n"
            "    'verdict': 'rejected',\n"
            "    'archive': argv[0],\n"
            "    'dest': dest,\n"
            "    'reject_reason': 'path_traversal',\n"
            "    'extracted': [],\n"
            "    'skipped': [],\n"
            "    'totals': {'extracted': 0, 'skipped': 0}\n"
            "}))\n"
            "sys.exit(77)\n",
        )
        archive = _make_dummy_archive(tmp_path)
        dest = _make_dest(tmp_path)
        result = extract_archive(archive, extractor, dest, timeout_seconds=10)
        assert result.ok is False
        assert result.error == "rejected"
        assert result.reject_reason == "path_traversal"
        assert result.extracted_paths == []

    def test_exit_77_without_parseable_manifest_still_rejected(self, tmp_path):
        """A deployer's extractor that dies before printing JSON still
        exits 77. Wrapper must still return ``error="rejected"`` — the
        rejection is what matters, the reason is diagnostic."""
        extractor = _write_fake_extractor(
            tmp_path,
            "print('not-json')\n"
            "sys.exit(77)\n",
        )
        archive = _make_dummy_archive(tmp_path)
        dest = _make_dest(tmp_path)
        result = extract_archive(archive, extractor, dest, timeout_seconds=10)
        assert result.ok is False
        assert result.error == "rejected"
        assert result.reject_reason is None


class TestExtractArchiveToolError:
    def test_exit_78_reports_tool_error(self, tmp_path):
        extractor = _write_fake_extractor(
            tmp_path,
            "sys.stderr.write('corrupt zip\\n')\n"
            "sys.exit(78)\n",
        )
        archive = _make_dummy_archive(tmp_path)
        dest = _make_dest(tmp_path)
        result = extract_archive(archive, extractor, dest, timeout_seconds=10)
        assert result.ok is False
        assert result.error == "tool_error"
        assert result.extracted_paths == []

    def test_unexpected_exit_code_maps_to_tool_error(self, tmp_path):
        """Anything outside the 0/77/78 contract collapses to
        ``tool_error`` so the user gets *some* notice (never nothing)."""
        extractor = _write_fake_extractor(
            tmp_path,
            "sys.exit(3)\n",
        )
        archive = _make_dummy_archive(tmp_path)
        dest = _make_dest(tmp_path)
        result = extract_archive(archive, extractor, dest, timeout_seconds=10)
        assert result.ok is False
        assert result.error == "tool_error"

    def test_bad_manifest_on_exit_0_is_bad_manifest(self, tmp_path):
        """Exit 0 promises a JSON manifest. Broken JSON is a real bug
        somewhere; surface it as a distinct error class so operators can
        tell it apart from a legit tool_error."""
        extractor = _write_fake_extractor(
            tmp_path,
            "print('not-json-at-all')\n"
            "sys.exit(0)\n",
        )
        archive = _make_dummy_archive(tmp_path)
        dest = _make_dest(tmp_path)
        result = extract_archive(archive, extractor, dest, timeout_seconds=10)
        assert result.ok is False
        assert result.error == "bad_manifest"


# ---------------------------------------------------------------------------
# Timeout / missing binary / defensive
# ---------------------------------------------------------------------------


class TestExtractArchiveTimeout:
    def test_wall_clock_timeout_returns_timeout_error(self, tmp_path):
        """Long-running extractor MUST be killed by the wrapper's
        wall-clock cap. The don't-wedge-dispatch guarantee — mirrors
        transcribe.py's timeout test."""
        extractor = _write_fake_extractor(
            tmp_path,
            "time.sleep(5)\n"
            "sys.exit(0)\n",
        )
        archive = _make_dummy_archive(tmp_path)
        dest = _make_dest(tmp_path)
        result = extract_archive(archive, extractor, dest, timeout_seconds=1)
        assert result.ok is False
        assert result.error == "timeout"

    def test_wrapper_never_raises_on_timeout(self, tmp_path):
        """Belt-and-suspenders — the caller relies on this to avoid
        wedging the single-threaded dispatch loop."""
        with patch(
            "landline.media.extract.subprocess.run",
            side_effect=subprocess.TimeoutExpired(
                cmd="fake-extractor", timeout=1,
            ),
        ):
            archive = _make_dummy_archive(tmp_path)
            dest = _make_dest(tmp_path)
            # No pytest.raises — just call and check the return shape.
            result = extract_archive(
                archive, "/nowhere/fake", dest, timeout_seconds=1,
            )
        assert isinstance(result, ExtractionResult)
        assert result.error == "timeout"


class TestExtractArchiveMissingBinary:
    def test_missing_binary_reports_extractor_missing(self, tmp_path):
        archive = _make_dummy_archive(tmp_path)
        dest = _make_dest(tmp_path)
        result = extract_archive(
            archive, "/definitely/not/a/real/binary", dest,
            timeout_seconds=10,
        )
        assert result.ok is False
        assert result.error == "extractor_missing"


class TestExtractArchiveNeverRaises:
    def test_unknown_exception_collapses_to_unknown_error(self, tmp_path):
        """Any weird runtime exception on spawn (PermissionError, OSError
        other than FileNotFoundError, etc.) collapses to error='unknown'
        so the caller can still send a notice + advance the cursor."""
        archive = _make_dummy_archive(tmp_path)
        dest = _make_dest(tmp_path)
        with patch(
            "landline.media.extract.subprocess.run",
            side_effect=PermissionError("no exec bit"),
        ):
            result = extract_archive(
                archive, "/nowhere/fake", dest, timeout_seconds=10,
            )
        assert result.ok is False
        assert result.error == "unknown"


# ---------------------------------------------------------------------------
# Privacy invariant
# ---------------------------------------------------------------------------


class TestExtractArchivePrivacy:
    """Wrapper MUST NOT log per-entry names or the archive filename. Only
    metadata (elapsed, exit code, counts, error class)."""

    def test_success_log_carries_only_counts_not_names(self, tmp_path):
        secret_name = "PRIVATE_MEDICAL_XSecret.pdf"
        extractor = _write_fake_extractor(
            tmp_path,
            "print(json.dumps({\n"
            "    'dest': dest,\n"
            "    'extracted': [{'name': %r, 'size': 1, 'type': 'pdf'}],\n"
            "    'skipped': []\n"
            "}))\n"
            "sys.exit(0)\n" % secret_name,
        )
        archive = _make_dummy_archive(tmp_path, name="private.zip")
        dest = _make_dest(tmp_path)
        messages: list = []
        with patch(
            "landline.media.extract.log",
            side_effect=lambda m: messages.append(m),
        ):
            extract_archive(archive, extractor, dest, timeout_seconds=10)
        for msg in messages:
            assert secret_name not in msg
            assert "XSecret" not in msg
            assert "private.zip" not in msg

    def test_rejection_log_carries_only_reason_slug(self, tmp_path):
        secret_name = "confidential_XSecret2.pdf"
        extractor = _write_fake_extractor(
            tmp_path,
            "print(json.dumps({\n"
            "    'reject_reason': 'declared_size_bomb',\n"
            "    'extracted': [{'name': %r, 'size': 1, 'type': 'pdf'}],\n"
            "    'skipped': []\n"
            "}))\n"
            "sys.exit(77)\n" % secret_name,
        )
        archive = _make_dummy_archive(tmp_path, name="hostile.zip")
        dest = _make_dest(tmp_path)
        messages: list = []
        with patch(
            "landline.media.extract.log",
            side_effect=lambda m: messages.append(m),
        ):
            extract_archive(archive, extractor, dest, timeout_seconds=10)
        for msg in messages:
            assert secret_name not in msg
            assert "XSecret2" not in msg
            assert "hostile.zip" not in msg


# ---------------------------------------------------------------------------
# Containment + control-char defenses in _paths_from_manifest
# ---------------------------------------------------------------------------


class TestPathsFromManifestContainment:
    """Fix for CRIT-1 layer (b) / HIGH-2 / MED (containment).

    Wrapper NEVER trusts the extractor's manifest — every per-entry
    path is realpath'd and checked against the CALLER's dest_dir.
    Entries that escape are dropped (metadata-only log); a lying
    manifest ``dest`` collapses the whole thing to ``bad_manifest``.
    """

    def test_absolute_path_name_dropped(self, tmp_path):
        """A rogue manifest entry with ``name="/etc/passwd"`` must not
        surface a path at ``/etc/passwd`` — the joined path escapes
        the caller's dest, containment drops it silently."""
        from landline.media.extract import _paths_from_manifest
        dest = _make_dest(tmp_path)
        manifest = {
            "dest": str(dest),
            "extracted": [
                {"name": "hello.txt", "size": 5},
                {"name": "/etc/passwd", "size": 1},
            ],
            "skipped": [],
        }
        (dest / "hello.txt").write_text("x", encoding="utf-8")
        paths = _paths_from_manifest(manifest, dest)
        assert paths is not None
        # Only the legit entry survives.
        assert paths == [os.path.join(str(dest), "hello.txt")]

    def test_dotdot_traversal_name_dropped(self, tmp_path):
        """``name="../../etc/passwd"`` joins to somewhere outside dest;
        realpath containment drops it."""
        from landline.media.extract import _paths_from_manifest
        dest = _make_dest(tmp_path)
        manifest = {
            "dest": str(dest),
            "extracted": [
                {"name": "safe.txt", "size": 3},
                {"name": "../../etc/passwd", "size": 1},
                {"name": "../outside.txt", "size": 1},
            ],
            "skipped": [],
        }
        (dest / "safe.txt").write_text("ok", encoding="utf-8")
        paths = _paths_from_manifest(manifest, dest)
        assert paths is not None
        assert paths == [os.path.join(str(dest), "safe.txt")]

    def test_control_char_name_dropped(self, tmp_path):
        """A name with an embedded ``\\n`` (the CRIT-1 exploit vector)
        is dropped by the wrapper even if it somehow bypasses safe-unzip
        layer (a). Also covers ``\\r``, ``\\t``, and DEL."""
        from landline.media.extract import _paths_from_manifest
        dest = _make_dest(tmp_path)
        (dest / "clean.txt").write_text("x", encoding="utf-8")
        # Materialize the exact filenames so realpath containment can't
        # fail for reasons OTHER than the control-char guard.
        try:
            (dest / "inner\nname.txt").write_text("y", encoding="utf-8")
        except (OSError, ValueError):
            # macOS refuses `\n` on APFS; that's fine — the join step
            # still contains the newline, and _has_control_char fires
            # on the JOINED path before disk touch.
            pass
        manifest = {
            "dest": str(dest),
            "extracted": [
                {"name": "clean.txt", "size": 1},
                {"name": "inner\nname.txt", "size": 1},
                {"name": "tab\there.txt", "size": 1},
                {"name": "cr\rhere.txt", "size": 1},
                {"name": "del\x7fhere.txt", "size": 1},
            ],
            "skipped": [],
        }
        paths = _paths_from_manifest(manifest, dest)
        assert paths is not None
        assert paths == [os.path.join(str(dest), "clean.txt")]

    def test_lying_manifest_dest_returns_none(self, tmp_path):
        """If ``manifest["dest"]`` is not under the caller's dest_dir
        (e.g. ``/etc`` or a sibling tmpdir), the whole thing collapses
        to ``None`` — caller maps to ``bad_manifest``."""
        from landline.media.extract import _paths_from_manifest
        dest = _make_dest(tmp_path)
        sibling = _make_dest(tmp_path, name="sibling")
        manifest = {
            "dest": str(sibling),
            "extracted": [
                {"name": "a.txt", "size": 1},
            ],
            "skipped": [],
        }
        paths = _paths_from_manifest(manifest, dest)
        assert paths is None

    def test_lying_manifest_dest_root_returns_none(self, tmp_path):
        """The obvious hostile case: manifest reports ``dest="/etc"``."""
        from landline.media.extract import _paths_from_manifest
        dest = _make_dest(tmp_path)
        manifest = {
            "dest": "/etc",
            "extracted": [
                {"name": "passwd", "size": 1},
            ],
            "skipped": [],
        }
        paths = _paths_from_manifest(manifest, dest)
        assert paths is None

    def test_empty_manifest_dest_returns_empty_paths(self, tmp_path):
        """Missing dest is a defensive edge — return no paths but do NOT
        crash. Caller's success branch treats zero paths as "empty
        extraction"."""
        from landline.media.extract import _paths_from_manifest
        dest = _make_dest(tmp_path)
        paths = _paths_from_manifest({"dest": "", "extracted": []}, dest)
        assert paths == []
