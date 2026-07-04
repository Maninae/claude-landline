"""Tests for landline.telegram_download — Cluster 1 (generalized download +
safe-basename sanitizer)."""

import os
import stat
from unittest.mock import MagicMock, patch

from landline.config import DOCUMENT_ALLOWED_EXTENSIONS, TELEGRAM_IMAGE_DIR
from landline.telegram_download import _safe_basename, download_file


class TestSafeBasename:
    """Path-traversal and control-char sanitizer used by document ingestion."""

    def test_strips_path_traversal(self):
        # Basename strips leading path segments; the .pdf suffix is allowed.
        assert _safe_basename("../../etc/passwd", frozenset({".pdf"})) is None
        assert (
            _safe_basename("../../report.pdf", frozenset({".pdf"}))
            == "report.pdf"
        )

    def test_lowercases_extension(self):
        assert (
            _safe_basename("report.PDF", frozenset({".pdf"})) == "report.pdf"
        )

    def test_rejects_dot_and_dotdot(self):
        assert _safe_basename(".", frozenset({".pdf"})) is None
        assert _safe_basename("..", frozenset({".pdf"})) is None

    def test_rejects_nul(self):
        assert _safe_basename("rep\x00ort.pdf", frozenset({".pdf"})) is None

    def test_rejects_disallowed_extension(self):
        assert _safe_basename("malware.exe", frozenset({".pdf"})) is None

    def test_rejects_too_long_name(self):
        long_name = "a" * 300 + ".pdf"
        assert _safe_basename(long_name, frozenset({".pdf"})) is None

    def test_control_chars_stripped_from_stem(self):
        # \x01\x02.pdf → stem becomes empty → replaced with 'file'
        assert (
            _safe_basename("\x01\x02.pdf", frozenset({".pdf"}))
            == "file.pdf"
        )

    def test_control_chars_stripped_but_stem_survives(self):
        # 'rep\x01ort.pdf' → stem 'report' survives
        assert (
            _safe_basename("rep\x01ort.pdf", frozenset({".pdf"}))
            == "report.pdf"
        )

    def test_del_stripped_from_stem(self):
        """PIN: finding #6 — DEL (0x7F) was preserved by the prior
        ``ord(ch) >= 0x20`` gate. Must now be stripped."""
        assert (
            _safe_basename("\x7fname.pdf", frozenset({".pdf"}))
            == "name.pdf"
        )

    def test_rtl_override_stripped_from_stem(self):
        """PIN: finding #6 — U+202E RTL-override flips subsequent chars'
        display direction and could disguise the extension in the daemon
        log tail or in the ``[document: <name>, ...]`` Claude prompt
        fragment. Must be stripped (category ``Cf``)."""
        # "‮fdp.evil" would render right-to-left as "live.pdf".
        raw = "safe‮fdp.pdf"
        result = _safe_basename(raw, frozenset({".pdf"}))
        assert result is not None
        assert "‮" not in result
        assert result == "safefdp.pdf"

    def test_bom_zwsp_stripped_from_stem(self):
        """PIN: finding #6 — Unicode format-class chars (BOM U+FEFF,
        ZWSP U+200B) sneaked through the old byte-value gate. Must be
        stripped (category ``Cf``)."""
        raw = "﻿name​.pdf"
        assert (
            _safe_basename(raw, frozenset({".pdf"}))
            == "name.pdf"
        )

    def test_line_separator_stripped_from_stem(self):
        """PIN: finding #6 — U+2028 LINE SEPARATOR is treated as a
        newline by many parsers and could inject an apparent new line
        into the ``[document: ...]`` prompt fragment. Category ``Zl`` is
        also caught by ``str.isprintable()``. Must be stripped."""
        raw = "line break.pdf"
        assert (
            _safe_basename(raw, frozenset({".pdf"}))
            == "linebreak.pdf"
        )

    def test_c1_control_stripped_from_stem(self):
        """PIN: finding #6 — C1 controls (0x80–0x9F) passed the old
        ``ord(ch) >= 0x20`` gate. Category ``Cc``, must be stripped."""
        raw = "rep\x85ort.pdf"  # 0x85 = NEL (Next Line, C1 control)
        assert (
            _safe_basename(raw, frozenset({".pdf"}))
            == "report.pdf"
        )

    def test_none_input_returns_none(self):
        assert _safe_basename(None, frozenset({".pdf"})) is None

    def test_empty_input_returns_none(self):
        assert _safe_basename("", frozenset({".pdf"})) is None

    def test_extension_gating_uses_supplied_set(self):
        # Prove the allow-list is honored — .md accepted with the wider set
        # (mirrors DOCUMENT_ALLOWED_EXTENSIONS) and rejected under a narrow one.
        assert (
            _safe_basename("notes.md", DOCUMENT_ALLOWED_EXTENSIONS)
            == "notes.md"
        )
        assert _safe_basename("notes.md", frozenset({".pdf"})) is None


class TestDownloadFileTargetDir:
    """Regression: photo path unchanged; new target_dir routes documents."""

    def _make_good_urlopen(self, body: bytes = b"payload"):
        """urlopen mock that yields ``body`` in one chunk then EOF."""
        resp = MagicMock()
        # First read returns body, next returns b"" (EOF)
        resp.read.side_effect = [body, b""]
        resp.__enter__ = MagicMock(return_value=resp)
        resp.__exit__ = MagicMock(return_value=False)
        return resp

    def test_default_target_dir_is_image_cache(self, tmp_path, monkeypatch):
        """No target_dir → writes to TELEGRAM_IMAGE_DIR (photo path)."""
        image_dir = tmp_path / "images"
        monkeypatch.setattr(
            "landline.telegram_download.TELEGRAM_IMAGE_DIR", image_dir,
        )
        good_resp = {
            "ok": True,
            "result": {"file_path": "photos/x.jpg", "file_size": 7},
        }
        with patch(
            "landline.telegram_download.telegram_api", return_value=good_resp,
        ), patch(
            "urllib.request.urlopen",
            return_value=self._make_good_urlopen(b"payload"),
        ):
            local = download_file("tok", "fake-id", "out.jpg")
        assert local is not None
        assert str(image_dir) in local
        assert (image_dir / "out.jpg").exists()

    def test_target_dir_routes_and_dir_is_0700(self, tmp_path):
        """target_dir=<path> → file lands there; dir mode is 0700."""
        target = tmp_path / "files"
        good_resp = {
            "ok": True,
            "result": {"file_path": "docs/x.pdf", "file_size": 7},
        }
        with patch(
            "landline.telegram_download.telegram_api", return_value=good_resp,
        ), patch(
            "urllib.request.urlopen",
            return_value=self._make_good_urlopen(b"payload"),
        ):
            local = download_file(
                "tok", "fake-id", "out.pdf", target_dir=target,
            )
        assert local is not None
        assert (target / "out.pdf").exists()
        mode = stat.S_IMODE(os.stat(str(target)).st_mode)
        assert mode == 0o700

    def test_size_cap_honored(self, tmp_path):
        """size_cap trims download when Telegram reports over-cap size."""
        target = tmp_path / "files"
        # getFile returns a size larger than our per-doc cap.
        over_cap_resp = {
            "ok": True,
            "result": {
                "file_path": "docs/x.pdf",
                "file_size": 99 * 1024 * 1024,
            },
        }
        with patch(
            "landline.telegram_download.telegram_api", return_value=over_cap_resp,
        ):
            local = download_file(
                "tok", "fake-id", "out.pdf",
                target_dir=target, size_cap=10 * 1024 * 1024,
            )
        assert local is None


class TestDownloadFileLogPrivacy:
    """Finding pin (daemon/telegram_download.py:173): the success log
    line MUST NOT include the destination filename — the caller may
    pass a filename derived from a sensitive user-supplied document
    stem (e.g. "20260703_140000_private_medical_records.pdf"). Log the
    directory + byte count only."""

    def _make_good_urlopen(self, body: bytes = b"payload"):
        resp = MagicMock()
        resp.read.side_effect = [body, b""]
        resp.__enter__ = MagicMock(return_value=resp)
        resp.__exit__ = MagicMock(return_value=False)
        return resp

    def test_downloaded_log_does_not_leak_filename(self, tmp_path):
        target = tmp_path / "files"
        sensitive = "20260703_140000_private_medical_records_XPrivateToken.pdf"
        good_resp = {
            "ok": True,
            "result": {"file_path": "docs/x.pdf", "file_size": 7},
        }
        with patch(
            "landline.telegram_download.telegram_api", return_value=good_resp,
        ), patch(
            "urllib.request.urlopen",
            return_value=self._make_good_urlopen(b"payload"),
        ), patch(
            "landline.telegram_download.log",
        ) as mock_log:
            local = download_file(
                "tok", "fake-id", sensitive, target_dir=target,
            )
        assert local is not None
        for call in mock_log.call_args_list:
            for arg in call.args:
                if isinstance(arg, str):
                    assert sensitive not in arg, (
                        f"sensitive filename leaked into download log: {arg!r}"
                    )
                    assert "XPrivateToken" not in arg
