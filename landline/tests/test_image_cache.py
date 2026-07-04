"""Tests for landline.image_cache — Cluster 1 (generalized multi-dir sweep +
single-dir back-compat wrapper)."""

import os
import time

from landline.image_cache import (
    _sweep_dir,
    _sweep_telegram_image_cache,
    sweep_media_caches,
)


def _make_old(path, hours=48):
    path.write_bytes(b"x")
    now = time.time()
    os.utime(str(path), (now - hours * 3600, now - hours * 3600))


def _make_recent(path):
    path.write_bytes(b"y")


class TestBackCompatWrapper:
    """Regression: `_sweep_telegram_image_cache` still sweeps a single dir."""

    def test_single_dir_sweep(self, tmp_path):
        old = tmp_path / "old.jpg"
        recent = tmp_path / "recent.jpg"
        _make_old(old, hours=48)
        _make_recent(recent)
        swept = _sweep_telegram_image_cache(
            image_dir=tmp_path, retention_hours=24,
        )
        assert swept == 1
        assert not old.exists()
        assert recent.exists()


class TestSweepMediaCaches:
    def test_multi_dir_sweep(self, tmp_path):
        d1 = tmp_path / "images"
        d2 = tmp_path / "files"
        d1.mkdir()
        d2.mkdir()

        old_1 = d1 / "old.jpg"
        recent_1 = d1 / "recent.jpg"
        old_2 = d2 / "old.pdf"
        recent_2 = d2 / "recent.pdf"
        _make_old(old_1)
        _make_recent(recent_1)
        _make_old(old_2)
        _make_recent(recent_2)

        total = sweep_media_caches(dirs=(d1, d2), retention_hours=24)
        assert total == 2
        assert not old_1.exists()
        assert not old_2.exists()
        assert recent_1.exists()
        assert recent_2.exists()

    def test_missing_dir_logged_and_skipped(self, tmp_path):
        """A missing directory is a no-op; other dirs still swept."""
        missing = tmp_path / "does-not-exist"
        real = tmp_path / "real"
        real.mkdir()
        old = real / "old.pdf"
        _make_old(old)
        total = sweep_media_caches(dirs=(missing, real), retention_hours=24)
        assert total == 1
        assert not old.exists()

    def test_default_dirs_are_media_cache_dirs(self):
        """The default arg is ``MEDIA_CACHE_DIRS`` — a wiring guard so a
        maintainer accidentally shadowing the default doesn't silently
        skip a cache dir at startup."""
        import inspect
        from landline import config, image_cache
        sig = inspect.signature(image_cache.sweep_media_caches)
        default = sig.parameters["dirs"].default
        assert default == config.MEDIA_CACHE_DIRS

    def test_per_dir_retention_honors_voice_and_file_constants(
        self, tmp_path, monkeypatch,
    ):
        """PIN (findings #3/#5): each cache dir must be swept with its OWN
        retention window. the operator tunes ``TELEGRAM_VOICE_RETENTION_HOURS`` = 1
        (privacy-sensitive raw audio + transcript) or
        ``TELEGRAM_FILE_RETENTION_HOURS`` = 2 (tighter PDF window) — under
        the pre-fix code the sweep silently applied the 24h image default
        to every dir and both knobs were dead config.
        """
        image_dir = tmp_path / "images"
        file_dir = tmp_path / "files"
        voice_dir = tmp_path / "voice"
        for d in (image_dir, file_dir, voice_dir):
            d.mkdir()

        # File in each dir, aged just past that dir's *own* retention.
        # If the sweep used the image default (24h), the voice/file
        # entries — aged 3h/5h — would survive incorrectly.
        old_image = image_dir / "old.jpg"
        old_file = file_dir / "old.pdf"
        old_voice = voice_dir / "old.txt"
        _make_old(old_image, hours=48)   # >24h — swept under image=24
        _make_old(old_file, hours=5)     # >2h  — swept under file=2
        _make_old(old_voice, hours=3)    # >1h  — swept under voice=1

        # Fresh entry per dir — must survive.
        recent_image = image_dir / "recent.jpg"
        recent_file = file_dir / "recent.pdf"
        recent_voice = voice_dir / "recent.txt"
        for p in (recent_image, recent_file, recent_voice):
            _make_recent(p)

        # Tune retention to the operator's tight-privacy scenario.
        monkeypatch.setattr(
            "landline.image_cache.MEDIA_CACHE_RETENTION_HOURS",
            {image_dir: 24, file_dir: 2, voice_dir: 1},
        )

        total = sweep_media_caches(dirs=(image_dir, file_dir, voice_dir))
        assert total == 3
        assert not old_image.exists()
        assert not old_file.exists(), (
            "TELEGRAM_FILE_RETENTION_HOURS knob is not being honored"
        )
        assert not old_voice.exists(), (
            "TELEGRAM_VOICE_RETENTION_HOURS knob is not being honored"
        )
        assert recent_image.exists()
        assert recent_file.exists()
        assert recent_voice.exists()

    def test_config_map_covers_all_media_cache_dirs(self):
        """The retention map must have an entry for every cache dir so no
        dir silently falls back to the image default."""
        from landline import config
        for d in config.MEDIA_CACHE_DIRS:
            assert d in config.MEDIA_CACHE_RETENTION_HOURS, (
                f"{d} has no retention entry in "
                "config.MEDIA_CACHE_RETENTION_HOURS — the voice/file "
                "retention constants will be silently ignored"
            )


class TestSweepDir:
    def test_missing_dir_returns_zero(self, tmp_path):
        missing = tmp_path / "nope"
        assert _sweep_dir(missing, retention_hours=24) == 0

    def test_recent_files_preserved(self, tmp_path):
        recent = tmp_path / "r.pdf"
        _make_recent(recent)
        assert _sweep_dir(tmp_path, retention_hours=24) == 0
        assert recent.exists()

    def test_stale_whisper_subdir_recursively_removed(self, tmp_path):
        """PIN: findings #2 and #5 — whisper mkdtemp subdirs are cleaned
        up by the startup sweep. Under the prior ``if not entry.is_file():
        continue`` guard, stale whisper_XXX/ subdirs (containing a
        plaintext transcript) accumulated forever after SIGKILL /
        launchd force-kill / power loss bypassed the ``finally:
        shutil.rmtree`` in ``voice_transcribe.transcribe_file``. Now
        stale subdirs are ``rmtree``-ed.
        """
        # Simulate the whisper tmpdir with a stale transcript.
        whisper_dir = tmp_path / "whisper_abcdef"
        whisper_dir.mkdir()
        transcript = whisper_dir / "audio.txt"
        transcript.write_text("operator private voice content")
        # Age BOTH the file and the dir past retention. Dir mtime is
        # what gates removal in the new sweep path.
        aged = time.time() - 48 * 3600
        os.utime(str(transcript), (aged, aged))
        os.utime(str(whisper_dir), (aged, aged))

        # Also drop a recent whisper dir — must survive the sweep.
        fresh_dir = tmp_path / "whisper_fresh"
        fresh_dir.mkdir()
        (fresh_dir / "audio.txt").write_text("recent")

        swept = _sweep_dir(tmp_path, retention_hours=24)
        assert swept == 1
        assert not whisper_dir.exists(), (
            "stale whisper_ subdir must be removed"
        )
        assert not transcript.exists(), (
            "stale transcript must be removed with its subdir"
        )
        assert fresh_dir.exists(), (
            "recent whisper_ subdir must be preserved"
        )

    def test_per_entry_error_log_never_leaks_filename_or_path(
        self, tmp_path, monkeypatch,
    ):
        """Finding #3 regression: the per-entry ``except`` in ``_sweep_dir``
        must NEVER emit ``entry.name`` (sanitized-but-still-sensitive
        filenames like ``private_medical_records.pdf`` or
        ``<ts>_voice_note.oga``) OR the exception's ``__str__`` (an
        ``OSError`` renders as e.g.
        ``[Errno 13] Permission denied: '/.../<ts>_<name>'`` — embedding
        the FULL absolute path in the daemon log). Both would land in
        the 25MB rotating ``daemon.log`` and outlive the 0700 cache dir
        — the exact leak surface ``document_handler`` / ``voice_handler``
        / ``telegram_download`` were carefully hardened against.
        """
        # Force ``entry.unlink`` to raise a PermissionError whose ``str()``
        # embeds the full absolute path — matches the real OSError shape.
        sensitive_name = "20260703_141522_private_medical_records.pdf"
        target = tmp_path / sensitive_name
        target.write_bytes(b"x")
        aged = time.time() - 48 * 3600
        os.utime(str(target), (aged, aged))

        captured: list = []
        monkeypatch.setattr(
            "landline.image_cache.log",
            lambda msg: captured.append(msg),
        )

        original_unlink = target.__class__.unlink

        def raising_unlink(self, *args, **kwargs):
            if self.name == sensitive_name:
                raise PermissionError(13, "Permission denied", str(self))
            return original_unlink(self, *args, **kwargs)

        monkeypatch.setattr(target.__class__, "unlink", raising_unlink)

        _sweep_dir(tmp_path, retention_hours=24)

        # Assert the sanitized-but-sensitive basename NEVER appears in
        # any emitted log line…
        for line in captured:
            assert sensitive_name not in line, (
                "Media cache sweep leaked entry.name into the daemon log: "
                + repr(line)
            )
            # …and the FULL absolute path (which the OSError.__str__
            # rendering would embed) never appears either.
            assert str(target) not in line, (
                "Media cache sweep leaked the full path into the daemon "
                "log: " + repr(line)
            )
            assert "private_medical" not in line
        # At least one line must record that a per-entry failure happened
        # (observability of failures matters — we just want it metadata-only).
        assert any("failed to remove" in line for line in captured), (
            "Per-entry error must still be observably logged (metadata only)"
        )

    def test_symlink_not_followed(self, tmp_path):
        """Sweep must never follow a symlink out of the cache dir."""
        outside = tmp_path.parent / "outside_dir"
        outside.mkdir(exist_ok=True)
        outside_file = outside / "keep.pdf"
        outside_file.write_bytes(b"keep")
        # Age the outside file so a naive follow would delete it.
        aged = time.time() - 48 * 3600
        os.utime(str(outside_file), (aged, aged))

        cache = tmp_path / "cache"
        cache.mkdir()
        link = cache / "sym"
        os.symlink(str(outside), str(link))
        os.utime(str(link), (aged, aged), follow_symlinks=False)

        _sweep_dir(cache, retention_hours=24)
        # The outside file must NOT have been removed.
        assert outside_file.exists()
