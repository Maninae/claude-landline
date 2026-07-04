"""Tests for landline.inject — inject queue draining (two-phase commit)."""

import json
from pathlib import Path

import pytest

from landline.inject import commit_inject_queue, drain_inject_queue


class TestDrainInjectQueue:
    def test_empty_when_dir_missing(self, tmp_path):
        text, paths = drain_inject_queue(tmp_path / "nonexistent")
        assert text == ""
        assert paths == []

    def test_empty_when_no_json_files(self, tmp_path):
        queue_dir = tmp_path / "queue"
        queue_dir.mkdir()
        text, paths = drain_inject_queue(queue_dir)
        assert text == ""
        assert paths == []

    def test_single_report(self, tmp_path):
        queue_dir = tmp_path / "queue"
        queue_dir.mkdir()
        data = {"label": "morning-brief", "content": "Good morning, here's your brief."}
        (queue_dir / "20260510T080000.json").write_text(json.dumps(data))
        text, paths = drain_inject_queue(queue_dir)
        assert "morning-brief" in text
        assert "Good morning" in text
        assert len(paths) == 1

    def test_does_not_delete_files_on_drain(self, tmp_path):
        """Two-phase commit: drain MUST NOT unlink well-formed files.
        Otherwise a crashed Claude call drops the report on the floor."""
        queue_dir = tmp_path / "queue"
        queue_dir.mkdir()
        data = {"label": "test", "content": "content"}
        item = queue_dir / "item.json"
        item.write_text(json.dumps(data))
        text, paths = drain_inject_queue(queue_dir)
        assert item.exists(), "drain must NOT delete files (commit does)"
        assert paths == [item]

    def test_commit_deletes_files(self, tmp_path):
        queue_dir = tmp_path / "queue"
        queue_dir.mkdir()
        data = {"label": "test", "content": "content"}
        item = queue_dir / "item.json"
        item.write_text(json.dumps(data))
        _, paths = drain_inject_queue(queue_dir)
        commit_inject_queue(paths)
        assert list(queue_dir.glob("*.json")) == []

    def test_multiple_reports_sorted(self, tmp_path):
        queue_dir = tmp_path / "queue"
        queue_dir.mkdir()
        for i, label in enumerate(["alpha", "beta", "gamma"]):
            data = {"label": label, "content": f"content-{label}"}
            (queue_dir / f"2026051{i}T100000.json").write_text(json.dumps(data))
        text, _ = drain_inject_queue(queue_dir)
        assert "alpha" in text
        assert "beta" in text
        assert "gamma" in text
        alpha_pos = text.index("alpha")
        beta_pos = text.index("beta")
        gamma_pos = text.index("gamma")
        assert alpha_pos < beta_pos < gamma_pos

    def test_parses_timestamp_from_filename(self, tmp_path):
        queue_dir = tmp_path / "queue"
        queue_dir.mkdir()
        data = {"label": "brief", "content": "stuff"}
        (queue_dir / "20260510T143000.json").write_text(json.dumps(data))
        text, _ = drain_inject_queue(queue_dir)
        assert "14:30" in text

    def test_empty_content_still_in_summary(self, tmp_path):
        queue_dir = tmp_path / "queue"
        queue_dir.mkdir()
        data = {"label": "empty-report", "content": ""}
        (queue_dir / "item.json").write_text(json.dumps(data))
        text, _ = drain_inject_queue(queue_dir)
        assert "empty-report" in text

    def test_missing_label_defaults_to_cron(self, tmp_path):
        queue_dir = tmp_path / "queue"
        queue_dir.mkdir()
        data = {"content": "some content"}
        (queue_dir / "item.json").write_text(json.dumps(data))
        text, _ = drain_inject_queue(queue_dir)
        assert "cron" in text

    def test_bad_json_file_skipped(self, tmp_path):
        queue_dir = tmp_path / "queue"
        queue_dir.mkdir()
        (queue_dir / "bad.json").write_text("not json {{{")
        good = queue_dir / "good.json"
        good.write_text(json.dumps({"label": "good", "content": "ok"}))
        text, paths = drain_inject_queue(queue_dir)
        assert "good" in text
        # Only the good file is returned for commit; the corrupt one was
        # unlinked inline by drain.
        assert paths == [good]

    def test_content_wrapped_in_xml_tags(self, tmp_path):
        queue_dir = tmp_path / "queue"
        queue_dir.mkdir()
        data = {"label": "report", "content": "important data"}
        (queue_dir / "item.json").write_text(json.dumps(data))
        text, _ = drain_inject_queue(queue_dir)
        assert "<injected-report" in text
        assert "</injected-report>" in text
        assert "important data" in text

    def test_header_format(self, tmp_path):
        queue_dir = tmp_path / "queue"
        queue_dir.mkdir()
        data = {"label": "brief", "content": "text"}
        (queue_dir / "item.json").write_text(json.dumps(data))
        text, _ = drain_inject_queue(queue_dir)
        from landline.config import USER_NAME
        assert text.startswith(f"[Reports delivered to {USER_NAME}")

    def test_whitespace_only_content_not_in_blocks(self, tmp_path):
        queue_dir = tmp_path / "queue"
        queue_dir.mkdir()
        data = {"label": "empty", "content": "   \n  "}
        (queue_dir / "item.json").write_text(json.dumps(data))
        text, _ = drain_inject_queue(queue_dir)
        assert "empty" in text
        assert "<injected-report" not in text

    def test_filename_without_timestamp_omits_time(self, tmp_path):
        """A stem like 'item' (too short / no T) should produce no time suffix."""
        queue_dir = tmp_path / "queue"
        queue_dir.mkdir()
        data = {"label": "anon", "content": "stuff"}
        (queue_dir / "item.json").write_text(json.dumps(data))
        text, _ = drain_inject_queue(queue_dir)
        # Header contains the label but no parenthesized time.
        assert "anon" in text
        assert "anon (" not in text

    def test_invalid_timestamp_in_filename_skipped_gracefully(self, tmp_path):
        """Stem matches length+T but contains non-numeric digits — fall through, no crash."""
        queue_dir = tmp_path / "queue"
        queue_dir.mkdir()
        data = {"label": "weird", "content": "x"}
        # Position 8 = 'T', positions 9-12 are non-numeric → int() raises, caught.
        (queue_dir / "AAAAAAAATXXXXXX.json").write_text(json.dumps(data))
        text, _ = drain_inject_queue(queue_dir)
        assert "weird" in text

    def test_bad_file_unlinked_inline(self, tmp_path):
        """Corrupt files must be unlinked by drain itself so they don't
        keep failing on every drain.  They are NOT included in the
        consumed_paths list returned to the caller."""
        queue_dir = tmp_path / "queue"
        queue_dir.mkdir()
        bad_path = queue_dir / "bad.json"
        bad_path.write_text("not json {{{")
        _, paths = drain_inject_queue(queue_dir)
        assert not bad_path.exists()
        assert bad_path not in paths

    def test_header_lists_all_labels(self, tmp_path):
        """Header summary should contain every label in the queue."""
        queue_dir = tmp_path / "queue"
        queue_dir.mkdir()
        for label in ["alpha", "beta"]:
            (queue_dir / f"{label}.json").write_text(
                json.dumps({"label": label, "content": "c"})
            )
        text, _ = drain_inject_queue(queue_dir)
        first_line = text.splitlines()[0]
        assert "alpha" in first_line
        assert "beta" in first_line

    def test_returns_empty_when_all_items_corrupt(self, tmp_path):
        """If every file is corrupt, summary_parts stays empty and we return ('', [])."""
        queue_dir = tmp_path / "queue"
        queue_dir.mkdir()
        (queue_dir / "a.json").write_text("garbage 1")
        (queue_dir / "b.json").write_text("garbage 2")
        text, paths = drain_inject_queue(queue_dir)
        assert text == ""
        assert paths == []

    def test_label_appears_in_xml_attribute(self, tmp_path):
        """Content block must have name attribute matching the label."""
        from datetime import datetime
        from landline.config import INJECT_TIMESTAMP_FORMAT, TIMEZONE
        queue_dir = tmp_path / "queue"
        queue_dir.mkdir()
        data = {"label": "morning-brief", "content": "good morning"}
        (queue_dir / "20260510T080000.json").write_text(json.dumps(data))
        text, _ = drain_inject_queue(queue_dir)
        assert 'name="morning-brief"' in text
        # time attribute uses "HH:MM %Z" — the tz abbreviation is host-
        # dependent, so build the expected literal dynamically.
        parsed = datetime.strptime("20260510T080000", INJECT_TIMESTAMP_FORMAT).replace(
            tzinfo=TIMEZONE,
        )
        expected_time = parsed.strftime("%H:%M %Z")
        assert f'time="{expected_time}"' in text

    def test_oserror_on_read_does_not_unlink(self, tmp_path, monkeypatch):
        """A transient OSError reading a queue file (e.g. EACCES/EIO) must NOT
        unlink the file — otherwise a good morning brief / report disappears
        on a permission blip. The file should still be present after drain,
        and not included in consumed_paths (since drain couldn't read it)."""
        queue_dir = tmp_path / "queue"
        queue_dir.mkdir()
        # A perfectly good report file — its only crime is being unreadable
        # at this exact moment.
        good_data = {"label": "morning-brief", "content": "Good morning."}
        good_path = queue_dir / "20260615T070000.json"
        good_path.write_text(json.dumps(good_data))

        # Force Path.read_text on this file to raise OSError.
        real_read_text = Path.read_text

        def fake_read_text(self, *args, **kwargs):
            if self == good_path:
                raise OSError("simulated EACCES")
            return real_read_text(self, *args, **kwargs)

        monkeypatch.setattr(Path, "read_text", fake_read_text)

        text, paths = drain_inject_queue(queue_dir)

        # File MUST still be on disk for the next drain to retry.
        assert good_path.exists(), \
            "OSError on read must NOT unlink — the file is good, just unreadable now"
        # Nothing was successfully read this drain.
        assert paths == []
        assert text == ""

    def test_malformed_json_is_unlinked(self, tmp_path):
        """A file that is genuinely bad JSON IS unlinked — same behavior as
        before. Pairs with the OSError test to verify the split: malformed
        deletes, transient I/O does not."""
        queue_dir = tmp_path / "queue"
        queue_dir.mkdir()
        bad_path = queue_dir / "garbage.json"
        bad_path.write_text("this is not { valid json")

        _, paths = drain_inject_queue(queue_dir)

        assert not bad_path.exists(), \
            "Malformed JSON must be unlinked so it doesn't keep failing"
        assert paths == []

    def test_bad_utf8_bytes_is_unlinked(self, tmp_path):
        """A queue file with invalid UTF-8 bytes raises UnicodeDecodeError on
        read_text — it must be treated as malformed (unlinked), NOT propagated.
        Otherwise it jams the queue and drops every later message on each drain
        (the infinite-reprocess trap). A good batch entry alongside it still
        processes."""
        queue_dir = tmp_path / "queue"
        queue_dir.mkdir()
        bad_path = queue_dir / "a-badbytes.json"
        bad_path.write_bytes(b'\xff\xfe\x00 not valid utf-8 \x80\x81')
        good_path = queue_dir / "b-good.json"
        good_path.write_text(json.dumps({"label": "ok", "content": "hello"}))

        text, paths = drain_inject_queue(queue_dir)

        assert not bad_path.exists(), \
            "Bad-UTF8 file must be unlinked, not left to jam the queue forever"
        assert good_path in paths and "hello" in text
        assert bad_path not in paths

    def test_sort_by_mtime_not_filename(self, tmp_path):
        """Drain must sort by mtime, not filename — proves the producer can
        write any filename and still get correct delivery order. D2 regression."""
        import os
        import time
        queue_dir = tmp_path / "queue"
        queue_dir.mkdir()
        # b-zeta is older (written first) but sorts later by name.
        zeta = queue_dir / "b-zeta.json"
        zeta.write_text(json.dumps({"label": "zeta", "content": "z"}))
        # Force a real mtime gap so the test isn't filesystem-precision dependent.
        os.utime(zeta, (time.time() - 10, time.time() - 10))
        alpha = queue_dir / "a-alpha.json"
        alpha.write_text(json.dumps({"label": "alpha", "content": "a"}))
        text, _ = drain_inject_queue(queue_dir)
        assert text.index("zeta") < text.index("alpha"), \
            "drain must sort by mtime — D2 regression"

    def test_sort_tiebreak_by_name(self, tmp_path, monkeypatch):
        """On mtime tie, fall back to lexicographic name order for determinism.

        Revert-sensitivity: if the sort key is reduced to just ``st_mtime``
        (no ``name`` tiebreak), the stable sort would preserve the glob order,
        and on APFS glob already returns alphabetical entries — so a naive
        same-mtime test passes whether or not the tiebreak is present (the
        filesystem masks the bug). We defend against this by monkeypatching
        ``Path.glob`` to return entries in REVERSE name order, so the test
        depends solely on the sort key. With the tiebreak, the (mtime, name)
        key re-orders to [a, b]; without it, the stable sort preserves the
        reversed glob order [b, a] and the assertion flips. D2 regression."""
        import os
        queue_dir = tmp_path / "queue"
        queue_dir.mkdir()
        # Write 'b' BEFORE 'a' so creation order is non-alphabetical too.
        b = queue_dir / "b.json"
        b.write_text(json.dumps({"label": "b-label", "content": "y"}))
        a = queue_dir / "a.json"
        a.write_text(json.dumps({"label": "a-label", "content": "x"}))
        # Force identical mtimes — the (mtime, name) key must tiebreak by name.
        same = b.stat().st_mtime
        os.utime(a, (same, same))

        # Monkeypatch glob to return reverse-name order so the test is
        # filesystem-independent and TRULY depends on the name tiebreak.
        real_glob = Path.glob

        def reversed_glob(self, pattern):
            return iter(sorted(real_glob(self, pattern), key=lambda p: p.name, reverse=True))

        monkeypatch.setattr(Path, "glob", reversed_glob)

        text, _ = drain_inject_queue(queue_dir)
        assert text.index("a-label") < text.index("b-label"), \
            "name tiebreak missing — sort key reduced to st_mtime only would fail this"

    def test_inject_timestamp_uses_shared_format_constant(self, tmp_path):
        """Consumer-side coupling: the time annotation must render via
        INJECT_TIMESTAMP_FORMAT from landline.config. If inject.py drifts to a
        different format string, this test fails. (Producer-side drift is
        caught by the wave-4 reviewer grep gate, by design.) D2 regression."""
        from datetime import datetime as _dt
        from landline.config import INJECT_TIMESTAMP_FORMAT
        assert INJECT_TIMESTAMP_FORMAT == "%Y%m%dT%H%M%S"
        queue_dir = tmp_path / "queue"
        queue_dir.mkdir()
        now = _dt.now()
        stem = now.strftime(INJECT_TIMESTAMP_FORMAT)
        data = {"label": "brief", "content": "stuff"}
        (queue_dir / f"{stem}.json").write_text(json.dumps(data))
        text, _ = drain_inject_queue(queue_dir)
        assert now.strftime("%H:%M") in text

    def test_inject_bad_timestamp_no_crash(self, tmp_path):
        """Stem passes length guard but fails strptime — no crash, label
        appears, no (HH:MM TIMEZONE) substring. D2 regression."""
        queue_dir = tmp_path / "queue"
        queue_dir.mkdir()
        data = {"label": "weird", "content": "x"}
        # Stem is 15 chars, passes length guard, but strptime raises ValueError.
        (queue_dir / "AAAAAAAATXXXXXX.json").write_text(json.dumps(data))
        text, _ = drain_inject_queue(queue_dir)
        assert "weird" in text
        assert " TIMEZONE)" not in text

    def test_inject_calendar_invalid_stem_no_annotation(self, tmp_path):
        """Stem with structurally valid digits in HH/MM positions but invalid
        calendar values (e.g. month 99). Under magic-index parse:
        stem[8]='T' passes guard, int(stem[9:11])=7, int(stem[11:13])=0 →
        WOULD render '(07:00 TIMEZONE)' silently. Under strptime, the full parse
        validates the calendar and raises ValueError → annotation omitted.

        Revert-sensitivity: this is the LOAD-BEARING regression test for the
        parse change. Reverting daemon/inject.py to the magic-index logic
        causes this test to FAIL because the bogus annotation is rendered.
        Architecture-record §3 invariant 9. D2 regression."""
        queue_dir = tmp_path / "queue"
        queue_dir.mkdir()
        data = {"label": "weird", "content": "x"}
        # Stem '20269999T070000' — month=99/day=99 invalid calendar values.
        # Magic indices land on 'T' at [8] and digits at [9:11]/[11:13] → silent
        # mis-parse renders "07:00 TIMEZONE". strptime validates the calendar.
        (queue_dir / "20269999T070000.json").write_text(json.dumps(data))
        text, _ = drain_inject_queue(queue_dir)
        assert "weird" in text
        assert " TIMEZONE)" not in text, \
            "calendar-invalid stem must NOT render time annotation — magic-index revert would fail here"
        assert "07:00" not in text

    def test_inject_label_prefixed_filename_no_time_annotation(self, tmp_path):
        """A hypothetical label-prefixed producer drops a file whose stem
        has 'T' at position 8 AND digits at positions [9:11]/[11:13], so the
        magic-index parse would silently mis-render label characters as a
        time annotation. Under strptime, stem[:15] fails to parse, so the
        annotation is omitted.

        Revert-sensitivity: stem 'urgent-T2026061T07' chosen so stem[8]='T'
        (the '-T' boundary at offset 7-8) and stem[9:11]='20', stem[11:13]='26'
        — magic-index parse renders '(20:26 TIMEZONE)' from label bytes; strptime
        rejects 'urgent-T2026061' as malformed. Architecture-record §3
        invariant 9. D2 regression pin against magic-index revert."""
        queue_dir = tmp_path / "queue"
        queue_dir.mkdir()
        data = {"label": "weird", "content": "x"}
        # Build a stem where char[8]='T' AND char[9:11], char[11:13] are digits
        # → magic-index parse silently mis-reads label as time. strptime fails.
        # Stem layout (offset:char):  0:u 1:r 2:g 3:e 4:n 5:t 6:- 7:T 8:T 9:2 10:0 11:2 12:6 13:0 14:6
        # → char[8]='T' (guard passes), int('20')=20, int('26')=26 → "20:26 TIMEZONE"
        stem = "urgent-TT2026061"  # 16 chars, slice [:15] = "urgent-TT202606"
        # Drop the trailing char to make stem exactly 15 chars: "urgent-TT202606"
        stem = stem[:15]
        assert len(stem) == 15 and stem[8] == "T", "test fixture invariant"
        assert stem[9:11].isdigit() and stem[11:13].isdigit(), "test fixture invariant"
        (queue_dir / f"{stem}.json").write_text(json.dumps(data))
        text, _ = drain_inject_queue(queue_dir)
        assert "weird" in text
        assert " TIMEZONE)" not in text, \
            "label-prefixed stem with digit-bytes must NOT render time — magic-index revert would mis-parse"

    def test_non_string_content_is_skipped_and_unlinked(self, tmp_path):
        """A queue file whose JSON parses fine but has a non-string ``content``
        (e.g. ``null`` or a list) must be skipped without raising AND its file
        must be unlinked — otherwise the same bad file would be re-processed
        on every drain forever. Valid entries in the same batch must still
        be processed."""
        queue_dir = tmp_path / "queue"
        queue_dir.mkdir()
        # null content
        null_path = queue_dir / "a-null.json"
        null_path.write_text(json.dumps({"label": "null-report", "content": None}))
        # list content
        list_path = queue_dir / "b-list.json"
        list_path.write_text(
            json.dumps({"label": "list-report", "content": ["not", "a", "string"]})
        )
        # valid entry in the same drain
        good_path = queue_dir / "c-good.json"
        good_path.write_text(json.dumps({"label": "good-report", "content": "real data"}))

        text, paths = drain_inject_queue(queue_dir)

        # Bad files were unlinked inline (so they don't keep failing every drain).
        assert not null_path.exists()
        assert not list_path.exists()
        # Good file is preserved for two-phase commit and returned to caller.
        assert good_path.exists()
        assert paths == [good_path]
        # Good entry made it into the rendered text; bad labels did not.
        assert "good-report" in text
        assert "real data" in text
        assert "null-report" not in text
        assert "list-report" not in text


class TestCommitInjectQueue:
    def test_empty_list_is_noop(self, tmp_path):
        # Just ensure it doesn't raise.
        commit_inject_queue([])

    def test_missing_file_is_tolerated(self, tmp_path):
        """If a path is already gone (e.g. another process raced), commit
        must not raise — it's best-effort cleanup."""
        ghost = tmp_path / "ghost.json"
        commit_inject_queue([ghost])  # no exception

    def test_deletes_each_path(self, tmp_path):
        a = tmp_path / "a.json"
        b = tmp_path / "b.json"
        a.write_text("{}")
        b.write_text("{}")
        commit_inject_queue([a, b])
        assert not a.exists()
        assert not b.exists()
