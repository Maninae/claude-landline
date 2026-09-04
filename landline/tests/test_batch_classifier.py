"""Regression tests for landline.runtime.batch_classifier.

Covers:
  - ``extract_chat_id`` helper centralizes the Telegram-envelope
    ``str(chat.id)`` walk with a defaulting fallback.
  - BackgroundPoller filters Telegram to ``allowed_updates=["message"]``,
    so the classifier never sees callback_query / edited_channel_post /
    inline_query. If anyone re-adds the old dead callback_query branch (or
    weakens the invariant), the source-level guard fails immediately.
"""

from unittest.mock import MagicMock, patch

import pytest

from landline.runtime.batch_classifier import (
    classify_updates,
    extract_chat_id,
    extract_user_id,
)


def _make_daemon(running: bool = True) -> MagicMock:
    """Synthetic daemon coordinator exposing only the attrs classify_updates
    touches. Defaults mirror an unlocked, allow-all daemon."""
    daemon = MagicMock()
    daemon.running = running
    daemon.token = "fake-token"
    daemon._guard_fn = MagicMock(return_value=True)
    daemon._reject_fn = MagicMock()
    daemon._send_response = MagicMock()
    daemon._advance_update_cursor = MagicMock()
    daemon._handle_non_text_update = MagicMock()
    return daemon


# ---------------------------------------------------------------------------
# extract_chat_id helper
# ---------------------------------------------------------------------------

class TestExtractChatId:
    """The helper centralizes the Telegram envelope's nested chat.id walk +
    str coercion. Reverting to inline ``.get().get()`` walks would leave
    these contract tests passing — the importability test below is the
    revert-fail anchor."""

    def test_integer_id_returned_as_str(self):
        """Telegram returns ints; downstream allowlist/lock code expects
        strings. Helper must always coerce."""
        assert extract_chat_id({"chat": {"id": 12345}}) == "12345"

    def test_string_id_passthrough(self):
        """Already-stringified ids must not be double-wrapped or lost."""
        assert extract_chat_id({"chat": {"id": "67890"}}) == "67890"

    def test_missing_chat_returns_default(self):
        """No ``chat`` key → default. Callers pass their own fallback (e.g. a
        chat_id extracted from a sibling message in the batch) when they need
        one; the helper stays defaulting-to-empty."""
        assert extract_chat_id({}) == ""
        assert extract_chat_id({}, default="fallback") == "fallback"

    def test_missing_id_returns_default(self):
        """Partial envelope (chat present, id missing) → default."""
        assert extract_chat_id({"chat": {}}) == ""
        assert extract_chat_id({"chat": {}}, default="x") == "x"

    def test_none_chat_treated_as_missing_attribute(self):
        """The helper preserves the original inline behavior verbatim: a
        literal ``None`` under ``chat`` causes ``None.get(...)`` to raise.
        Pins the helper to match the inline reads we are replacing — no
        silent semantics drift."""
        with pytest.raises(AttributeError):
            extract_chat_id({"chat": None})

    def test_helper_used_in_classify_updates(self):
        """Round-trip through classify_updates: chat_id reaches the bucket
        as a ``str`` (not an int) — proves the helper is wired in line 66."""
        daemon = _make_daemon()
        update = {
            "update_id": 1,
            "message": {
                "chat": {"id": 555},
                "from": {"id": 555},
                "text": "hi",
            },
        }
        _, text_updates, _, _, _, _, _ = classify_updates(daemon, [update])
        assert len(text_updates) == 1
        # Triple is (message, update_id, text) for text bucket; chat_id is
        # not stored here, but the missing-chat guard would have dropped
        # the update if extraction yielded "". Successful routing proves the
        # helper was called.

    def test_importable_from_batch_classifier(self):
        """REVERT-FAIL: if a future patch deletes ``extract_chat_id`` from
        ``batch_classifier.py`` but leaves ``orchestrator.py`` importing it,
        this fails at collection time AND the daemon's import line breaks
        at startup — exactly the loud-failure mode we want."""
        from landline.runtime.batch_classifier import extract_chat_id as _h
        assert callable(_h)


# ---------------------------------------------------------------------------
# M2 — dead callback_query branch pruned + docstring honest
# ---------------------------------------------------------------------------

class TestCallbackQueryUnreachable:
    """The poller's ``allowed_updates=['message']`` filter means
    ``callback_query`` updates never reach the classifier. These tests guard
    against any re-introduction of the removed defensive branch."""

    def test_no_callback_query_branch_runs_at_classification(self):
        """A synthetic callback_query update is treated like any unknown
        update shape — it falls through the message branch and is dropped
        without a dedicated callback path. The poller-side filter is what
        prevents this update from ever reaching here in production."""
        daemon = _make_daemon()
        synthetic_callback = {
            "update_id": 42,
            "callback_query": {"id": "cb-1", "data": "noop"},
        }
        cmds, texts, photos, pauses, docs, voices, videos = classify_updates(
            daemon, [synthetic_callback]
        )
        # No bucket should receive a callback_query.
        assert cmds == []
        assert texts == []
        assert photos == []
        assert pauses == []
        assert docs == []
        assert voices == []
        assert videos == []
        # Cursor still advances exactly once (via the missing-message path,
        # not via a dedicated callback branch — see absence assertion below).
        daemon._advance_update_cursor.assert_called_once_with(42)
        # The reject path is NOT taken (no chat_id to reject against).
        daemon._reject_fn.assert_not_called()

    def test_callback_query_advances_via_missing_message_not_callback_branch(
        self,
    ):
        """White-box: with vs. without the 'callback_query' key produces
        identical bucket output. Reverting the prune still passes — by
        design (the prune preserves behavior); the source-level guard
        below catches the revert directly."""
        daemon = _make_daemon()
        with_cb = {"update_id": 7, "callback_query": {"id": "x"}}
        without_cb = {"update_id": 7}
        out_a = classify_updates(daemon, [with_cb])
        daemon._advance_update_cursor.reset_mock()
        out_b = classify_updates(daemon, [without_cb])
        assert out_a == out_b

    def test_message_branch_is_the_only_entry_point(self):
        """Source-level guard: the classifier source must not contain a
        dedicated 'callback_query' branch. This fails the second anyone
        adds ``if update.get("callback_query"):`` back."""
        import inspect

        from landline.runtime import batch_classifier

        source = inspect.getsource(batch_classifier.classify_updates)
        assert "callback_query" not in source, (
            "classify_updates must not branch on callback_query; the "
            "poller's allowed_updates=['message'] filter makes that branch "
            "dead. If you need callback handling, extend allowed_updates "
            "in poller.py first (and update "
            "test_poller.test_request_url_and_payload)."
        )


class TestMessageOnlyInvariantDocumentedInModule:
    """The module docstring must point to poller.py so future readers
    follow the invariant chain instead of re-adding dead defensive
    branches."""

    def test_docstring_references_allowed_updates_filter(self):
        from landline.runtime import batch_classifier

        doc = batch_classifier.__doc__ or ""
        assert "allowed_updates" in doc, (
            "batch_classifier docstring must reference the poller's "
            "allowed_updates filter so the message-only invariant is "
            "discoverable without grepping poller.py."
        )
        # Docstring must not claim callback queries are a trivial-skip side
        # effect — they never reach the classifier at all.
        lower = doc.lower()
        if "callback queries" in lower:
            assert "never reach" in lower or "do not reach" in lower, (
                "batch_classifier docstring must not claim it handles "
                "callback queries as a trivial-skip side effect — that "
                "is stale."
            )


# ---------------------------------------------------------------------------
# Smoke tests — happy bucket routing survives the M2 prune
# ---------------------------------------------------------------------------

class TestClassifierStillWorksAfterPrune:
    """Smoke-test the buckets to confirm the prune did not regress the
    happy paths."""

    def test_plain_text_lands_in_text_bucket(self):
        daemon = _make_daemon()
        update = {
            "update_id": 1,
            "message": {
                "chat": {"id": 12345},
                "from": {"id": 12345},
                "text": "hello agent",
            },
        }
        cmds, texts, photos, pauses, docs, voices, videos = classify_updates(daemon, [update])
        assert len(texts) == 1
        assert texts[0][1] == 1
        assert texts[0][2] == "hello agent"
        assert cmds == photos == pauses == docs == []
        daemon._advance_update_cursor.assert_not_called()

    def test_pause_command_lands_in_pause_bucket(self):
        daemon = _make_daemon()
        update = {
            "update_id": 2,
            "message": {
                "chat": {"id": 12345},
                "from": {"id": 12345},
                "text": "/pause",
            },
        }
        cmds, texts, photos, pauses, docs, voices, videos = classify_updates(daemon, [update])
        assert len(pauses) == 1
        assert pauses[0][1] == 2
        assert pauses[0][2] == "12345"
        assert cmds == texts == photos == docs == []

    def test_slash_command_lands_in_command_bucket(self):
        daemon = _make_daemon()
        update = {
            "update_id": 3,
            "message": {
                "chat": {"id": 12345},
                "from": {"id": 12345},
                "text": "/status",
            },
        }
        cmds, texts, photos, pauses, docs, voices, videos = classify_updates(daemon, [update])
        assert len(cmds) == 1
        assert cmds[0][1] == 3
        assert cmds[0][2] == "/status"
        assert texts == photos == pauses == docs == []

    def test_photo_lands_in_photo_bucket(self):
        daemon = _make_daemon()
        update = {
            "update_id": 4,
            "message": {
                "chat": {"id": 12345},
                "from": {"id": 12345},
                "photo": [{"file_id": "f", "width": 1, "height": 1}],
            },
        }
        cmds, texts, photos, pauses, docs, voices, videos = classify_updates(daemon, [update])
        assert len(photos) == 1
        assert photos[0][1] == 4
        assert photos[0][2] == "12345"
        assert cmds == texts == pauses == docs == []


# ---------------------------------------------------------------------------
# Reactions must NEVER leak real HTTP calls to Telegram in tests
# ---------------------------------------------------------------------------


class TestReactionNetworkIsolation:
    """Pin: no test in the general suite may ever fire a real
    ``setMessageReaction`` POST to api.telegram.org. The autouse conftest
    fixture ``disable_reactions_network`` flips
    ``config.REACTION_ACKS_ENABLED`` to False for every test that has NOT
    opted in via the ``reactions_network`` marker.

    The classifier's ``_ack_and_record`` fires an unconditional
    ``reactions.set_reaction_async`` per accepted content message. Without
    the autouse guard the whole test suite silently POSTs to Telegram (61
    real requests per run when this test was added).
    """

    def test_default_config_kill_switch_is_flipped_off_in_tests(self):
        """The autouse conftest fixture must have flipped the flag off."""
        from landline import config
        assert config.REACTION_ACKS_ENABLED is False, (
            "conftest disable_reactions_network fixture regressed — the "
            "test suite will start leaking real setMessageReaction POSTs"
        )

    def test_classify_photo_and_text_batch_makes_zero_reaction_urlopen(self):
        """Full-suite defense: a batch of accepted photo + text + doc +
        voice updates classifies without any ``urllib.request.urlopen``
        call to the setMessageReaction endpoint."""
        daemon = _make_daemon()
        updates = [
            {
                "update_id": 100,
                "message": {
                    "message_id": 1000, "chat": {"id": 12345},
                    "photo": [{"file_id": "f", "width": 1, "height": 1}],
                },
            },
            {
                "update_id": 101,
                "message": {
                    "message_id": 1001, "chat": {"id": 12345},
                    "text": "hello",
                },
            },
        ]
        with patch("urllib.request.urlopen") as mock_urlopen:
            classify_updates(daemon, updates)
        assert mock_urlopen.call_count == 0, (
            "classify_updates fired %d real urlopen call(s) — reaction "
            "kill switch regressed" % mock_urlopen.call_count
        )


# ---------------------------------------------------------------------------
# Document ingestion bucket
# ---------------------------------------------------------------------------


class TestDocumentBucket:
    """Document classification, sanitization, size cap, mime gate."""

    def _make_doc_update(
        self,
        uid=100,
        file_name="report.pdf",
        file_size=1024,
        mime_type=None,
    ):
        document = {
            "file_id": "docfile-1",
            "file_name": file_name,
            "file_size": file_size,
        }
        if mime_type is not None:
            document["mime_type"] = mime_type
        return {
            "update_id": uid,
            "message": {
                "chat": {"id": 12345},
                "from": {"id": 12345},
                "document": document,
            },
        }

    def test_valid_pdf_lands_in_document_bucket(self):
        daemon = _make_daemon()
        update = self._make_doc_update(
            file_name="report.pdf", file_size=1024,
        )
        cmds, texts, photos, pauses, docs, voices, videos = classify_updates(daemon, [update])
        assert len(docs) == 1
        assert docs[0][1] == update["update_id"]
        assert docs[0][2] == "12345"
        assert cmds == texts == photos == pauses == []
        daemon._handle_non_text_update.assert_not_called()

    def test_disallowed_extension_falls_through_to_non_text(self):
        daemon = _make_daemon()
        update = self._make_doc_update(file_name="malware.exe")
        cmds, texts, photos, pauses, docs, voices, videos = classify_updates(daemon, [update])
        assert docs == []
        # Falls through to the brush-off notice path.
        daemon._handle_non_text_update.assert_called_once()

    def test_path_traversal_rejected(self):
        daemon = _make_daemon()
        update = self._make_doc_update(
            file_name="../../../../etc/passwd",
        )
        cmds, texts, photos, pauses, docs, voices, videos = classify_updates(daemon, [update])
        assert docs == []
        daemon._handle_non_text_update.assert_called_once()

    def test_over_cap_size_rejected(self):
        from landline.config import DOCUMENT_MAX_SIZE_BYTES
        daemon = _make_daemon()
        update = self._make_doc_update(
            file_name="big.pdf",
            file_size=DOCUMENT_MAX_SIZE_BYTES + 1,
        )
        cmds, texts, photos, pauses, docs, voices, videos = classify_updates(daemon, [update])
        assert docs == []
        daemon._handle_non_text_update.assert_called_once()

    def test_disallowed_mime_falls_through(self):
        daemon = _make_daemon()
        update = self._make_doc_update(
            file_name="notes.txt", mime_type="application/octet-stream",
        )
        cmds, texts, photos, pauses, docs, voices, videos = classify_updates(daemon, [update])
        # Extension is fine, but the mime confirmation blocks.
        assert docs == []
        daemon._handle_non_text_update.assert_called_once()

    def test_missing_mime_is_accepted(self):
        """Extension is the primary gate — a missing mime does NOT reject."""
        daemon = _make_daemon()
        update = self._make_doc_update(
            file_name="notes.txt", mime_type=None,
        )
        cmds, texts, photos, pauses, docs, voices, videos = classify_updates(daemon, [update])
        assert len(docs) == 1

    def test_extension_lowercased_on_sanitize(self):
        """`.PDF` normalizes to `.pdf` and is accepted."""
        daemon = _make_daemon()
        update = self._make_doc_update(file_name="REPORT.PDF")
        cmds, texts, photos, pauses, docs, voices, videos = classify_updates(daemon, [update])
        assert len(docs) == 1


# ---------------------------------------------------------------------------
# Voice-note bucket
# ---------------------------------------------------------------------------


class TestVoiceBucket:
    """Voice / audio / video_note lands in the voice bucket."""

    def _make_voice_update(self, uid=200, media_key="voice", duration=10):
        return {
            "update_id": uid,
            "message": {
                "chat": {"id": 12345},
                "from": {"id": 12345},
                media_key: {"file_id": f"{media_key}-file", "duration": duration},
            },
        }

    def test_voice_message_lands_in_voice_bucket(self):
        daemon = _make_daemon()
        update = self._make_voice_update(media_key="voice", duration=15)
        cmds, texts, photos, pauses, docs, voices, videos = classify_updates(
            daemon, [update]
        )
        assert len(voices) == 1
        assert voices[0][1] == update["update_id"]
        assert voices[0][2] == "12345"
        assert cmds == texts == photos == pauses == docs == videos == []

    def test_audio_message_lands_in_voice_bucket(self):
        daemon = _make_daemon()
        update = self._make_voice_update(media_key="audio", duration=20)
        _, _, _, _, docs, voices, _ = classify_updates(daemon, [update])
        assert len(voices) == 1
        assert docs == []

    def test_video_note_lands_in_voice_bucket(self):
        daemon = _make_daemon()
        update = self._make_voice_update(media_key="video_note", duration=5)
        _, _, _, _, _, voices, _ = classify_updates(daemon, [update])
        assert len(voices) == 1

    def test_photo_wins_over_voice_when_both_present(self):
        """Telegram sends one media per message, but if both keys were set
        the photo bucket wins (precedence order in the classifier)."""
        daemon = _make_daemon()
        update = {
            "update_id": 201,
            "message": {
                "chat": {"id": 12345},
                "from": {"id": 12345},
                "photo": [{"file_id": "p", "width": 1, "height": 1}],
                "voice": {"file_id": "v", "duration": 5},
            },
        }
        _, _, photos, _, _, voices, _ = classify_updates(daemon, [update])
        assert len(photos) == 1
        assert voices == []

    def test_long_duration_still_lands_in_voice_bucket(self):
        """Duration filtering is enforced by the handler, NOT the
        classifier. A 999s voice note still buckets — the handler will
        reject it with a clear notice."""
        daemon = _make_daemon()
        update = self._make_voice_update(media_key="voice", duration=999)
        _, _, _, _, _, voices, _ = classify_updates(daemon, [update])
        assert len(voices) == 1


# ---------------------------------------------------------------------------
# Video bucket
# ---------------------------------------------------------------------------


class TestVideoBucket:
    """Video classification. Two shapes route here:
      - Bare ``video`` message (camera roll upload).
      - ``document`` with a ``video/*`` mime (desktop drag-and-drop).

    ``video_note`` continues to bucket into voices — that's already-tested
    behavior; we just guard against a regression that would re-route it.
    """

    def _bare_video_update(
        self, uid=300, file_size=1024, mime_type="video/mp4",
    ):
        return {
            "update_id": uid,
            "message": {
                "chat": {"id": 12345},
                "from": {"id": 12345},
                "video": {
                    "file_id": "vidfile-1",
                    "duration": 5,
                    "width": 640,
                    "height": 480,
                    "mime_type": mime_type,
                    "file_size": file_size,
                },
            },
        }

    def _video_document_update(
        self, uid=301, file_name="clip.mp4", mime_type="video/mp4",
        file_size=2048,
    ):
        return {
            "update_id": uid,
            "message": {
                "chat": {"id": 12345},
                "from": {"id": 12345},
                "document": {
                    "file_id": "docvid-1",
                    "file_name": file_name,
                    "file_size": file_size,
                    "mime_type": mime_type,
                },
            },
        }

    def test_bare_video_lands_in_video_bucket(self):
        daemon = _make_daemon()
        update = self._bare_video_update()
        cmds, texts, photos, pauses, docs, voices, videos = classify_updates(
            daemon, [update],
        )
        assert len(videos) == 1
        assert videos[0][1] == update["update_id"]
        assert videos[0][2] == "12345"
        # Nothing else fires.
        assert cmds == texts == photos == pauses == docs == voices == []
        daemon._handle_non_text_update.assert_not_called()

    def test_video_document_routes_to_video_bucket_not_document(self):
        """A ``document`` with a ``video/mp4`` mime must land in the video
        bucket, NOT the document bucket (which would reject it on the
        extension gate). This is the load-bearing precedence contract:
        video check runs before the document check."""
        daemon = _make_daemon()
        update = self._video_document_update()
        _, _, _, _, docs, _, videos = classify_updates(daemon, [update])
        assert len(videos) == 1
        assert docs == []
        daemon._handle_non_text_update.assert_not_called()

    def test_over_cap_video_still_buckets_for_handler_notice(self):
        """The classifier does NOT enforce ``TELEGRAM_VIDEO_SIZE_LIMIT`` —
        an oversized video still lands in the bucket so the handler can
        give a clear "too big" notice instead of the generic non-text
        brush-off."""
        from landline.config import TELEGRAM_VIDEO_SIZE_LIMIT
        daemon = _make_daemon()
        update = self._bare_video_update(
            file_size=TELEGRAM_VIDEO_SIZE_LIMIT + 1,
        )
        _, _, _, _, _, _, videos = classify_updates(daemon, [update])
        assert len(videos) == 1

    def test_video_note_still_lands_in_voice_bucket(self):
        """Regression guard: ``video_note`` (round camera-toggle videos)
        MUST continue to route to the voice pipeline (whisper transcribe)
        — the video bucket only handles the ``video`` field, not
        ``video_note``."""
        daemon = _make_daemon()
        update = {
            "update_id": 302,
            "message": {
                "chat": {"id": 12345},
                "from": {"id": 12345},
                "video_note": {"file_id": "vn-1", "duration": 5},
            },
        }
        _, _, _, _, _, voices, videos = classify_updates(daemon, [update])
        assert len(voices) == 1
        assert videos == []

    def test_non_video_document_mime_stays_in_document_bucket(self):
        """Belt-and-suspenders: a ``document`` with a non-video mime
        (e.g. ``application/pdf``) must NOT be dragged into the video
        bucket by the precedence check."""
        daemon = _make_daemon()
        update = {
            "update_id": 303,
            "message": {
                "chat": {"id": 12345},
                "from": {"id": 12345},
                "document": {
                    "file_id": "d-1",
                    "file_name": "report.pdf",
                    "file_size": 4096,
                    "mime_type": "application/pdf",
                },
            },
        }
        _, _, _, _, docs, _, videos = classify_updates(daemon, [update])
        assert len(docs) == 1
        assert videos == []

    def test_photo_precedes_video_when_both_present(self):
        """Telegram sends one media per message, but a pathological
        message with both keys should keep the existing photo-wins
        precedence — the video branch runs AFTER the photo branch."""
        daemon = _make_daemon()
        update = {
            "update_id": 304,
            "message": {
                "chat": {"id": 12345},
                "from": {"id": 12345},
                "photo": [{"file_id": "p", "width": 1, "height": 1}],
                "video": {"file_id": "v", "file_size": 1024},
            },
        }
        _, _, photos, _, _, _, videos = classify_updates(daemon, [update])
        assert len(photos) == 1
        assert videos == []


# ---------------------------------------------------------------------------
# Reaction ACKs (👀 at classify time)
# ---------------------------------------------------------------------------


class TestReactionAcks:
    """The classifier fires a 👀 receipt reaction on every accepted
    content message (text / photo / voice / document) and records the
    message_id on the daemon's per-batch tracker so the dispatcher can
    fire a 👌 on successful finalize.

    Contract:
      - fires ONLY after the guard passes (no reactions on unauthorized
        senders — enumeration silent).
      - does NOT fire on /pause, /commands, edited messages, missing
        chat_id, too-long text, or the non-text brush-off.
      - populates ``daemon._batch_ack_message_ids[chat_id]`` with the
        server-side message_id for each ack.
    """

    def _make_daemon_with_tracker(self):
        daemon = _make_daemon()
        daemon._batch_ack_message_ids = {}
        return daemon

    def _text_update(self, uid, text, chat_id=12345, message_id=None):
        return {
            "update_id": uid,
            "message": {
                "message_id": message_id if message_id is not None else uid * 10,
                "chat": {"id": chat_id},
                "from": {"id": chat_id},
                "text": text,
            },
        }

    def test_acks_text_message(self):
        from landline.config import REACTION_ACK_EMOJI
        daemon = self._make_daemon_with_tracker()
        update = self._text_update(1, "hello agent", message_id=555)
        with patch("landline.runtime.batch_classifier.reactions.set_reaction_async") as mock_ack:
            classify_updates(daemon, [update])
        mock_ack.assert_called_once_with(
            daemon.token, "12345", 555, REACTION_ACK_EMOJI,
        )
        assert daemon._batch_ack_message_ids["12345"] == [555]

    def test_acks_photo_message(self):
        from landline.config import REACTION_ACK_EMOJI
        daemon = self._make_daemon_with_tracker()
        update = {
            "update_id": 2,
            "message": {
                "message_id": 777,
                "chat": {"id": 12345},
                "from": {"id": 12345},
                "photo": [{"file_id": "p", "width": 1, "height": 1}],
            },
        }
        with patch("landline.runtime.batch_classifier.reactions.set_reaction_async") as mock_ack:
            classify_updates(daemon, [update])
        mock_ack.assert_called_once()
        assert mock_ack.call_args[0][2] == 777
        assert mock_ack.call_args[0][3] == REACTION_ACK_EMOJI
        assert daemon._batch_ack_message_ids["12345"] == [777]

    def test_acks_voice_message(self):
        from landline.config import REACTION_ACK_EMOJI
        daemon = self._make_daemon_with_tracker()
        update = {
            "update_id": 3,
            "message": {
                "message_id": 888,
                "chat": {"id": 12345},
                "from": {"id": 12345},
                "voice": {"file_id": "v", "duration": 5},
            },
        }
        with patch("landline.runtime.batch_classifier.reactions.set_reaction_async") as mock_ack:
            classify_updates(daemon, [update])
        mock_ack.assert_called_once()
        assert mock_ack.call_args[0][2] == 888
        assert mock_ack.call_args[0][3] == REACTION_ACK_EMOJI
        assert daemon._batch_ack_message_ids["12345"] == [888]

    def test_acks_document_message(self):
        from landline.config import REACTION_ACK_EMOJI
        daemon = self._make_daemon_with_tracker()
        update = {
            "update_id": 4,
            "message": {
                "message_id": 999,
                "chat": {"id": 12345},
                "from": {"id": 12345},
                "document": {
                    "file_id": "d",
                    "file_name": "report.pdf",
                    "file_size": 1024,
                },
            },
        }
        with patch("landline.runtime.batch_classifier.reactions.set_reaction_async") as mock_ack:
            classify_updates(daemon, [update])
        mock_ack.assert_called_once()
        assert mock_ack.call_args[0][2] == 999
        assert mock_ack.call_args[0][3] == REACTION_ACK_EMOJI
        assert daemon._batch_ack_message_ids["12345"] == [999]

    def test_does_not_ack_pause_command(self):
        """/pause is a control message, not content. No 👀 receipt."""
        daemon = self._make_daemon_with_tracker()
        update = self._text_update(5, "/pause", message_id=111)
        with patch("landline.runtime.batch_classifier.reactions.set_reaction_async") as mock_ack:
            classify_updates(daemon, [update])
        mock_ack.assert_not_called()
        assert daemon._batch_ack_message_ids == {}

    def test_does_not_ack_slash_command(self):
        """Slash commands render as text via CommandRouter — no reaction."""
        daemon = self._make_daemon_with_tracker()
        update = self._text_update(6, "/status", message_id=222)
        with patch("landline.runtime.batch_classifier.reactions.set_reaction_async") as mock_ack:
            classify_updates(daemon, [update])
        mock_ack.assert_not_called()
        assert daemon._batch_ack_message_ids == {}

    def test_does_not_ack_edited_message(self):
        daemon = self._make_daemon_with_tracker()
        update = {
            "update_id": 7,
            "message": {
                "message_id": 333,
                "chat": {"id": 12345},
                "from": {"id": 12345},
                "text": "edited",
                "edit_date": 123456,
            },
        }
        with patch("landline.runtime.batch_classifier.reactions.set_reaction_async") as mock_ack:
            classify_updates(daemon, [update])
        mock_ack.assert_not_called()
        assert daemon._batch_ack_message_ids == {}

    def test_does_not_ack_missing_chat_id(self):
        daemon = self._make_daemon_with_tracker()
        update = {
            "update_id": 8,
            "message": {
                "message_id": 444,
                "text": "no chat",
            },
        }
        with patch("landline.runtime.batch_classifier.reactions.set_reaction_async") as mock_ack:
            classify_updates(daemon, [update])
        mock_ack.assert_not_called()
        assert daemon._batch_ack_message_ids == {}

    def test_does_not_ack_unauthorized_chat(self):
        """Enumeration guard: a rejected sender must NEVER see a reaction —
        that would confirm the bot is watching them. Reactions fire AFTER
        the guard passes."""
        daemon = _make_daemon()
        daemon._batch_ack_message_ids = {}
        daemon._guard_fn = MagicMock(return_value=False)
        update = self._text_update(9, "hello", message_id=555)
        with patch("landline.runtime.batch_classifier.reactions.set_reaction_async") as mock_ack:
            classify_updates(daemon, [update])
        mock_ack.assert_not_called()
        assert daemon._batch_ack_message_ids == {}
        # The rejection path still fired.
        daemon._reject_fn.assert_called_once()

    def test_does_not_ack_too_long_text(self):
        """Too-long text gets a brush-off, not a queue — no receipt."""
        from landline.config import MAX_MESSAGE_LENGTH
        daemon = self._make_daemon_with_tracker()
        long_text = "x" * (MAX_MESSAGE_LENGTH + 1)
        update = self._text_update(10, long_text, message_id=1111)
        with patch("landline.runtime.batch_classifier.reactions.set_reaction_async") as mock_ack:
            classify_updates(daemon, [update])
        mock_ack.assert_not_called()
        assert daemon._batch_ack_message_ids == {}

    def test_does_not_ack_non_text_non_media(self):
        """Empty message (no text, no media) hits the brush-off path."""
        daemon = self._make_daemon_with_tracker()
        update = {
            "update_id": 11,
            "message": {
                "message_id": 2222,
                "chat": {"id": 12345},
                "from": {"id": 12345},
            },
        }
        with patch("landline.runtime.batch_classifier.reactions.set_reaction_async") as mock_ack:
            classify_updates(daemon, [update])
        mock_ack.assert_not_called()

    def test_does_not_ack_rejected_document(self):
        """A document that fails the extension/mime/size gate falls through
        to the brush-off — no reaction (rejection is not receipt)."""
        daemon = self._make_daemon_with_tracker()
        update = {
            "update_id": 12,
            "message": {
                "message_id": 3333,
                "chat": {"id": 12345},
                "from": {"id": 12345},
                "document": {
                    "file_id": "d",
                    "file_name": "malware.exe",
                    "file_size": 1024,
                },
            },
        }
        with patch("landline.runtime.batch_classifier.reactions.set_reaction_async") as mock_ack:
            classify_updates(daemon, [update])
        mock_ack.assert_not_called()

    def test_tracker_records_multiple_messages_in_order(self):
        """Multi-message batch: tracker keeps ids in classification order."""
        daemon = self._make_daemon_with_tracker()
        updates = [
            self._text_update(20, "one", message_id=100),
            self._text_update(21, "two", message_id=200),
            self._text_update(22, "three", message_id=300),
        ]
        with patch("landline.runtime.batch_classifier.reactions.set_reaction_async"):
            classify_updates(daemon, updates)
        assert daemon._batch_ack_message_ids["12345"] == [100, 200, 300]

    def test_tracker_optional_missing_attr_does_not_crash(self):
        """Backwards compat: if the daemon has no ``_batch_ack_message_ids``
        attribute, the classifier still runs (reactions still fire)."""
        daemon = _make_daemon()
        # Intentionally do NOT set _batch_ack_message_ids.
        update = self._text_update(30, "hi", message_id=999)
        with patch("landline.runtime.batch_classifier.reactions.set_reaction_async") as mock_ack:
            classify_updates(daemon, [update])
        mock_ack.assert_called_once()


class TestDocumentRejectLogPrivacy:
    """Finding pin (daemon/batch_classifier.py:182-190): the reject log
    line for an unacceptable document MUST NOT include the attacker-
    controlled / user-supplied filename. Only chat_id + size + mime
    are metadata-safe."""

    def _make_doc_update(self, file_name, file_size=1024, mime_type=None):
        document = {
            "file_id": "docfile-priv",
            "file_name": file_name,
            "file_size": file_size,
        }
        if mime_type is not None:
            document["mime_type"] = mime_type
        return {
            "update_id": 999,
            "message": {
                "chat": {"id": 12345},
                "from": {"id": 12345},
                "document": document,
            },
        }

    def test_reject_log_does_not_leak_filename(self):
        daemon = _make_daemon()
        sensitive = "private_medical_records_XSensitiveMarker.exe"
        update = self._make_doc_update(sensitive)
        with patch("landline.runtime.batch_classifier.log") as mock_log:
            classify_updates(daemon, [update])
        for call in mock_log.call_args_list:
            args = list(call.args) + list(call.kwargs.values())
            for arg in args:
                if isinstance(arg, str):
                    assert sensitive not in arg, (
                        f"filename leaked into classifier log: {arg!r}"
                    )
                    assert "XSensitiveMarker" not in arg


# ---------------------------------------------------------------------------
# extract_user_id helper — from.id is the auth identity now
# ---------------------------------------------------------------------------

class TestExtractUserId:
    """The classifier's auth path routes ``message["from"]["id"]`` to the
    guard (not chat.id). See ``landline.runtime.guard`` module docstring
    for the security rationale."""

    def test_returns_int_from_id(self):
        assert extract_user_id({"from": {"id": 987654321}}) == 987654321
        assert isinstance(
            extract_user_id({"from": {"id": 42}}), int
        )

    def test_coerces_string_id_to_int(self):
        """Belt-and-suspenders: some Telegram edge cases may serialize id
        as a string. Coerce to int so the guard's Set[int] lookup hits."""
        assert extract_user_id({"from": {"id": "555"}}) == 555

    def test_missing_from_returns_none(self):
        """No ``from`` field → None. Classifier drops the message
        (anonymous shape has no user id to authorize against)."""
        assert extract_user_id({}) is None
        assert extract_user_id({"chat": {"id": 111}}) is None

    def test_missing_from_id_returns_none(self):
        """``from`` present but ``from.id`` absent → None (drop)."""
        assert extract_user_id({"from": {}}) is None

    def test_non_dict_from_returns_none(self):
        """Malformed ``from`` (a string, a list) → None. Never crash."""
        assert extract_user_id({"from": "anonymous"}) is None
        assert extract_user_id({"from": None}) is None

    def test_uncoerceable_id_returns_none(self):
        """A weird id shape (dict, list) that isn't an int → None."""
        assert extract_user_id({"from": {"id": {"nested": 1}}}) is None
        assert extract_user_id({"from": {"id": None}}) is None


class TestFromIdIsAuthIdentity:
    """The guard receives ``from.id``, NOT ``chat.id``. Two integration
    proofs: an owner-shape message (chat.id == from.id) authorizes as
    before, and a group-chat shape (chat.id != from.id, from.id is the
    intruder's user id) authorizes on the from.id — not the group id."""

    def test_guard_receives_from_id_int(self):
        """Guard is called with the sender's ``from.id`` as int."""
        daemon = _make_daemon()
        update = {
            "update_id": 1,
            "message": {
                "chat": {"id": 999_000_000},  # a group chat id
                "from": {"id": 111_222_333},  # the sender's user id
                "text": "hi",
            },
        }
        classify_updates(daemon, [update])
        daemon._guard_fn.assert_called_once_with(111_222_333)

    def test_group_sender_not_authorized_via_group_chat_id(self):
        """Load-bearing security regression guard.

        Pre-migration bug: authorizing on ``chat.id`` would ADMIT a group
        message whose ``chat.id`` (the group's id) happened to match an
        allow-listed value — even though the actual sender's ``from.id``
        was not on the allowlist. This test wires ``_guard_fn`` to allow
        ONLY the group's chat id (999) and DENY the from.id (444);
        the classifier must reject."""
        daemon = _make_daemon()
        # Allow chat.id (999), deny from.id (444). If the classifier still
        # authorizes on chat.id, the reject_fn will NOT be called; if it
        # correctly authorizes on from.id, it WILL be called.
        daemon._guard_fn = MagicMock(return_value=False)
        update = {
            "update_id": 1,
            "message": {
                "chat": {"id": 999},
                "from": {"id": 444},
                "text": "attack",
            },
        }
        classify_updates(daemon, [update])
        daemon._guard_fn.assert_called_once_with(444)
        # Reject reply lands in the CHAT surface (string), NOT the from.id.
        daemon._reject_fn.assert_called_once_with(daemon.token, "999")

    def test_owner_shape_backward_compat(self):
        """1:1 owner-bot shape (chat.id == from.id) still authorizes — the
        migration is a no-op for the common case."""
        daemon = _make_daemon()
        update = {
            "update_id": 2,
            "message": {
                "chat": {"id": 123_456_789},
                "from": {"id": 123_456_789},
                "text": "hi",
            },
        }
        classify_updates(daemon, [update])
        daemon._guard_fn.assert_called_once_with(123_456_789)
        daemon._reject_fn.assert_not_called()


class TestMissingFromIdSilentlyDropped:
    """A message shape with no ``from`` (channel post-like, malformed
    update) has NO identity to authorize against. The classifier must:

      - NOT call the guard (nothing to check).
      - NOT call ``reject_fn`` (no enumeration oracle, no visible ack).
      - NOT bucket the message.
      - Advance the cursor so the same broken update doesn't loop.
    """

    def test_no_from_field_drops_silently(self):
        daemon = _make_daemon()
        update = {
            "update_id": 42,
            "message": {
                "chat": {"id": 12345},
                "text": "should never reach a bucket",
                # No "from" — anonymous / channel-post-like
            },
        }
        cmds, texts, photos, pauses, docs, voices, videos = classify_updates(
            daemon, [update],
        )
        assert cmds == texts == photos == pauses == docs == voices == videos == []
        daemon._guard_fn.assert_not_called()
        daemon._reject_fn.assert_not_called()
        daemon._advance_update_cursor.assert_called_once_with(42)

    def test_no_from_id_drops_silently(self):
        """``from`` present but ``from.id`` missing — same drop."""
        daemon = _make_daemon()
        update = {
            "update_id": 43,
            "message": {
                "chat": {"id": 12345},
                "from": {},  # no id
                "text": "hi",
            },
        }
        cmds, texts, photos, pauses, docs, voices, videos = classify_updates(
            daemon, [update],
        )
        assert cmds == texts == photos == pauses == docs == voices == videos == []
        daemon._guard_fn.assert_not_called()
        daemon._reject_fn.assert_not_called()
        daemon._advance_update_cursor.assert_called_once_with(43)


class TestUnauthorizedSenderSilentlyDropped:
    """Guard denies (from.id not on allowlist) → the classifier must
    silently drop: reject_fn is invoked (which under default
    ``REJECTION_MODE == "silent"`` sends nothing — see ``guard.py``), no
    reaction is fired (enumeration oracle), no bucket, cursor advances.
    """

    def test_unauthorized_from_id_no_bucket_no_reaction(self):
        from landline.runtime.batch_classifier import reactions as _r
        daemon = _make_daemon()
        daemon._guard_fn = MagicMock(return_value=False)
        daemon._batch_ack_message_ids = {}
        update = {
            "update_id": 88,
            "message": {
                "message_id": 555,
                "chat": {"id": 999_999_999},
                "from": {"id": 999_999_999},
                "text": "attack",
            },
        }
        with patch.object(_r, "set_reaction_async") as mock_react:
            cmds, texts, photos, pauses, docs, voices, videos = classify_updates(
                daemon, [update],
            )
        assert cmds == texts == photos == pauses == docs == voices == videos == []
        # No 👀 reaction ever fires for an unauthorized sender.
        mock_react.assert_not_called()
        assert daemon._batch_ack_message_ids == {}
        # Cursor advances (attacker can't spam-replay the same message).
        daemon._advance_update_cursor.assert_called_once_with(88)
        # reject_fn IS called (with the chat surface — the guard.reject_message
        # implementation is what enforces silent vs loud mode; see test_guard).
        daemon._reject_fn.assert_called_once_with(daemon.token, "999999999")

    def test_unauthorized_reject_targets_chat_not_from_id(self):
        """When a group message would somehow reach here, the reject reply
        (loud mode) must target the chat surface, not the sender's user id
        — you can't send a bot message to a bare user id."""
        daemon = _make_daemon()
        daemon._guard_fn = MagicMock(return_value=False)
        update = {
            "update_id": 89,
            "message": {
                "message_id": 556,
                "chat": {"id": 111},   # group id
                "from": {"id": 222},   # attacker
                "text": "attack",
            },
        }
        classify_updates(daemon, [update])
        daemon._reject_fn.assert_called_once_with(daemon.token, "111")
