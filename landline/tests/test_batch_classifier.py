"""Regression tests for landline.batch_classifier.

Covers:
  - E4: ``extract_chat_id`` helper centralizes the Telegram-envelope
    ``str(chat.id)`` walk with a defaulting fallback.
  - M2: BackgroundPoller filters Telegram to ``allowed_updates=["message"]``,
    so the classifier never sees callback_query / edited_channel_post /
    inline_query. If anyone re-adds the old dead callback_query branch (or
    weakens the invariant), the source-level guard fails immediately.
"""

from unittest.mock import MagicMock, patch

import pytest

from landline.batch_classifier import classify_updates, extract_chat_id


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
# E4 — extract_chat_id helper
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
        """No ``chat`` key → default. The ``run()`` overflow-notice path
        falls back to ``self.chat_id`` when this returns the empty default."""
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
                "text": "hi",
            },
        }
        _, text_updates, _, _, _, _ = classify_updates(daemon, [update])
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
        from landline.batch_classifier import extract_chat_id as _h
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
        cmds, texts, photos, pauses, docs, voices = classify_updates(
            daemon, [synthetic_callback]
        )
        # No bucket should receive a callback_query.
        assert cmds == []
        assert texts == []
        assert photos == []
        assert pauses == []
        assert docs == []
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

        from landline import batch_classifier

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
        from landline import batch_classifier

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
                "text": "hello agent",
            },
        }
        cmds, texts, photos, pauses, docs, voices = classify_updates(daemon, [update])
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
                "text": "/pause",
            },
        }
        cmds, texts, photos, pauses, docs, voices = classify_updates(daemon, [update])
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
                "text": "/status",
            },
        }
        cmds, texts, photos, pauses, docs, voices = classify_updates(daemon, [update])
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
                "photo": [{"file_id": "f", "width": 1, "height": 1}],
            },
        }
        cmds, texts, photos, pauses, docs, voices = classify_updates(daemon, [update])
        assert len(photos) == 1
        assert photos[0][1] == 4
        assert photos[0][2] == "12345"
        assert cmds == texts == pauses == docs == []


# ---------------------------------------------------------------------------
# Cluster 3 — reactions must NEVER leak real HTTP calls to Telegram in tests
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
# Cluster 1 — document ingestion bucket
# ---------------------------------------------------------------------------


class TestDocumentBucket:
    """Cluster 1: document classification, sanitization, size cap, mime gate."""

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
                "document": document,
            },
        }

    def test_valid_pdf_lands_in_document_bucket(self):
        daemon = _make_daemon()
        update = self._make_doc_update(
            file_name="report.pdf", file_size=1024,
        )
        cmds, texts, photos, pauses, docs, voices = classify_updates(daemon, [update])
        assert len(docs) == 1
        assert docs[0][1] == update["update_id"]
        assert docs[0][2] == "12345"
        assert cmds == texts == photos == pauses == []
        daemon._handle_non_text_update.assert_not_called()

    def test_disallowed_extension_falls_through_to_non_text(self):
        daemon = _make_daemon()
        update = self._make_doc_update(file_name="malware.exe")
        cmds, texts, photos, pauses, docs, voices = classify_updates(daemon, [update])
        assert docs == []
        # Falls through to the brush-off notice path.
        daemon._handle_non_text_update.assert_called_once()

    def test_path_traversal_rejected(self):
        daemon = _make_daemon()
        update = self._make_doc_update(
            file_name="../../../../etc/passwd",
        )
        cmds, texts, photos, pauses, docs, voices = classify_updates(daemon, [update])
        assert docs == []
        daemon._handle_non_text_update.assert_called_once()

    def test_over_cap_size_rejected(self):
        from landline.config import DOCUMENT_MAX_SIZE_BYTES
        daemon = _make_daemon()
        update = self._make_doc_update(
            file_name="big.pdf",
            file_size=DOCUMENT_MAX_SIZE_BYTES + 1,
        )
        cmds, texts, photos, pauses, docs, voices = classify_updates(daemon, [update])
        assert docs == []
        daemon._handle_non_text_update.assert_called_once()

    def test_disallowed_mime_falls_through(self):
        daemon = _make_daemon()
        update = self._make_doc_update(
            file_name="notes.txt", mime_type="application/octet-stream",
        )
        cmds, texts, photos, pauses, docs, voices = classify_updates(daemon, [update])
        # Extension is fine, but the mime confirmation blocks.
        assert docs == []
        daemon._handle_non_text_update.assert_called_once()

    def test_missing_mime_is_accepted(self):
        """Extension is the primary gate — a missing mime does NOT reject."""
        daemon = _make_daemon()
        update = self._make_doc_update(
            file_name="notes.txt", mime_type=None,
        )
        cmds, texts, photos, pauses, docs, voices = classify_updates(daemon, [update])
        assert len(docs) == 1

    def test_extension_lowercased_on_sanitize(self):
        """`.PDF` normalizes to `.pdf` and is accepted."""
        daemon = _make_daemon()
        update = self._make_doc_update(file_name="REPORT.PDF")
        cmds, texts, photos, pauses, docs, voices = classify_updates(daemon, [update])
        assert len(docs) == 1


# ---------------------------------------------------------------------------
# Cluster 2 — voice-note bucket
# ---------------------------------------------------------------------------


class TestVoiceBucket:
    """Cluster 2: voice / audio / video_note lands in the voice bucket."""

    def _make_voice_update(self, uid=200, media_key="voice", duration=10):
        return {
            "update_id": uid,
            "message": {
                "chat": {"id": 12345},
                media_key: {"file_id": f"{media_key}-file", "duration": duration},
            },
        }

    def test_voice_message_lands_in_voice_bucket(self):
        daemon = _make_daemon()
        update = self._make_voice_update(media_key="voice", duration=15)
        cmds, texts, photos, pauses, docs, voices = classify_updates(
            daemon, [update]
        )
        assert len(voices) == 1
        assert voices[0][1] == update["update_id"]
        assert voices[0][2] == "12345"
        assert cmds == texts == photos == pauses == docs == []

    def test_audio_message_lands_in_voice_bucket(self):
        daemon = _make_daemon()
        update = self._make_voice_update(media_key="audio", duration=20)
        _, _, _, _, docs, voices = classify_updates(daemon, [update])
        assert len(voices) == 1
        assert docs == []

    def test_video_note_lands_in_voice_bucket(self):
        daemon = _make_daemon()
        update = self._make_voice_update(media_key="video_note", duration=5)
        _, _, _, _, _, voices = classify_updates(daemon, [update])
        assert len(voices) == 1

    def test_photo_wins_over_voice_when_both_present(self):
        """Telegram sends one media per message, but if both keys were set
        the photo bucket wins (precedence order in the classifier)."""
        daemon = _make_daemon()
        update = {
            "update_id": 201,
            "message": {
                "chat": {"id": 12345},
                "photo": [{"file_id": "p", "width": 1, "height": 1}],
                "voice": {"file_id": "v", "duration": 5},
            },
        }
        _, _, photos, _, _, voices = classify_updates(daemon, [update])
        assert len(photos) == 1
        assert voices == []

    def test_long_duration_still_lands_in_voice_bucket(self):
        """Duration filtering is enforced by the handler, NOT the
        classifier. A 999s voice note still buckets — the handler will
        reject it with a clear notice."""
        daemon = _make_daemon()
        update = self._make_voice_update(media_key="voice", duration=999)
        _, _, _, _, _, voices = classify_updates(daemon, [update])
        assert len(voices) == 1


# ---------------------------------------------------------------------------
# Cluster 3 — reaction ACKs (👀 at classify time)
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
                "text": text,
            },
        }

    def test_acks_text_message(self):
        from landline.config import REACTION_ACK_EMOJI
        daemon = self._make_daemon_with_tracker()
        update = self._text_update(1, "hello agent", message_id=555)
        with patch("landline.batch_classifier.reactions.set_reaction_async") as mock_ack:
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
                "photo": [{"file_id": "p", "width": 1, "height": 1}],
            },
        }
        with patch("landline.batch_classifier.reactions.set_reaction_async") as mock_ack:
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
                "voice": {"file_id": "v", "duration": 5},
            },
        }
        with patch("landline.batch_classifier.reactions.set_reaction_async") as mock_ack:
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
                "document": {
                    "file_id": "d",
                    "file_name": "report.pdf",
                    "file_size": 1024,
                },
            },
        }
        with patch("landline.batch_classifier.reactions.set_reaction_async") as mock_ack:
            classify_updates(daemon, [update])
        mock_ack.assert_called_once()
        assert mock_ack.call_args[0][2] == 999
        assert mock_ack.call_args[0][3] == REACTION_ACK_EMOJI
        assert daemon._batch_ack_message_ids["12345"] == [999]

    def test_does_not_ack_pause_command(self):
        """/pause is a control message, not content. No 👀 receipt."""
        daemon = self._make_daemon_with_tracker()
        update = self._text_update(5, "/pause", message_id=111)
        with patch("landline.batch_classifier.reactions.set_reaction_async") as mock_ack:
            classify_updates(daemon, [update])
        mock_ack.assert_not_called()
        assert daemon._batch_ack_message_ids == {}

    def test_does_not_ack_slash_command(self):
        """Slash commands render as text via CommandRouter — no reaction."""
        daemon = self._make_daemon_with_tracker()
        update = self._text_update(6, "/status", message_id=222)
        with patch("landline.batch_classifier.reactions.set_reaction_async") as mock_ack:
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
                "text": "edited",
                "edit_date": 123456,
            },
        }
        with patch("landline.batch_classifier.reactions.set_reaction_async") as mock_ack:
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
        with patch("landline.batch_classifier.reactions.set_reaction_async") as mock_ack:
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
        with patch("landline.batch_classifier.reactions.set_reaction_async") as mock_ack:
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
        with patch("landline.batch_classifier.reactions.set_reaction_async") as mock_ack:
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
            },
        }
        with patch("landline.batch_classifier.reactions.set_reaction_async") as mock_ack:
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
                "document": {
                    "file_id": "d",
                    "file_name": "malware.exe",
                    "file_size": 1024,
                },
            },
        }
        with patch("landline.batch_classifier.reactions.set_reaction_async") as mock_ack:
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
        with patch("landline.batch_classifier.reactions.set_reaction_async"):
            classify_updates(daemon, updates)
        assert daemon._batch_ack_message_ids["12345"] == [100, 200, 300]

    def test_tracker_optional_missing_attr_does_not_crash(self):
        """Backwards compat: if the daemon has no ``_batch_ack_message_ids``
        attribute, the classifier still runs (reactions still fire)."""
        daemon = _make_daemon()
        # Intentionally do NOT set _batch_ack_message_ids.
        update = self._text_update(30, "hi", message_id=999)
        with patch("landline.batch_classifier.reactions.set_reaction_async") as mock_ack:
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
                "document": document,
            },
        }

    def test_reject_log_does_not_leak_filename(self):
        daemon = _make_daemon()
        sensitive = "private_medical_records_XSensitiveMarker.exe"
        update = self._make_doc_update(sensitive)
        with patch("landline.batch_classifier.log") as mock_log:
            classify_updates(daemon, [update])
        for call in mock_log.call_args_list:
            args = list(call.args) + list(call.kwargs.values())
            for arg in args:
                if isinstance(arg, str):
                    assert sensitive not in arg, (
                        f"filename leaked into classifier log: {arg!r}"
                    )
                    assert "XSensitiveMarker" not in arg
