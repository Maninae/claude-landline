"""Tests for landline.media.photo — locked-batch bail-outs must clear
the classifier's 👀 acks, matching voice_handler / document_handler.

The finding: ``dispatch_photo_group`` used to bail out on
``_check_lock_gate`` without clearing the 👀 acks the classifier had
already fired on every photo message. That left the 👀 stuck on the operator's
photos forever with no matching 👌 or CLEAR — violating the
"every 👀 gets a matching 👌 (or CLEAR on rejection)" invariant the
other handlers already enforce.
"""

from unittest.mock import MagicMock, patch

from landline.media.photo import (
    dispatch_photo_group,
    process_photo_batch,
)


def _make_daemon_stub():
    daemon = MagicMock()
    daemon.token = "fake-token"
    daemon._check_lock_gate = MagicMock(return_value=False)
    daemon._send_response = MagicMock()
    daemon._advance_update_cursor = MagicMock()
    daemon._inject_and_dispatch = MagicMock()
    return daemon


def _photo_msg(mid, file_id="p-1", media_group_id=None):
    msg = {
        "chat": {"id": 12345},
        "message_id": mid,
        "photo": [{"file_id": file_id, "file_size": 1024}],
    }
    if media_group_id:
        msg["media_group_id"] = media_group_id
    return msg


class TestDispatchPhotoGroupLockGateClearsAcks:
    def test_locked_gate_clears_acks_on_standalone_photo(self):
        """A locked session bail-out on a standalone photo must CLEAR the
        👀 ack — otherwise it lingers forever with no matching 👌."""
        daemon = _make_daemon_stub()
        daemon._check_lock_gate = MagicMock(return_value=True)
        messages = [_photo_msg(mid=6001)]
        with patch(
            "landline.media.photo.reactions.set_reactions_batch_async",
        ) as mock_batch_clear, patch(
            "landline.orchestrator.download_file",
        ) as mock_dl:
            dispatch_photo_group(daemon, messages, [301], "12345")
        # No download happened, no Claude dispatch.
        mock_dl.assert_not_called()
        daemon._inject_and_dispatch.assert_not_called()
        # 👀 was CLEARED on the photo message_id (emoji=None argument).
        clear_calls = [
            c for c in mock_batch_clear.call_args_list if c.args[3] is None
        ]
        assert len(clear_calls) == 1
        cleared_ids = list(clear_calls[0].args[2])
        assert cleared_ids == [6001]

    def test_locked_gate_clears_acks_on_album(self):
        """A locked session bail-out on an album must CLEAR the 👀 acks
        on EVERY photo message in the album — not just the first."""
        daemon = _make_daemon_stub()
        daemon._check_lock_gate = MagicMock(return_value=True)
        messages = [
            _photo_msg(mid=6101, file_id="a", media_group_id="album-B"),
            _photo_msg(mid=6102, file_id="b", media_group_id="album-B"),
            _photo_msg(mid=6103, file_id="c", media_group_id="album-B"),
        ]
        with patch(
            "landline.media.photo.reactions.set_reactions_batch_async",
        ) as mock_batch_clear, patch(
            "landline.orchestrator.download_file",
        ) as mock_dl:
            dispatch_photo_group(daemon, messages, [401, 402, 403], "12345")
        mock_dl.assert_not_called()
        daemon._inject_and_dispatch.assert_not_called()
        clear_calls = [
            c for c in mock_batch_clear.call_args_list if c.args[3] is None
        ]
        assert len(clear_calls) == 1
        cleared_ids = sorted(clear_calls[0].args[2])
        assert cleared_ids == [6101, 6102, 6103]


class TestProcessPhotoBatchLockedBailsClear:
    """End-to-end through process_photo_batch: a locked session must not
    leak 👀 emojis, no matter how the classifier bucketed the photos."""

    def test_locked_standalone_batch_clears_acks(self):
        daemon = _make_daemon_stub()
        daemon._check_lock_gate = MagicMock(return_value=True)
        updates = [
            (_photo_msg(mid=6201, file_id="x"), 501, "12345"),
            (_photo_msg(mid=6202, file_id="y"), 502, "12345"),
        ]
        with patch(
            "landline.media.photo.reactions.set_reactions_batch_async",
        ) as mock_batch_clear, patch(
            "landline.orchestrator.download_file",
        ):
            process_photo_batch(daemon, updates)
        daemon._inject_and_dispatch.assert_not_called()
        cleared_all = []
        for c in mock_batch_clear.call_args_list:
            if c.args[3] is None:
                cleared_all.extend(list(c.args[2]))
        assert sorted(cleared_all) == [6201, 6202]
