"""Tests for landline.client — Telegram transport, formatting, chunking, sending."""

import json
from unittest.mock import patch, MagicMock

from landline.client import (
    md_to_telegram_html,
    chunk_text,
    send_response,
    send_html,
    send_typing,
    _chunk_html,
    _parse_retry_after,
    _scan_open_tags_at,
    _send_chunk,
    _send_with_retry,
    _utf16_len,
)
from landline.config import (
    SEND_MAX_ATTEMPTS,
    SEND_RETRY_AFTER_CAP,
    SEND_RETRY_AFTER_FALLBACK,
)
from landline.telegram_fmt import pre


class TestMdToTelegramHtml:
    def test_bold(self):
        assert "<b>hello</b>" in md_to_telegram_html("**hello**")

    def test_italic(self):
        assert "<i>hello</i>" in md_to_telegram_html("*hello*")

    def test_bold_italic(self):
        result = md_to_telegram_html("***hello***")
        assert "<b><i>hello</i></b>" in result

    def test_strikethrough(self):
        assert "<s>gone</s>" in md_to_telegram_html("~~gone~~")

    def test_inline_code(self):
        result = md_to_telegram_html("use `foo()` here")
        assert "<code>foo()</code>" in result

    def test_fenced_code_block(self):
        md = "```python\nprint('hi')\n```"
        result = md_to_telegram_html(md)
        assert "<pre>" in result
        assert "print" in result
        assert "</pre>" in result

    def test_heading(self):
        result = md_to_telegram_html("## My Title")
        assert "<b>My Title</b>" in result

    def test_link(self):
        result = md_to_telegram_html("[click](https://example.com)")
        assert '<a href="https://example.com">click</a>' in result

    def test_link_with_quote_in_url_escaped(self):
        """Double quotes inside the URL must be escaped to avoid breaking the href."""
        result = md_to_telegram_html('[x](https://example.com/?a="b")')
        # href must not be broken by an unescaped double quote.
        assert '&quot;' in result or 'href="https://example.com/?a=' in result
        # Ensure the URL is wrapped in a single href attribute (no second `"` breaks).
        assert result.count('href="') == 1

    def test_non_http_link_not_converted(self):
        md = "[click](ftp://example.com)"
        result = md_to_telegram_html(md)
        assert "<a" not in result

    def test_html_entities_escaped(self):
        result = md_to_telegram_html("a < b & c > d")
        assert "&lt;" in result
        assert "&amp;" in result
        assert "&gt;" in result

    def test_inline_code_escapes_html(self):
        result = md_to_telegram_html("`<script>`")
        assert "<code>&lt;script&gt;</code>" in result

    def test_fenced_code_escapes_html(self):
        md = "```\n<div>&amp;</div>\n```"
        result = md_to_telegram_html(md)
        assert "&lt;div&gt;" in result

    def test_empty_input(self):
        assert md_to_telegram_html("") == ""

    def test_plain_text_passthrough(self):
        result = md_to_telegram_html("just plain text")
        assert "just plain text" in result

    def test_multiple_formatting(self):
        md = "**bold** and *italic* and `code`"
        result = md_to_telegram_html(md)
        assert "<b>bold</b>" in result
        assert "<i>italic</i>" in result
        assert "<code>code</code>" in result


class TestChunkText:
    def test_short_text_single_chunk(self):
        assert chunk_text("hello", 100) == ["hello"]

    def test_exact_limit_single_chunk(self):
        text = "x" * 100
        assert chunk_text(text, 100) == [text]

    def test_splits_on_double_newline(self):
        text = "a" * 50 + "\n\n" + "b" * 50
        chunks = chunk_text(text, 80)
        assert len(chunks) == 2
        assert chunks[0].strip() == "a" * 50
        assert chunks[1].strip() == "b" * 50

    def test_splits_on_single_newline(self):
        text = "a" * 50 + "\n" + "b" * 50
        chunks = chunk_text(text, 80)
        assert len(chunks) == 2

    def test_splits_on_space(self):
        text = "a" * 50 + " " + "b" * 50
        chunks = chunk_text(text, 80)
        assert len(chunks) == 2

    def test_hard_split_when_no_separator(self):
        text = "x" * 200
        chunks = chunk_text(text, 100)
        assert len(chunks) >= 2
        assert "".join(chunks) == text

    def test_empty_input(self):
        assert chunk_text("", 100) == [""]

    def test_default_limit_is_4096(self):
        text = "x" * 4096
        assert chunk_text(text) == [text]
        text2 = "x" * 4097
        assert len(chunk_text(text2)) >= 2

    def test_no_empty_chunks(self):
        text = "hello\n\n\n\nworld"
        chunks = chunk_text(text, 10)
        assert all(c for c in chunks)

    def test_all_chunks_within_limit(self):
        """Every produced chunk must respect the size cap."""
        text = ("a" * 30 + "\n\n") * 50  # ~1500 chars, with split points
        chunks = chunk_text(text, 100)
        for c in chunks:
            assert len(c) <= 100

    def test_hard_split_preserves_all_bytes(self):
        """When the text has no separators, concatenated chunks reconstruct it."""
        text = "x" * 200
        chunks = chunk_text(text, 100)
        assert "".join(chunks) == text


class TestSendResponse:
    def test_empty_text_noop(self, no_network):
        send_response("token", "123", "")
        no_network.assert_not_called()

    def test_whitespace_only_noop(self, no_network):
        send_response("token", "123", "   \n  ")
        no_network.assert_not_called()

    def test_sends_html_formatted(self, no_network):
        send_response("token", "123", "**bold** text")
        assert no_network.called
        # Inspect the actual request payload to verify HTML mode + content.
        req = no_network.call_args[0][0]
        body = json.loads(req.data.decode())
        assert body["chat_id"] == "123"
        assert body["parse_mode"] == "HTML"
        assert body["disable_web_page_preview"] is True
        assert "<b>bold</b>" in body["text"]
        # URL must target the token-authenticated sendMessage endpoint.
        assert "/bottoken/sendMessage" in req.full_url

    def test_429_retries_once_with_advertised_delay(self):
        """On 429, sleep for parameters.retry_after seconds and retry once."""
        import urllib.error
        error_429 = urllib.error.HTTPError(
            "url", 429, "rate limited", {},
            MagicMock(read=MagicMock(return_value=b'{"parameters":{"retry_after":5}}'))
        )
        call_count = [0]

        def side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise error_429
            resp = MagicMock()
            resp.read.return_value = b'{"ok":true}'
            return resp

        with patch("urllib.request.urlopen", side_effect=side_effect), \
             patch("time.sleep") as mock_sleep:
            send_response("token", "123", "hello")
        # Exactly two attempts: original + one retry.
        assert call_count[0] == 2
        # Slept for the advertised retry_after (clamped between 1 and 30).
        sleep_args = [c[0][0] for c in mock_sleep.call_args_list]
        assert 5 in sleep_args

    def test_429_retry_after_clamped_to_30(self):
        """Absurd retry_after values get clamped to 30 seconds."""
        import urllib.error
        error_429 = urllib.error.HTTPError(
            "url", 429, "rate limited", {},
            MagicMock(read=MagicMock(return_value=b'{"parameters":{"retry_after":9999}}'))
        )
        call_count = [0]

        def side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise error_429
            resp = MagicMock()
            resp.read.return_value = b'{"ok":true}'
            return resp

        with patch("urllib.request.urlopen", side_effect=side_effect), \
             patch("time.sleep") as mock_sleep:
            send_response("token", "123", "hello")
        sleep_args = [c[0][0] for c in mock_sleep.call_args_list]
        # Clamped to 30, not 9999.
        assert 30 in sleep_args
        assert 9999 not in sleep_args

    def test_400_falls_back_to_plain_text(self):
        """When HTML parse fails (400), retry the same chunk in plain text mode."""
        import urllib.error
        attempts = []

        def side_effect(req, **kwargs):
            body = json.loads(req.data.decode()) if req.data else {}
            attempts.append(body)
            if body.get("parse_mode") == "HTML":
                raise urllib.error.HTTPError(
                    "url", 400, "bad html", {},
                    MagicMock(read=MagicMock(return_value=b'{}')),
                )
            resp = MagicMock()
            resp.read.return_value = b'{"ok":true}'
            return resp

        with patch("urllib.request.urlopen", side_effect=side_effect):
            send_response("token", "123", "**bold**")
        # First attempt: HTML mode.
        assert attempts[0].get("parse_mode") == "HTML"
        # Final successful attempt: plain text (no parse_mode).
        assert "parse_mode" not in attempts[-1]
        # Plain-text body must be the raw markdown — not the HTML rendering.
        assert attempts[-1]["text"] == "**bold**"

    def test_5xx_retried_then_falls_back_to_plain(self):
        """A persistent 5xx is retried up to SEND_MAX_ATTEMPTS on the HTML
        send AND on the plain fallback. End-to-end: 2 * SEND_MAX_ATTEMPTS
        urlopen calls when both paths exhaust their retries."""
        import urllib.error
        call_count = [0]

        def side_effect(*args, **kwargs):
            call_count[0] += 1
            raise urllib.error.HTTPError(
                "url", 500, "server", {},
                MagicMock(read=MagicMock(return_value=b'{}')),
            )

        with patch("urllib.request.urlopen", side_effect=side_effect), \
             patch("time.sleep"):
            send_response("token", "123", "hello")
        # HTML attempt exhausts SEND_MAX_ATTEMPTS, then plain fallback also
        # exhausts SEND_MAX_ATTEMPTS.
        assert call_count[0] == 2 * SEND_MAX_ATTEMPTS

    def test_send_response_chunks_long_text(self):
        """Text longer than 4096 chars must produce multiple send calls."""
        call_count = [0]

        def side_effect(*args, **kwargs):
            call_count[0] += 1
            resp = MagicMock()
            resp.read.return_value = b'{"ok":true}'
            return resp

        long_text = "x" * 5000
        with patch("urllib.request.urlopen", side_effect=side_effect):
            send_response("token", "123", long_text)
        # At least two HTML sends for a 5000-char message at 4096-char limit.
        assert call_count[0] >= 2


class TestSendWithRetry:
    """Tests for the extracted _send_with_retry helper."""

    def test_success_first_try_no_retry(self):
        """A successful send needs no retry and no sleep."""
        with patch("landline.telegram_transport._send_chunk", return_value=(True, None, 0)) as mock_chunk, \
             patch("time.sleep") as mock_sleep:
            ok, code = _send_with_retry("token", "123", "hello", html_mode=True, label="t")
        assert ok is True
        assert code is None
        assert mock_chunk.call_count == 1
        mock_sleep.assert_not_called()

    def test_429_retries_up_to_send_max_attempts(self):
        """On a persistent 429, retry up to SEND_MAX_ATTEMPTS-1 times (total
        SEND_MAX_ATTEMPTS attempts) and then give up."""
        # Build a side_effect that always returns 429 — enough entries for
        # SEND_MAX_ATTEMPTS attempts.
        with patch(
            "landline.telegram_transport._send_chunk",
            side_effect=[(False, 429, 2)] * SEND_MAX_ATTEMPTS,
        ) as mock_chunk, \
             patch("time.sleep") as mock_sleep:
            ok, code = _send_with_retry("token", "123", "hello", html_mode=True, label="t")
        assert ok is False
        assert code == 429
        assert mock_chunk.call_count == SEND_MAX_ATTEMPTS
        # One sleep per retry (no sleep after the last attempt).
        assert mock_sleep.call_count == SEND_MAX_ATTEMPTS - 1
        # Each retry slept for the advertised retry_after (clamped).
        for call in mock_sleep.call_args_list:
            assert call[0][0] == 2

    def test_429_retry_delay_minimum_one_second(self):
        """retry_after=0 from the server still results in at least 1s sleep."""
        with patch("landline.telegram_transport._send_chunk", side_effect=[
            (False, 429, 0),
            (True, None, 0),
        ]), patch("time.sleep") as mock_sleep:
            _send_with_retry("token", "123", "hello", html_mode=True, label="t")
        assert mock_sleep.call_args[0][0] == 1

    def test_400_does_not_retry(self):
        """Non-429 errors return immediately without retry."""
        with patch("landline.telegram_transport._send_chunk", return_value=(False, 400, 0)) as mock_chunk, \
             patch("time.sleep") as mock_sleep:
            ok, code = _send_with_retry("token", "123", "hello", html_mode=True, label="t")
        assert ok is False
        assert code == 400
        assert mock_chunk.call_count == 1
        mock_sleep.assert_not_called()

    def test_5xx_retried_then_succeeds(self):
        """A transient 5xx is retried; the retry succeeds → overall success."""
        with patch("landline.telegram_transport._send_chunk", side_effect=[
            (False, 502, 0),
            (True, None, 0),
        ]) as mock_chunk, \
             patch("time.sleep") as mock_sleep:
            ok, code = _send_with_retry(
                "token", "123", "hello", html_mode=True, label="t",
            )
        assert ok is True
        assert mock_chunk.call_count == 2
        # Slept once before the successful retry — short, fixed backoff.
        assert mock_sleep.call_count == 1
        assert mock_sleep.call_args[0][0] >= 1  # SEND_RETRY_BACKOFF_SECONDS[0]

    def test_connection_error_retried_then_succeeds(self):
        """A connection/timeout error (code=None) is retried like a 5xx."""
        with patch("landline.telegram_transport._send_chunk", side_effect=[
            (False, None, 0),   # network error — looks like connection refused
            (True, None, 0),
        ]) as mock_chunk, \
             patch("time.sleep") as mock_sleep:
            ok, code = _send_with_retry(
                "token", "123", "hello", html_mode=True, label="t",
            )
        assert ok is True
        assert mock_chunk.call_count == 2
        assert mock_sleep.call_count == 1


class TestParseRetryAfter:
    """Tests for the `_parse_retry_after` helper added in R4. The header is
    checked FIRST (mirroring poller._poll_loop), then the JSON body, then the
    SEND_RETRY_AFTER_FALLBACK default. This is the bug-fix premise: a 429
    with ONLY the `Retry-After` HTTP header (no body field) was previously
    silently ignored."""

    def _make_429(self, body: bytes = b"", header_retry_after=None):
        """Build a urllib HTTPError with the given body and optional header.

        Telegram's actual HTTPError headers expose `.get("Retry-After")` —
        EmailMessage-style. We use a real `http.client.HTTPMessage` so the
        header lookup goes through the same code path as production.
        """
        import urllib.error
        import http.client
        headers = http.client.HTTPMessage()
        if header_retry_after is not None:
            headers["Retry-After"] = str(header_retry_after)
        err = urllib.error.HTTPError(
            "url", 429, "rate limited", headers,
            MagicMock(read=MagicMock(return_value=body)),
        )
        return err

    def test_header_only(self):
        """429 with ONLY a Retry-After header (no body field) is honored."""
        err = self._make_429(body=b"{}", header_retry_after=7)
        assert _parse_retry_after(err) == 7

    def test_header_wins_over_body(self):
        """Header takes precedence over the JSON body field."""
        err = self._make_429(
            body=b'{"parameters":{"retry_after":99}}',
            header_retry_after=4,
        )
        assert _parse_retry_after(err) == 4

    def test_body_only(self):
        """No header → fall through to the JSON body."""
        err = self._make_429(
            body=b'{"parameters":{"retry_after":12}}',
            header_retry_after=None,
        )
        assert _parse_retry_after(err) == 12

    def test_neither_falls_back_to_default(self):
        """Empty body, no header → SEND_RETRY_AFTER_FALLBACK default."""
        err = self._make_429(body=b"", header_retry_after=None)
        assert _parse_retry_after(err) == SEND_RETRY_AFTER_FALLBACK

    def test_invalid_header_falls_through_to_body(self):
        """Unparseable header value (e.g. an HTTP-date) is skipped; we use
        the body if it has a valid value."""
        err = self._make_429(
            body=b'{"parameters":{"retry_after":8}}',
            header_retry_after="Wed, 10 Jun 2026 14:30:00 GMT",
        )
        assert _parse_retry_after(err) == 8

    def test_fractional_header_clamped_to_at_least_one(self):
        """A fractional retry-after below 1 still produces at least 1s."""
        err = self._make_429(body=b"{}", header_retry_after="0.5")
        assert _parse_retry_after(err) == 1


class TestRetryAfterHeaderIntegration:
    """End-to-end: a 429 with ONLY a Retry-After HEADER (no body field) must
    propagate through _send_chunk to _send_with_retry so the retry waits the
    advertised duration. This is the primary bug-fix regression test."""

    def test_429_header_only_triggers_retry_with_header_delay(self):
        """A 429 with only the `Retry-After` header (no `parameters.retry_after`
        in the body) triggers a retry that sleeps for the header's value."""
        import urllib.error
        import http.client

        headers = http.client.HTTPMessage()
        headers["Retry-After"] = "6"
        error_429 = urllib.error.HTTPError(
            "url", 429, "rate limited", headers,
            MagicMock(read=MagicMock(return_value=b"{}")),
        )

        call_count = [0]

        def side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise error_429
            resp = MagicMock()
            resp.read.return_value = b'{"ok":true}'
            return resp

        with patch("urllib.request.urlopen", side_effect=side_effect), \
             patch("time.sleep") as mock_sleep:
            send_response("token", "123", "hello")

        # Retry happened.
        assert call_count[0] == 2
        # And the retry slept for the HEADER value (clamped to [1, CAP]).
        sleep_args = [c[0][0] for c in mock_sleep.call_args_list]
        assert 6 in sleep_args


class TestMultiChunkTruncation:
    """Observability: when a multi-chunk reply fails partway through, the
    daemon logs a clear truncation notice so partial deliveries are
    greppable in the log."""

    def test_mid_stream_failure_logs_truncation(self):
        """Chunk 2/3 fails after retries on BOTH HTML and plain — chunks
        2..3 will not be delivered. Log must say so."""
        import urllib.error

        # Two chunks worth of text. Use 4001+ char text so chunk_text(4000)
        # produces multiple chunks.
        text = ("a" * 3900) + "\n\n" + ("b" * 3900) + "\n\n" + ("c" * 3900)

        # First chunk: succeeds (HTML mode). Second chunk: fails everywhere
        # — HTML retries exhaust, plain fallback retries exhaust.
        send_index = [0]

        def side_effect(req, **kwargs):
            send_index[0] += 1
            body = json.loads(req.data.decode())
            text_field = body.get("text", "")
            # First chunk = starts with "a"; succeed.
            if text_field.startswith("a"):
                resp = MagicMock()
                resp.read.return_value = b'{"ok":true}'
                return resp
            # Everything else = 500 server error (transient — retried).
            raise urllib.error.HTTPError(
                "url", 500, "server", {},
                MagicMock(read=MagicMock(return_value=b'{}')),
            )

        logged: list = []

        with patch("urllib.request.urlopen", side_effect=side_effect), \
             patch("time.sleep"), \
             patch(
                 "landline.telegram_transport.log",
                 side_effect=lambda msg: logged.append(msg),
             ):
            send_response("token", "123", text)

        # Truncation notice MUST appear in the log, with index/total.
        truncation_logs = [
            m for m in logged if "aborted mid-stream" in m
        ]
        assert truncation_logs, (
            "expected mid-stream truncation log, got: %r" % logged
        )
        # And the notice should name the chunk index and total — the user
        # should be able to grep "chunk X/Y" to understand the gap.
        assert any("/3" in m for m in truncation_logs), (
            "truncation log should include `/3` (total chunks), got: %r"
            % truncation_logs
        )


class TestPreCodeRendering:
    """Regression: tool-status HTML built via `pre(text, lang)` must round-trip
    through `_chunk_html` as ONE intact HTML chunk — not degrade to plain text.

    Root cause history: `_scan_open_tags_at` classified close tags by name
    alone, so `</code>` popped from the simple stack even when the matching
    `<code class="language-...">` open had pushed to the complex stack. That
    left an orphan `code` on the complex stack and the chunker degraded every
    `pre(text, lang)` status to plain text — exposing already-escaped
    `&lt;&lt;` literally and losing the `<pre>` formatting.
    """

    def test_scan_open_tags_at_balanced_pre_code_returns_empty(self):
        """Balanced `<pre><code class=...>x</code></pre>` leaves both stacks empty."""
        html = '<pre><code class="language-Shell">x</code></pre>'
        simple, complex_ = _scan_open_tags_at(html, len(html))
        assert simple == []
        assert complex_ == []

    def test_chunk_html_roundtrips_shell_heredoc_status(self):
        """A Bash tool-status with `<<'EOF'` round-trips as one HTML chunk."""
        html = pre("cat <<'EOF' | wc -m", "Shell")
        # Sanity: the helper pushed `&lt;&lt;` into the body.
        assert "&lt;&lt;" in html
        result = _chunk_html(html, 4096)
        assert result == [(html, True)]

    def test_chunk_html_roundtrips_python_status(self):
        """A Python tool-status round-trips as one HTML chunk."""
        html = pre("def foo():\n    pass", "py")
        result = _chunk_html(html, 4096)
        assert result == [(html, True)]

    def test_chunker_recovers_to_html_after_pre_code_degrade(self):
        """Regression: after a Tier-2 degrade through `<pre><code class="...">`,
        the pending-stack walker must balance the `<code class="...">` open
        against `</code>` (mirroring the `_scan_open_tags_at` complex-first-
        then-simple rule) so subsequent safe content is NOT permanently stuck
        in plain-text degrade mode.

        Pre-fix bug: the walker classified close tags by
        `(name not in _REOPENABLE_SIMPLE_TAGS) or bool(attrs)`. Since closes
        carry no attrs, `</code>` always routed to the simple stack — but a
        `<code class="language-py">` open had pushed to the complex stack on
        degrade. Result: `pending_complex_closes=['code']` could never be
        popped, every subsequent piece stayed `is_html=False` plain text,
        and clean follow-up HTML was silently stripped.

        Construction:
          - A long `<pre><code class="language-py">...</code></pre>` block
            (>4096 UTF-16 units) FORCES at least one degrade cut inside the
            open `<code class="...">`, seeding `pending_complex_closes=['code']`
            and `pending_simple_closes=['pre']`.
          - Followed by safe HTML (`<b>follow-up</b>`).

        Post-fix expectation: at least one piece AFTER the code block is
        `is_html=True` — the walker balances both stacks once `</code>`
        and `</pre>` are consumed, and re-enters HTML mode.

        Pre-fix would fail this assertion: the safe follow-up never re-enters
        HTML mode (verified by reverting the walker locally — pieces are all
        `is_html=False`).
        """
        long_code = "x" * 5000  # >4096 → forces at least one degrade cut
        html = (
            '<pre><code class="language-py">' + long_code + '</code></pre>'
            "\n\nfollow-up paragraph: <b>safe HTML content here</b>."
        )
        pieces = _chunk_html(html, 4096)

        # Must split into multiple pieces.
        assert len(pieces) >= 2

        # The CORE invariant being locked in: at least one piece AFTER the
        # first must be is_html=True — i.e. the walker recovered to HTML
        # mode for the safe follow-up. This is what fails on the pre-fix
        # walker (every piece stays is_html=False because `</code>` never
        # pops from the complex pending stack).
        assert any(is_html for _, is_html in pieces[1:]), (
            "Walker stuck in plain-text degrade mode after `<pre><code "
            "class=...>...</code></pre>`; pieces=%r" % [
                (p[:40], h) for p, h in pieces]
        )

        # Safety invariant: every is_html=True piece has balanced tags;
        # every piece (any mode) fits the UTF-16 cap.
        for piece, is_html in pieces:
            assert _utf16_len(piece) <= 4096, (
                "piece exceeded UTF-16 cap: len=%d" % _utf16_len(piece))
            if is_html:
                assert TestChunkHtmlTier2Safety._tags_balanced(piece), (
                    "unbalanced HTML piece: %r" % piece[:120])

        # Lossless content: concatenating the visible text across all pieces
        # preserves the original visible text (after stripping tags from the
        # HTML pieces and the already-stripped plain pieces just used as-is).
        from landline.client import _strip_tags
        recovered = "".join(_strip_tags(p) if h else p for p, h in pieces)
        # The original visible text is the entire html with tags stripped.
        # Note: in plain-degrade pieces the chunker already calls _strip_tags,
        # so a second pass on plain pieces is a no-op.
        expected = _strip_tags(html)
        assert recovered == expected, (
            "visible-text loss after degrade+recover: pieces=%r" % [
                (p[:40], h) for p, h in pieces])

    def test_send_html_sends_bash_heredoc_in_html_mode(self):
        """End-to-end: a Bash status containing `<<'EOF'` reaches the wire as
        HTML, so `&lt;&lt;` is rendered as `<<` by Telegram, not exposed as
        literal `&lt;&lt;` plain text."""
        html = pre("cat <<'EOF' | wc -m", "Shell")
        with patch("landline.telegram_transport._send_with_retry", return_value=(True, None)) as mock_send:
            send_html("token", "123", html)
        # Exactly one HTML-mode send, with the full intact HTML payload.
        assert mock_send.call_count == 1
        args, kwargs = mock_send.call_args
        # Signature: (token, chat_id, text, html_mode=..., label=...)
        assert args[2] == html
        assert kwargs.get("html_mode") is True
        # And the body still has `&lt;&lt;` (escaped) — NOT stripped to literal `<<`.
        assert "&lt;&lt;" in args[2]

    # D1: `_strip_tags` must decode HTML entities in the plain-text degrade
    # path so Telegram users see the actual visible text (e.g. `Tom & Jerry`),
    # not literal entity strings (`Tom &amp; Jerry`). Telegram does NOT decode
    # entities when `parse_mode` is unset.

    def test_strip_tags_unescapes_named_entities(self):
        """D1: a degrade of `<a>Tom &amp; Jerry</a>` must decode `&amp;` to `&`."""
        from landline.client import _strip_tags
        assert _strip_tags('<a href="x">Tom &amp; Jerry</a>') == 'Tom & Jerry'

    def test_strip_tags_unescapes_lt_gt(self):
        """D1: `&lt;` / `&gt;` (the most common Telegram-escaped chars from
        code snippets) decode to `<` / `>` in the plain-text degrade."""
        from landline.client import _strip_tags
        assert _strip_tags('A &lt; B and B &gt; C') == 'A < B and B > C'

    def test_strip_tags_no_entities_is_noop(self):
        """D1: entity-free payloads pass through unchanged — `html.unescape`
        is documented no-op on inputs without entities. Guards against an
        over-eager replacement that mutates entity-free strings."""
        from landline.client import _strip_tags
        assert _strip_tags('<b>plain text only</b>') == 'plain text only'

    def test_strip_tags_single_pass_decode(self):
        """D1: single-pass decode semantic. `&amp;amp;` is the literal
        user-typed text `&amp;` and must survive decode as `&amp;`, not
        collapse to `&`. Pins the deliberately-rejected two-pass design.

        NOT framed as idempotence: `_strip_tags(_strip_tags(x))` on this
        input would decode `&amp;` to `&` on the second pass, so a
        fixed-point assertion would (correctly) fail on the implemented
        single-pass design. Assert the value, not `f(f(x)) == f(x)`.
        """
        from landline.client import _strip_tags
        assert _strip_tags('<a>Tom &amp;amp; Jerry</a>') == 'Tom &amp; Jerry'

    def test_send_html_plain_fallback_decodes_entities(self):
        """D1: when the HTML send returns 400 and `send_html` falls back to
        plain-text (`html_chunker.py:407`), the fallback body must have
        entities decoded — otherwise Telegram (in plain mode) renders
        `&amp;` literally."""
        html_payload = '<b>Tom &amp; Jerry</b>'
        # First call (HTML) returns (False, 400); second call (plain
        # fallback) returns (True, None). The plain-fallback body is what
        # we assert on.
        with patch(
            "landline.telegram_transport._send_with_retry",
            side_effect=[(False, 400), (True, None)],
        ) as mock_send:
            send_html("token", "123", html_payload)
        assert mock_send.call_count == 2
        # The plain fallback is the second call.
        plain_args, plain_kwargs = mock_send.call_args_list[1]
        # Signature: (token, chat_id, text, html_mode=..., label=...)
        assert plain_args[2] == 'Tom & Jerry'
        assert plain_kwargs.get("html_mode") is False

    def test_chunker_degrade_decodes_entities_in_plain_piece(self):
        """D1 (contract-level, revert-sensitive): a payload that EXCEEDS the
        4096-byte cap AND embeds a `&amp;` entity inside a tag spanning the
        cut must surface the decoded `&` in the plain-text degrade piece
        — never the raw `&amp;` literal.

        Construction (per D1 spec): the 5000-char prefix forces a degrade
        cut inside the open `<code class="...">`, so at least one piece is
        the plain-text degrade carrying the `Tom &amp; Jerry` substring.
        On a clean revert of D1 (drop `html.unescape` from `_strip_tags`),
        the `'&amp; Jerry' not in recovered` assertion flips — that's what
        makes this contract-level, not just unit-level. A `<= 4096` payload
        would skip degrade entirely and leave the assertion trivially
        satisfied by the HTML path, defeating the strengthening.
        """
        from landline.client import _strip_tags
        html = (
            '<pre><code class="language-py">'
            + ('x' * 5000)
            + ' Tom &amp; Jerry '
            + ('y' * 200)
            + '</code></pre>'
        )
        # Sanity: payload strictly exceeds the cap so degrade is forced.
        assert _utf16_len(html) > 4096
        pieces = _chunk_html(html, 4096)
        # Degrade did happen — anchors the >4096 assumption.
        assert len(pieces) >= 2
        recovered = "".join(_strip_tags(p) if h else p for p, h in pieces)
        # Decoded entity survives end-to-end through the degrade piece.
        assert 'Tom & Jerry' in recovered
        # Raw entity does NOT leak through — direct revert-sensitivity hook.
        # Pre-D1 (no `html.unescape`), the degrade piece carries the literal
        # `&amp; Jerry` substring and this assertion fires.
        assert '&amp; Jerry' not in recovered


class TestChunkHtmlTier2Safety:
    """The fix to `_scan_open_tags_at` MUST NOT regress Tier-2 chunking
    safety: genuinely-unsafe long content with an attribute-bearing open
    tag at the only safe cut still degrades to plain text, every is_html=True
    chunk has balanced tags, and every chunk respects the UTF-16 size cap.
    """

    @staticmethod
    def _tags_balanced(piece: str) -> bool:
        """True iff every open tag in `piece` has a later matching close.

        Tracks by-name innermost match (HTML-style nesting tolerance), same
        approach as the chunker harness.
        """
        from landline.client import _TAG_RE
        stack = []
        for m in _TAG_RE.finditer(piece):
            is_close = m.group(1) == "/"
            name = m.group(2).lower()
            if is_close:
                matched = False
                for i in range(len(stack) - 1, -1, -1):
                    if stack[i] == name:
                        stack.pop(i)
                        matched = True
                        break
                if not matched:
                    return False
            else:
                stack.append(name)
        return not stack

    def test_attribute_bearing_link_still_degrades_when_long(self):
        """A long `<a href="...">...</a>` span — non-reopenable, attribute-bearing
        — still produces at least one plain-text (degraded) piece when it
        straddles a cut. The fix must not make this safe-degrade go away."""
        html = '<a href="x">' + 5000 * "q" + "</a>"
        pieces = _chunk_html(html, 4096)
        # Must split into >1 piece given the length.
        assert len(pieces) >= 2
        # At least one piece must be a plain-text degrade — the chunker can't
        # safely emit `<a href="x">` on one chunk and `</a>` on another.
        assert any(not is_html for _, is_html in pieces)

    def test_all_html_pieces_balanced_and_within_cap(self):
        """Every is_html=True chunk has balanced tags, and EVERY chunk
        (html or degrade) is <= 4096 UTF-16 units."""
        cases = [
            pre("cat <<'EOF' | wc -m", "Shell"),
            pre("def foo():\n    pass", "py"),
            '<b>start <a href="x">' + 5000 * "q" + '</a> end</b>',
            '<pre>' + 5000 * "q" + '</pre>',
            '<a href="x">' + 5000 * "q" + '</a>',
            "🚀" * 3000,
        ]
        for html in cases:
            pieces = _chunk_html(html, 4096)
            for piece, is_html in pieces:
                assert _utf16_len(piece) <= 4096, (
                    "piece exceeded UTF-16 cap for input %r" % html[:60])
                if is_html:
                    assert self._tags_balanced(piece), (
                        "unbalanced HTML piece: %r" % piece[:120])

    def test_chunker_harness_if_present(self):
        """If `/tmp/chunker_harness.py` is present, run its full adversarial
        battery — it must exit cleanly (return code 0)."""
        import os
        import subprocess
        harness = "/tmp/chunker_harness.py"
        if not os.path.exists(harness):
            return  # not present in this environment; skip
        from landline.config import WORKSPACE
        result = subprocess.run(
            ["python3", harness],
            capture_output=True, text=True, cwd=str(WORKSPACE),
        )
        assert result.returncode == 0, (
            "chunker harness failed:\nstdout:\n%s\nstderr:\n%s" % (
                result.stdout, result.stderr))


class TestA4LogContentTransport:
    """A4: Transport error log lines must carry `chat_id` and `exc=<ClassName>`
    so incidents are grep-bisectable by victim and by exception class.

    Each test fails if its corresponding A4 edit in `telegram_transport.py`
    is reverted.
    """

    def test_send_chunk_error_log_includes_chat_id_and_exc_class(self):
        """Edit 1: `_send_chunk`'s catch-all `except Exception` log includes
        chat_id AND exc=<ClassName>."""
        import urllib.error

        logged: list = []
        with patch(
            "urllib.request.urlopen",
            side_effect=urllib.error.URLError("dns fail"),
        ), patch(
            "landline.telegram_transport.log",
            side_effect=lambda msg: logged.append(msg),
        ):
            _send_chunk("tok", "999111", "hi", html_mode=True)

        send_errors = [m for m in logged if "Telegram send error" in m]
        assert send_errors, "expected 'Telegram send error' log, got: %r" % logged
        assert any("chat_id=999111" in m for m in send_errors), (
            "send error log must include chat_id, got: %r" % send_errors
        )
        assert any("exc=URLError" in m for m in send_errors), (
            "send error log must include exc=URLError, got: %r" % send_errors
        )

    def test_send_response_400_log_includes_chat_id(self):
        """Edit 4: the 'Telegram 400 on HTML chunk' log carries chat_id."""
        import urllib.error
        attempts = []

        def side_effect(req, **kwargs):
            body = json.loads(req.data.decode()) if req.data else {}
            attempts.append(body)
            if body.get("parse_mode") == "HTML":
                raise urllib.error.HTTPError(
                    "url", 400, "bad html", {},
                    MagicMock(read=MagicMock(return_value=b"{}")),
                )
            resp = MagicMock()
            resp.read.return_value = b'{"ok":true}'
            return resp

        logged: list = []
        with patch("urllib.request.urlopen", side_effect=side_effect), \
             patch(
                 "landline.telegram_transport.log",
                 side_effect=lambda msg: logged.append(msg),
             ):
            send_response("token", "42", "**bold**")

        m = [x for x in logged if "Telegram 400 on HTML chunk" in x]
        assert m, "expected 'Telegram 400 on HTML chunk' log, got: %r" % logged
        assert any("chat_id=42" in x for x in m), (
            "400 log must include chat_id=42, got: %r" % m
        )

    def test_send_response_5xx_log_includes_chat_id(self):
        """Edit 5: the 'Telegram HTTP 502 on HTML send' fallback log carries chat_id."""
        import urllib.error

        def side_effect(*args, **kwargs):
            raise urllib.error.HTTPError(
                "url", 502, "bad gateway", {},
                MagicMock(read=MagicMock(return_value=b"{}")),
            )

        logged: list = []
        with patch("urllib.request.urlopen", side_effect=side_effect), \
             patch("time.sleep"), \
             patch(
                 "landline.telegram_transport.log",
                 side_effect=lambda msg: logged.append(msg),
             ):
            send_response("token", "42", "hello")

        m = [x for x in logged if "Telegram HTTP 502 on HTML send" in x]
        assert m, "expected 'Telegram HTTP 502 on HTML send' log, got: %r" % logged
        assert any("chat_id=42" in x for x in m), (
            "HTTP 502 fallback log must include chat_id=42, got: %r" % m
        )

    def test_send_response_network_error_log_includes_chat_id(self):
        """Edit 6: 'Telegram network error on HTML send' fallback log carries chat_id."""
        import urllib.error
        attempts = []

        def side_effect(req, **kwargs):
            body = json.loads(req.data.decode()) if req.data else {}
            attempts.append(body)
            if body.get("parse_mode") == "HTML":
                raise urllib.error.URLError("dns fail")
            resp = MagicMock()
            resp.read.return_value = b'{"ok":true}'
            return resp

        logged: list = []
        with patch("urllib.request.urlopen", side_effect=side_effect), \
             patch("time.sleep"), \
             patch(
                 "landline.telegram_transport.log",
                 side_effect=lambda msg: logged.append(msg),
             ):
            send_response("token", "42", "hello")

        m = [x for x in logged if "Telegram network error on HTML send" in x]
        assert m, "expected network error fallback log, got: %r" % logged
        assert any("chat_id=42" in x for x in m), (
            "network error fallback log must include chat_id=42, got: %r" % m
        )

    def test_send_response_multi_chunk_abort_log_includes_chat_id(self):
        """Edit 7: the mid-stream abort notice carries chat_id."""
        import urllib.error

        text = ("a" * 3900) + "\n\n" + ("b" * 3900) + "\n\n" + ("c" * 3900)

        def side_effect(req, **kwargs):
            body = json.loads(req.data.decode())
            text_field = body.get("text", "")
            if text_field.startswith("a"):
                resp = MagicMock()
                resp.read.return_value = b'{"ok":true}'
                return resp
            raise urllib.error.HTTPError(
                "url", 500, "server", {},
                MagicMock(read=MagicMock(return_value=b"{}")),
            )

        logged: list = []
        with patch("urllib.request.urlopen", side_effect=side_effect), \
             patch("time.sleep"), \
             patch(
                 "landline.telegram_transport.log",
                 side_effect=lambda msg: logged.append(msg),
             ):
            send_response("token", "424242", text)

        truncation_logs = [m for m in logged if "aborted mid-stream" in m]
        assert truncation_logs, (
            "expected mid-stream truncation log, got: %r" % logged
        )
        assert any("chat_id=424242" in m for m in truncation_logs), (
            "truncation log must include chat_id=424242, got: %r"
            % truncation_logs
        )

    def test_send_response_single_chunk_fail_log_includes_chat_id(self):
        """Edit 8: the single-chunk failure log carries chat_id."""
        import urllib.error

        def side_effect(*args, **kwargs):
            raise urllib.error.HTTPError(
                "url", 502, "server", {},
                MagicMock(read=MagicMock(return_value=b"{}")),
            )

        logged: list = []
        with patch("urllib.request.urlopen", side_effect=side_effect), \
             patch("time.sleep"), \
             patch(
                 "landline.telegram_transport.log",
                 side_effect=lambda msg: logged.append(msg),
             ):
            send_response("token", "77", "hello")

        m = [x for x in logged if "Telegram send failed" in x and "mid-stream" not in x]
        assert m, "expected 'Telegram send failed' log, got: %r" % logged
        assert any("chat_id=77" in x for x in m), (
            "single-chunk fail log must include chat_id=77, got: %r" % m
        )

    def test_send_with_retry_429_log_includes_chat_id(self):
        """Edit 2: the 429 retry log carries chat_id."""
        logged: list = []
        with patch(
            "landline.telegram_transport._send_chunk",
            side_effect=[(False, 429, 1), (True, None, 0)],
        ), patch("time.sleep"), patch(
            "landline.telegram_transport.log",
            side_effect=lambda msg: logged.append(msg),
        ):
            _send_with_retry("tok", "55", "hi", html_mode=True, label="HTML chunk")

        m = [x for x in logged if "Telegram 429 on HTML chunk" in x]
        assert m, "expected 'Telegram 429 on HTML chunk' log, got: %r" % logged
        assert any("chat_id=55" in x for x in m), (
            "429 retry log must include chat_id=55, got: %r" % m
        )

    def test_send_with_retry_5xx_log_includes_chat_id(self):
        """Edit 3: the 5xx/network retry log carries chat_id."""
        logged: list = []
        with patch(
            "landline.telegram_transport._send_chunk",
            side_effect=[(False, 502, 0), (True, None, 0)],
        ), patch("time.sleep"), patch(
            "landline.telegram_transport.log",
            side_effect=lambda msg: logged.append(msg),
        ):
            _send_with_retry("tok", "55", "hi", html_mode=True, label="HTML chunk")

        m = [x for x in logged if "Telegram HTTP 502 on HTML chunk" in x]
        assert m, "expected 'Telegram HTTP 502 on HTML chunk' log, got: %r" % logged
        assert any("chat_id=55" in x for x in m), (
            "5xx retry log must include chat_id=55, got: %r" % m
        )


class TestA4LogContentDownload:
    """A4: Download error log lines must carry `exc=<ClassName>`.

    `download_file` has no chat_id parameter (signature unchanged per the
    spec), so only the exception class is added — that's the relevant
    failure-class signal here.
    """

    def test_get_file_error_log_includes_exc_class(self):
        """Edit 9: getFile API call failure log includes exc=<ClassName>."""
        import urllib.error
        from landline.telegram_download import download_file

        logged: list = []
        with patch(
            "landline.telegram_download.telegram_api",
            side_effect=urllib.error.URLError("net"),
        ), patch(
            "landline.telegram_download.log",
            side_effect=lambda msg: logged.append(msg),
        ):
            result = download_file("tok", "AgACAgIA_fake_file_id_here", "x.jpg")
        assert result is None

        m = [x for x in logged if "getFile API call failed" in x]
        assert m, "expected 'getFile API call failed' log, got: %r" % logged
        assert any("exc=URLError" in x for x in m), (
            "getFile error log must include exc=URLError, got: %r" % m
        )

    def test_download_failure_log_includes_exc_class(self):
        """Edit 10: file download failure log includes exc=<ClassName>."""
        from landline.telegram_download import download_file

        good_resp = {
            "ok": True,
            "result": {"file_path": "photos/x.jpg"},
        }

        logged: list = []
        with patch(
            "landline.telegram_download.telegram_api",
            return_value=good_resp,
        ), patch(
            "urllib.request.urlopen",
            side_effect=OSError("disk full"),
        ), patch(
            "landline.telegram_download.log",
            side_effect=lambda msg: logged.append(msg),
        ):
            result = download_file("tok", "AgACAgIA_fake_file_id_here", "x.jpg")
        assert result is None

        m = [x for x in logged if "File download failed" in x]
        assert m, "expected 'File download failed' log, got: %r" % logged
        assert any("exc=OSError" in x for x in m), (
            "download failure log must include exc=OSError, got: %r" % m
        )


class TestSendTyping:
    def test_sends_typing_action(self):
        """Cluster 5: send_typing uses a pooled http.client.HTTPSConnection
        (not urllib.request.urlopen) on the happy path. Assert the request
        line/body/URL still describe a sendChatAction: typing."""
        import landline.telegram_transport as tt
        tt._reset_typing_conn()
        with patch("landline.telegram_transport.http.client.HTTPSConnection") as mock_cls:
            fake_conn = MagicMock()
            fake_resp = MagicMock(status=200)
            fake_resp.read.return_value = b'{"ok":true}'
            fake_conn.getresponse.return_value = fake_resp
            mock_cls.return_value = fake_conn
            send_typing("token", "123")
        assert fake_conn.request.called
        args, kwargs = fake_conn.request.call_args
        method = args[0]
        url = args[1]
        body = kwargs.get("body") if "body" in kwargs else args[2]
        assert method == "POST"
        assert "/bottoken/sendChatAction" in url
        parsed = json.loads(body.decode() if isinstance(body, bytes) else body)
        assert parsed["action"] == "typing"
        assert parsed["chat_id"] == "123"
        tt._reset_typing_conn()

    def test_survives_network_error(self):
        """A network failure during typing notice must not propagate.

        Cluster 5: the pooled HTTPSConnection can raise, then the one-shot
        ``telegram_api`` fallback also raises — send_typing must swallow
        both. This is the semantics-preserving contract for the pool
        change (matches pre-cluster behaviour when the network is down).
        """
        import landline.telegram_transport as tt
        tt._reset_typing_conn()
        with patch(
            "landline.telegram_transport.http.client.HTTPSConnection",
            side_effect=Exception("pool network"),
        ), patch(
            "urllib.request.urlopen", side_effect=Exception("fallback network"),
        ):
            send_typing("token", "123")  # must not raise
        tt._reset_typing_conn()


# ---------------------------------------------------------------------------
# Cluster 5 — outbound spool integration on _send_with_retry
# ---------------------------------------------------------------------------


class TestSpoolPersistOnSendWithRetry:
    """Cluster 5: every entry to _send_with_retry persists to the spool
    BEFORE the first attempt, marks success on ok, and marks failed on any
    terminal failure branch."""

    def test_send_with_retry_persists_before_send_and_deletes_on_success(self):
        """Patch outbound_spool.persist + mark_success; call _send_with_retry
        with a stub _send_chunk returning (True,None,0); assert persist called
        once, mark_success called once with the same spool_id, mark_failed
        never called."""
        with patch(
            "landline.telegram_transport.outbound_spool.persist",
            return_value="/tmp/fake-spool-id.json",
        ) as mock_persist, patch(
            "landline.telegram_transport.outbound_spool.mark_success",
        ) as mock_success, patch(
            "landline.telegram_transport.outbound_spool.mark_failed",
        ) as mock_failed, patch(
            "landline.telegram_transport._send_chunk",
            return_value=(True, None, 0),
        ):
            ok, code = _send_with_retry(
                "token", "42", "hello", html_mode=True, label="HTML chunk",
            )
        assert ok is True
        mock_persist.assert_called_once_with("42", "hello", True, "HTML chunk")
        mock_success.assert_called_once_with("/tmp/fake-spool-id.json")
        mock_failed.assert_not_called()

    def test_send_with_retry_leaves_file_pending_on_exhaustion(self):
        """REGRESSION: _send_chunk always returns (False,500,0); assert file
        remains pending after all attempts (mark_failed called once, on the
        terminal branch — this is the at-least-once persistence guarantee
        so the next daemon boot's replay pass picks it up)."""
        with patch(
            "landline.telegram_transport.outbound_spool.persist",
            return_value="/tmp/spool-abc.json",
        ), patch(
            "landline.telegram_transport.outbound_spool.mark_success",
        ) as mock_success, patch(
            "landline.telegram_transport.outbound_spool.mark_failed",
        ) as mock_failed, patch(
            "landline.telegram_transport._send_chunk",
            return_value=(False, 500, 0),
        ), patch("time.sleep"):
            ok, code = _send_with_retry(
                "token", "42", "hello", html_mode=True, label="HTML chunk",
            )
        assert ok is False
        assert code == 500
        # mark_failed called exactly once — the file survives to replay.
        assert mock_failed.call_count == 1
        assert mock_failed.call_args.args[0] == "/tmp/spool-abc.json"
        mock_success.assert_not_called()

    def test_send_with_retry_marks_failed_on_non_retryable_4xx(self):
        """A 400 (bad payload) is a non-retryable early return — mark_failed
        is still called so the replay pass can decide (it will unlink on a
        repeat 400)."""
        with patch(
            "landline.telegram_transport.outbound_spool.persist",
            return_value="/tmp/spool-400.json",
        ), patch(
            "landline.telegram_transport.outbound_spool.mark_success",
        ) as mock_success, patch(
            "landline.telegram_transport.outbound_spool.mark_failed",
        ) as mock_failed, patch(
            "landline.telegram_transport._send_chunk",
            return_value=(False, 400, 0),
        ):
            ok, code = _send_with_retry(
                "token", "42", "bad", html_mode=True, label="HTML chunk",
            )
        assert ok is False
        assert code == 400
        mock_failed.assert_called_once_with("/tmp/spool-400.json")
        mock_success.assert_not_called()

    def test_send_with_retry_survives_persist_disk_full(self):
        """Disk-full / persist raises OSError → send still proceeds; no
        mark_success or mark_failed is called (nothing was persisted)."""
        with patch(
            "landline.telegram_transport.outbound_spool.persist",
            side_effect=OSError("disk full"),
        ), patch(
            "landline.telegram_transport.outbound_spool.mark_success",
        ) as mock_success, patch(
            "landline.telegram_transport.outbound_spool.mark_failed",
        ) as mock_failed, patch(
            "landline.telegram_transport._send_chunk",
            return_value=(True, None, 0),
        ):
            ok, code = _send_with_retry(
                "token", "42", "hi", html_mode=True, label="HTML chunk",
            )
        assert ok is True
        mock_success.assert_not_called()
        mock_failed.assert_not_called()


class TestSpoolAndSendTypingPool:
    """Cluster 5: per-thread pooled HTTPSConnection for send_typing (and
    only send_typing)."""

    def test_send_typing_uses_pooled_https_connection(self):
        """Call send_typing twice; assert conn.request called twice on the
        SAME connection instance (no fresh handshake)."""
        import landline.telegram_transport as tt
        tt._reset_typing_conn()
        with patch(
            "landline.telegram_transport.http.client.HTTPSConnection",
        ) as mock_cls:
            fake_conn = MagicMock()
            fake_resp = MagicMock(status=200)
            fake_resp.read.return_value = b'{"ok":true}'
            fake_conn.getresponse.return_value = fake_resp
            mock_cls.return_value = fake_conn
            send_typing("token", "1")
            send_typing("token", "2")
        # HTTPSConnection constructed exactly once — connection is reused.
        assert mock_cls.call_count == 1
        # But request called twice on that same connection.
        assert fake_conn.request.call_count == 2
        tt._reset_typing_conn()

    def test_send_typing_falls_back_on_pool_error(self):
        """First pool call raises; assert one telegram_api fallback call
        and the local conn is reset to None."""
        import landline.telegram_transport as tt
        tt._reset_typing_conn()
        # Build a real conn placeholder, then mock the pool to raise on
        # first request(); the fallback path goes through telegram_api →
        # urllib.request.urlopen.
        with patch(
            "landline.telegram_transport.http.client.HTTPSConnection",
        ) as mock_cls, patch(
            "landline.telegram_transport.telegram_api",
        ) as mock_api:
            fake_conn = MagicMock()
            fake_conn.request.side_effect = OSError("stale keepalive")
            mock_cls.return_value = fake_conn
            send_typing("token", "1")
        # Fallback was invoked exactly once.
        assert mock_api.call_count == 1
        assert mock_api.call_args.args[1] == "sendChatAction"
        # After the failure the local conn is reset — a fresh conn will
        # be constructed on the next call.
        assert getattr(tt._typing_conn_local, "conn", None) is None
        tt._reset_typing_conn()

    def test_send_typing_per_thread_isolation(self):
        """Spawn two threads calling send_typing; each has its own
        _typing_conn_local.conn."""
        import threading
        import landline.telegram_transport as tt
        tt._reset_typing_conn()
        seen_conns = []
        lock = threading.Lock()

        def worker():
            with patch(
                "landline.telegram_transport.http.client.HTTPSConnection",
            ) as mock_cls:
                fake_conn = MagicMock()
                fake_resp = MagicMock(status=200)
                fake_resp.read.return_value = b'{"ok":true}'
                fake_conn.getresponse.return_value = fake_resp
                mock_cls.return_value = fake_conn
                send_typing("token", "42")
                # Each thread sees its own local — the pooled conn is
                # exactly the one this thread's mock returned.
                with lock:
                    seen_conns.append(id(tt._typing_conn_local.conn))
                tt._reset_typing_conn()

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        # Two threads, two distinct connection identities — threading.local
        # gives each thread its own attribute space.
        assert len(seen_conns) == 2
        assert seen_conns[0] != seen_conns[1]
        tt._reset_typing_conn()


class TestSendResponseNoDoubleSpoolOnFallback:
    """Regression: send_response used to persist BOTH the HTML variant and
    the plain-text fallback of the same logical chunk. When a transient
    5xx exhausted retries on both, two ``pending`` spool files survived
    → next replay pass delivered the same logical message twice."""

    def test_html_5xx_then_plain_5xx_leaves_only_one_pending_file(self, tmp_path):
        """Drive send_response with a stub _send_chunk that always returns
        (False, 500, 0). Both HTML and plain-text fallback exhaust their
        retries. Only the plain-text spool file (the last-attempted
        variant) may remain pending — the HTML one must be discarded so
        the replay pass delivers the message ONCE, not twice.
        """
        import landline.outbound_spool as spool_mod
        from landline.telegram_transport import send_response
        from landline.config import SPOOL_DIR

        # SPOOL_DIR is redirected to tmp by the autouse fixture; verify.
        assert str(SPOOL_DIR).startswith(str(tmp_path))

        with patch(
            "landline.telegram_transport._send_chunk",
            return_value=(False, 500, 0),
        ), patch("time.sleep"):
            send_response("token", "999", "hello")

        # Enumerate leftover spool files. Both attempts persisted a file at
        # entry; the fix discards the HTML variant before persisting the
        # plain-text fallback. Exactly one file must survive.
        leftover = [
            p for p in SPOOL_DIR.iterdir()
            if spool_mod._parse_spool_filename(p.name) is not None
        ]
        assert len(leftover) == 1, (
            "Expected exactly one pending spool file after HTML+plain "
            "double-5xx (double-spool regression), got %d: %s"
            % (len(leftover), [p.name for p in leftover])
        )
        # Its state must be ``pending`` so the replayer will pick it up.
        parsed = spool_mod._parse_spool_filename(leftover[0].name)
        assert parsed is not None
        assert parsed[2] == "pending"
        # And it must be the PLAIN-TEXT variant (html_mode False).
        import json as _json
        payload = _json.loads(leftover[0].read_text())
        assert payload["html_mode"] is False
        assert payload["chunk"] == "hello"

    def test_html_400_then_plain_success_discards_html_spool(self, tmp_path):
        """HTML fails with 400 (non-retryable); plain-text fallback
        succeeds. The plain-text spool is unlinked by mark_success and the
        HTML spool must be discarded by send_response — zero files must
        remain. (Prior behaviour left the HTML variant as ``pending`` for
        the replayer to attempt-and-discard on a repeat 400; the fix makes
        it deterministic: no orphaned files.)"""
        import landline.outbound_spool as spool_mod
        from landline.telegram_transport import send_response
        from landline.config import SPOOL_DIR

        # HTML returns 400 (non-retryable), plain returns 200.
        call_count = {"n": 0}

        def fake_send_chunk(token, chat_id, chunk, html_mode):
            call_count["n"] += 1
            if html_mode:
                return (False, 400, 0)
            return (True, None, 0)

        with patch(
            "landline.telegram_transport._send_chunk",
            side_effect=fake_send_chunk,
        ):
            send_response("token", "999", "hello")

        # HTML spool discarded, plain spool mark_success-unlinked → nothing left.
        leftover = [
            p for p in SPOOL_DIR.iterdir()
            if spool_mod._parse_spool_filename(p.name) is not None
        ]
        assert leftover == [], (
            "Expected zero spool files after HTML 400 + plain success, got %s"
            % [p.name for p in leftover]
        )


class TestOutboundSpoolDiscard:
    """The new outbound_spool.discard(spool_id) helper unlinks the spool
    file regardless of whether it's currently ``inflight-<pid>`` or has
    already been renamed to ``pending`` by mark_failed."""

    def test_discard_removes_pending_file(self, tmp_path):
        import landline.outbound_spool as spool_mod
        from landline.config import SPOOL_DIR

        spool_id = spool_mod.persist("42", "hello", True, "HTML chunk")
        # Simulate mark_failed → file is now in ``pending`` state.
        spool_mod.mark_failed(spool_id)
        # Before discard: exactly one file present (in pending state).
        entries = [p for p in SPOOL_DIR.iterdir()
                   if spool_mod._parse_spool_filename(p.name) is not None]
        assert len(entries) == 1
        assert spool_mod._parse_spool_filename(entries[0].name)[2] == "pending"
        # Discard by the original inflight spool_id — matches by created_ns+uid.
        spool_mod.discard(spool_id)
        entries = [p for p in SPOOL_DIR.iterdir()
                   if spool_mod._parse_spool_filename(p.name) is not None]
        assert entries == []

    def test_discard_removes_inflight_file(self, tmp_path):
        import landline.outbound_spool as spool_mod
        from landline.config import SPOOL_DIR

        spool_id = spool_mod.persist("42", "hello", True, "HTML chunk")
        # File is in inflight-<pid> state; no mark_failed yet.
        spool_mod.discard(spool_id)
        entries = [p for p in SPOOL_DIR.iterdir()
                   if spool_mod._parse_spool_filename(p.name) is not None]
        assert entries == []

    def test_discard_is_idempotent(self, tmp_path):
        import landline.outbound_spool as spool_mod

        spool_id = spool_mod.persist("42", "hello", True, "HTML chunk")
        spool_mod.discard(spool_id)
        # Second call must not raise even though the file is gone.
        spool_mod.discard(spool_id)


class TestSendResponseHtmlFailureRaceWithReplayer:
    """Regression: after HTML retries exhaust, ``send_response`` used to
    call ``mark_failed`` (rename inflight-<pid> → pending) inside
    ``_send_with_retry_tracked`` and only then call ``discard``. Between
    those two operations, the background OutboundSpoolReplayer could
    read the pending payload, rename to inflight, and send it — while
    ``discard`` unlinked the file. Net result: HTML variant delivered by
    the replayer AND plain-text fallback also delivered → user sees the
    same reply twice.

    Fix: ``send_response`` passes ``defer_failure_finalization=True`` to
    ``_send_with_retry_tracked``. The HTML spool file remains
    ``inflight-<pid>`` (invisible to the replayer, which only reads
    pending files) until ``send_response`` explicitly discards it.
    """

    def test_html_5xx_leaves_spool_in_inflight_state_until_discard(
        self, tmp_path,
    ):
        """After the HTML retry chain exhausts, no ``pending`` spool file
        must be visible — the replayer's per-tick scan of pending files
        would otherwise race the imminent ``discard`` call. The file MUST
        remain ``inflight-<pid>`` until the caller finalizes."""
        import landline.outbound_spool as spool_mod
        from landline.telegram_transport import _send_with_retry_tracked
        from landline.config import SPOOL_DIR

        assert str(SPOOL_DIR).startswith(str(tmp_path))

        with patch(
            "landline.telegram_transport._send_chunk",
            return_value=(False, 500, 0),
        ), patch("time.sleep"):
            ok, code, spool_id = _send_with_retry_tracked(
                "token", "999", "hello", html_mode=True,
                label="HTML chunk",
                defer_failure_finalization=True,
            )

        assert ok is False
        assert code == 500
        assert spool_id is not None

        # Enumerate leftover spool files. With defer_failure_finalization,
        # the file MUST NOT have been renamed to pending — it stays in
        # inflight-<pid> state so the background replayer's pending scan
        # cannot see it and race the caller's imminent discard.
        leftover = [
            p for p in SPOOL_DIR.iterdir()
            if spool_mod._parse_spool_filename(p.name) is not None
        ]
        assert len(leftover) == 1, (
            "Expected exactly one spool file after retry exhaustion, "
            "got %d: %s" % (len(leftover), [p.name for p in leftover])
        )
        parsed = spool_mod._parse_spool_filename(leftover[0].name)
        assert parsed is not None
        assert parsed[2].startswith("inflight-"), (
            "Spool file must remain in inflight-<pid> state (invisible to "
            "the pending-scanning replayer) with defer_failure_finalization=True, "
            "got state=%r" % parsed[2]
        )

    def test_send_response_html_5xx_replayer_scan_sees_no_pending_before_discard(
        self, tmp_path,
    ):
        """End-to-end: drive ``send_response`` with a stub ``_send_chunk``
        that fails HTML (5xx) then succeeds plain-text. Between the HTML
        exhaustion and the plain-text send, a snapshot of ``pending``
        spool files must be empty — proving the replayer would have
        found nothing to pick up in the race window. Before the fix,
        the HTML variant sat as ``pending`` in this window."""
        import landline.outbound_spool as spool_mod
        from landline.telegram_transport import send_response
        from landline.config import SPOOL_DIR

        assert str(SPOOL_DIR).startswith(str(tmp_path))

        pending_snapshots = {"between": None}
        call_count = {"n": 0}

        def fake_send_chunk(token, chat_id, chunk, html_mode):
            call_count["n"] += 1
            if html_mode:
                return (False, 500, 0)
            # First plain-text attempt: sample pending files just before
            # this send succeeds — matches the exact moment the racy
            # replayer tick would have run in production.
            if pending_snapshots["between"] is None:
                pending_snapshots["between"] = [
                    p.name for p in SPOOL_DIR.iterdir()
                    if spool_mod._parse_spool_filename(p.name) is not None
                    and spool_mod._parse_spool_filename(p.name)[2] == "pending"
                ]
            return (True, None, 0)

        with patch(
            "landline.telegram_transport._send_chunk",
            side_effect=fake_send_chunk,
        ), patch("time.sleep"):
            send_response("token", "999", "hello")

        assert pending_snapshots["between"] == [], (
            "Between HTML exhaustion and plain-text send, NO spool file "
            "should be in the ``pending`` state — otherwise the "
            "OutboundSpoolReplayer could race the pending scan against "
            "send_response's discard and double-deliver the HTML variant. "
            "Found pending: %s" % pending_snapshots["between"]
        )
