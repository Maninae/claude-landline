"""Tag-aware HTML chunker for Telegram messages.

Splits HTML payloads into Telegram-safe chunks without cutting between `<` and
`>`, prefers `\n` boundaries, closes + reopens simple formatting tags across
chunk boundaries, and degrades to plain text for any piece that can't be split
safely (e.g. an `<a href="...">` open at the only viable cut). Also exposes
`send_html`, the transport-layer entry point that drives the chunker and
forwards each piece through `_send_with_retry`.
"""

import html
import re
from typing import List, Optional, Tuple


# -----------------------------------------------------------------------------
# UTF-16 sizing — Telegram's actual length unit
# -----------------------------------------------------------------------------

def _utf16_len(s: str) -> int:
    """Length of `s` in UTF-16 code units — Telegram's actual size unit.

    A char above U+FFFF (e.g. emoji 🚀) counts as 2 UTF-16 units. Using
    `len(s)` (Python code points) under-counts these and lets us blow past
    Telegram's 4096 cap, producing a 400.
    """
    return len(s.encode("utf-16-le")) // 2


# -----------------------------------------------------------------------------
# Tag-aware HTML chunker
# -----------------------------------------------------------------------------

# Telegram-supported simple formatting tags that we re-balance across chunk
# boundaries. We deliberately do NOT try to reopen tags that carry attributes
# (e.g. `<a href="...">`, `<pre><code class="language-...">`) — that's the
# "complex/risky" piece we keep out of this helper. If such a tag is open at
# the cut, we degrade that chunk to plain text rather than emit broken HTML.
_REOPENABLE_SIMPLE_TAGS = ("pre", "code", "b", "i", "blockquote")

# Match an HTML tag: opening (with optional attrs), closing, or self-closing.
_TAG_RE = re.compile(r"<(/?)([a-zA-Z][a-zA-Z0-9]*)([^>]*)>")


def _index_inside_tag(s: str, idx: int) -> bool:
    """True if `idx` falls strictly between a `<` and its matching `>`.

    A cut at exactly `<` or exactly `>` is fine — it doesn't split a tag.
    A cut between them does.
    """
    if idx <= 0 or idx >= len(s):
        return False
    # Look backwards: most recent `<` vs most recent `>` before idx.
    last_lt = s.rfind("<", 0, idx)
    last_gt = s.rfind(">", 0, idx)
    return last_lt > last_gt


def _scan_open_tags_at(s: str, idx: int) -> Tuple[List[str], List[str]]:
    """Walk tags in `s[0:idx]` and return `(simple_stack, complex_stack)` —
    the lists of currently-open simple reopenable tags and currently-open
    complex (attribute-bearing / non-reopenable) tags at `idx`, both in
    open order (outer→inner).

    A complex tag pushed and then closed before `idx` does NOT linger in
    `complex_stack` — that's the Bug 1 fix. Only tags that are *actually
    open* at `idx` count.
    """
    simple_stack: List[str] = []
    complex_stack: List[str] = []
    for m in _TAG_RE.finditer(s, 0, idx):
        is_close = m.group(1) == "/"
        name = m.group(2).lower()
        attrs = m.group(3).strip()
        if is_close:
            # A close tag carries no attrs, so its name alone can't tell us
            # which stack its matching open lives on — e.g. `</code>` may
            # match either a plain `<code>` (simple) or a
            # `<code class="...">` (complex). Pop the innermost matching
            # name from EITHER stack (complex first, then simple) so an
            # attribute-bearing open balances correctly.
            popped = False
            for i in range(len(complex_stack) - 1, -1, -1):
                if complex_stack[i] == name:
                    complex_stack.pop(i)
                    popped = True
                    break
            if not popped:
                for i in range(len(simple_stack) - 1, -1, -1):
                    if simple_stack[i] == name:
                        simple_stack.pop(i)
                        break
            continue
        is_complex = (name not in _REOPENABLE_SIMPLE_TAGS) or bool(attrs)
        if is_complex:
            complex_stack.append(name)
        else:
            simple_stack.append(name)
    return simple_stack, complex_stack


def _open_simple_tags_at(s: str, idx: int) -> Optional[List[str]]:
    """Return the stack of currently-open simple reopenable tags at byte
    position `idx` in `s` (open order, outer→inner), or `None` if it is
    unsafe to split at `idx` because a complex / attribute-bearing tag is
    actually open at that position.

    A fully-balanced complex tag *before* `idx` (e.g. `<a href="x">x</a>`
    that closes before the cut) is fine — it does not poison subsequent
    cuts.
    """
    simple_stack, complex_stack = _scan_open_tags_at(s, idx)
    if complex_stack:
        return None
    return simple_stack


def _strip_tags(s: str) -> str:
    """Strip all HTML tags from `s` and decode HTML entities (plain-text
    degrade path).

    The `_TAG_RE.sub("", s)` deletes the tags; `html.unescape(...)` then
    decodes any HTML entities the payload carried (`&amp;`, `&lt;`,
    `&gt;`, numeric refs, the full HTML5 named-entity set). Without the
    unescape, a degrade of `<a>Tom &amp; Jerry</a>` would reach Telegram
    as the literal string `Tom &amp; Jerry` in plain-text mode — Telegram
    doesn't decode entities when `parse_mode` is unset, so the user sees
    raw `&amp;`. `html.unescape` is a no-op on entity-free payloads.

    Single-pass: `_strip_tags` is intentionally NOT idempotent on inputs
    containing nested-looking entities (`&amp;amp;` decodes to `&amp;`
    on first call, then to `&` on a second call). Single-pass matches
    what a browser would render — `&amp;amp;` is the literal user-typed
    text `&amp;` and must survive as `&amp;`, not collapse to `&`.
    """
    return html.unescape(_TAG_RE.sub("", s))


def _chunk_html(html: str, limit: int = 4096) -> List[Tuple[str, bool]]:
    """Split `html` into Telegram-safe chunks.

    Returns a list of `(chunk_text, is_html)` tuples:
      - `is_html=True`: send as HTML (well-formed, tags balanced).
      - `is_html=False`: send as plain text (a piece we couldn't split safely).

    Rules (minimal, conservative):
      1. Size by UTF-16 code units (Telegram's real cap).
      2. Never cut between a `<` and its matching `>`.
      3. Prefer `\n` boundaries.
      4. If a simple formatting tag (`<pre>`, `<code>`, `<b>`, `<i>`,
         `<blockquote>`) is open at the cut, close it at the end of the chunk
         and reopen it at the start of the next chunk.
      5. If a tag with attributes is open at the only safe cut point, degrade
         that piece to plain text (strip tags) rather than emit broken HTML.
    """
    if _utf16_len(html) <= limit:
        simple_end, complex_end = _scan_open_tags_at(html, len(html))
        if complex_end:
            return [(_strip_tags(html), False)]
        if simple_end:
            closers = "".join("</%s>" % n for n in reversed(simple_end))
            return [(html + closers, True)]
        return [(html, True)]

    chunks: List[Tuple[str, bool]] = []
    remaining = html
    reopen_prefix = ""  # tags to prepend to the next chunk
    # Complex/non-reopenable tags that were OPEN at a prior degrade cut and
    # whose matching closes have not yet been consumed. While this list is
    # non-empty, we MUST keep emitting plain-text (tags stripped) chunks to
    # avoid letting an orphan `</a>` (or similar) escape into a later HTML
    # chunk. Each entry is a tag name; matching `</name>` in the order they
    # appear in `remaining` pops the innermost matching entry. That's the
    # Bug 2 fix.
    pending_complex_closes: List[str] = []
    # Simple reopenable tags (e.g. `<b>`, `<i>`) that were OPEN at a prior
    # degrade cut. Their open was stripped from the plain-text degrade
    # piece, so we must also keep degrading until their matching closes
    # are consumed — otherwise the orphan `</b>` escapes into a later HTML
    # chunk as unbalanced HTML. Parallel to `pending_complex_closes`.
    pending_simple_closes: List[str] = []

    while remaining:
        # If any tag (simple or complex) is still "in flight" from a prior
        # degraded chunk, stay in plain-text mode until we've consumed its
        # matching close. Track BOTH stacks; only re-enter HTML mode once
        # BOTH are empty.
        if pending_complex_closes or pending_simple_closes:
            # Scan tags in `remaining` to find the position at which all
            # currently-open tags (complex + simple) become balanced (or
            # the end of `remaining` if they never do — defensive).
            complex_stack_scan = list(pending_complex_closes)
            simple_stack_scan = list(pending_simple_closes)
            consume_end = len(remaining)
            for m in _TAG_RE.finditer(remaining):
                if not complex_stack_scan and not simple_stack_scan:
                    consume_end = m.start()
                    break
                is_close = m.group(1) == "/"
                name = m.group(2).lower()
                attrs = m.group(3).strip()
                if is_close:
                    # A close tag carries no attrs, so its name alone can't
                    # tell us which stack its matching open lives on. Mirror
                    # `_scan_open_tags_at`: try the complex stack first, then
                    # fall back to simple. Otherwise `<code class="x">` (on
                    # complex) and a later `</code>` (which `bool(attrs)`
                    # makes look simple) never balance, and the walker stays
                    # stuck in plain-text degrade mode.
                    popped = False
                    for i in range(len(complex_stack_scan) - 1, -1, -1):
                        if complex_stack_scan[i] == name:
                            complex_stack_scan.pop(i)
                            popped = True
                            break
                    if not popped:
                        for i in range(len(simple_stack_scan) - 1, -1, -1):
                            if simple_stack_scan[i] == name:
                                simple_stack_scan.pop(i)
                                break
                    if not complex_stack_scan and not simple_stack_scan:
                        consume_end = m.end()
                        break
                else:
                    is_complex = (name not in _REOPENABLE_SIMPLE_TAGS) or bool(attrs)
                    if is_complex:
                        complex_stack_scan.append(name)
                    else:
                        simple_stack_scan.append(name)
            # Cap each degraded plain-text piece by the size budget so we
            # never emit a chunk above the Telegram cap.
            cap = limit
            piece = remaining[:consume_end]
            # Find the largest code-point prefix of piece that fits `cap`
            # UTF-16 units.
            if _utf16_len(piece) > cap:
                lo, hi = 0, len(piece)
                while lo < hi:
                    mid = (lo + hi + 1) // 2
                    if _utf16_len(piece[:mid]) <= cap:
                        lo = mid
                    else:
                        hi = mid - 1
                piece = piece[:lo]
                consume_end = lo
            # Rescan tags inside `piece` (the actually-consumed span) to
            # update BOTH pending stacks accurately — the size cap may have
            # truncated before all opens/closes were seen.
            new_pending_complex = list(pending_complex_closes)
            new_pending_simple = list(pending_simple_closes)
            for m in _TAG_RE.finditer(piece):
                is_close = m.group(1) == "/"
                name = m.group(2).lower()
                attrs = m.group(3).strip()
                if is_close:
                    # Same complex-first-then-simple rule as the
                    # balance-point walker above: closes carry no attrs,
                    # so try popping the innermost matching name from the
                    # complex stack first, else from the simple stack.
                    popped = False
                    for i in range(len(new_pending_complex) - 1, -1, -1):
                        if new_pending_complex[i] == name:
                            new_pending_complex.pop(i)
                            popped = True
                            break
                    if not popped:
                        for i in range(len(new_pending_simple) - 1, -1, -1):
                            if new_pending_simple[i] == name:
                                new_pending_simple.pop(i)
                                break
                else:
                    is_complex = (name not in _REOPENABLE_SIMPLE_TAGS) or bool(attrs)
                    if is_complex:
                        new_pending_complex.append(name)
                    else:
                        new_pending_simple.append(name)
            pending_complex_closes = new_pending_complex
            pending_simple_closes = new_pending_simple
            chunks.append((_strip_tags(piece), False))
            remaining = remaining[consume_end:]
            reopen_prefix = ""
            continue

        body = reopen_prefix + remaining
        if _utf16_len(body) <= limit:
            simple_end, complex_end = _scan_open_tags_at(body, len(body))
            if complex_end:
                chunks.append((_strip_tags(body), False))
            else:
                closers = "".join("</%s>" % n for n in reversed(simple_end))
                chunks.append((body + closers, True))
            break

        # Reserve a small headroom for closing tags we may need to append
        # after the cut (so the final chunk stays under `limit`). The
        # reopenable simple tags are short: `</blockquote>` is 13 chars, and
        # in practice no more than a couple are nested at once. Reserve 64
        # UTF-16 units — well under `limit` but enough for any realistic
        # nested-tag close sequence.
        closer_headroom = 64
        effective_limit = max(limit - closer_headroom, limit // 2)

        # Find the largest code-point prefix of `body` whose UTF-16 length
        # fits `effective_limit`.
        lo, hi = 0, len(body)
        while lo < hi:
            mid = (lo + hi + 1) // 2
            if _utf16_len(body[:mid]) <= effective_limit:
                lo = mid
            else:
                hi = mid - 1
        window_end = lo
        if window_end <= len(reopen_prefix):
            # Can't fit even the reopen prefix + 1 char — degrade this piece
            # by stripping tags entirely from a fixed-size slice and moving on.
            # This is a pathological case (huge reopen prefix vs tiny limit).
            slice_end = min(len(remaining), limit)
            simple_at_slice, complex_at_slice = _scan_open_tags_at(remaining, slice_end)
            if complex_at_slice:
                pending_complex_closes = list(complex_at_slice)
            if simple_at_slice:
                pending_simple_closes = list(simple_at_slice)
            chunks.append((_strip_tags(remaining[:slice_end]), False))
            remaining = remaining[slice_end:]
            reopen_prefix = ""
            continue

        # Walk back from window_end to find a cut that:
        #  (a) is not inside a tag, and
        #  (b) ideally lands on a `\n` boundary.
        # Try `\n` first, then any non-tag-interior position, then degrade.
        def _safe_cut(end: int, prefer_char: str = "") -> int:
            i = end
            while i > 0:
                if prefer_char:
                    j = body.rfind(prefer_char, 0, i)
                    if j < 0:
                        return -1
                    if not _index_inside_tag(body, j + len(prefer_char)):
                        return j + len(prefer_char)
                    i = j
                else:
                    if not _index_inside_tag(body, i):
                        return i
                    i -= 1
            return -1

        cut = _safe_cut(window_end, prefer_char="\n")
        sep_len = 0  # we keep the `\n` with the previous chunk; no separator to skip
        if cut < 0:
            # No newline boundary worked — accept any non-tag-interior cut.
            cut = _safe_cut(window_end)
        if cut < 0 or cut <= len(reopen_prefix):
            # Couldn't find any safe HTML cut. Degrade the body to plain text
            # at the size boundary and move on.
            simple_at_end, complex_at_end = _scan_open_tags_at(body, window_end)
            if complex_at_end:
                pending_complex_closes = list(complex_at_end)
            if simple_at_end:
                pending_simple_closes = list(simple_at_end)
            chunks.append((_strip_tags(body[:window_end]), False))
            remaining = remaining[window_end - len(reopen_prefix):]
            reopen_prefix = ""
            continue

        # Inspect open simple tags at the cut to decide
        # close-here / reopen-next-chunk.
        simple_stack, complex_stack = _scan_open_tags_at(body, cut)
        if complex_stack:
            # An unsafe (attribute-bearing or unsupported) tag is open at
            # the cut. Degrade this piece to plain text, and remember which
            # complex AND simple tags are still in flight so subsequent
            # iterations keep degrading until their matching closes are
            # consumed (otherwise the orphan `</a>` or `</b>` etc. escapes
            # as HTML).
            pending_complex_closes = list(complex_stack)
            if simple_stack:
                pending_simple_closes = list(simple_stack)
            chunks.append((_strip_tags(body[:cut]), False))
            remaining = remaining[cut - len(reopen_prefix) + sep_len:]
            reopen_prefix = ""
            continue
        open_stack = simple_stack

        # Build chunk: body[:cut] + closing tags in reverse order.
        closers = "".join("</%s>" % name for name in reversed(open_stack))
        chunk = body[:cut] + closers
        chunks.append((chunk, True))

        # Advance `remaining`: body = reopen_prefix + remaining, so the
        # portion of `remaining` consumed by this chunk is
        # `cut - len(reopen_prefix)`.
        consumed_from_remaining = cut - len(reopen_prefix)
        remaining = remaining[consumed_from_remaining + sep_len:]

        # Set up next chunk: reopen tags in original order.
        reopen_prefix = "".join("<%s>" % name for name in open_stack)

    # Final cleanup: drop empty pieces.
    return [(c, h) for (c, h) in chunks if c]


def send_html(token: str, chat_id: str, html: str) -> None:
    """Send pre-formatted HTML to Telegram. No markdown conversion.

    Chunking is tag-aware: we never cut between `<` and `>`, prefer `\n`
    boundaries, close + reopen simple formatting tags across chunk
    boundaries, and degrade to plain text for any piece we can't split
    safely (e.g. an `<a href="...">` open at the only viable cut).
    """
    # Import lazily to avoid a circular import with `telegram_transport`,
    # which itself imports `_utf16_len` from this module.
    from landline.telegram.transport import _send_with_retry

    if not html or not html.strip():
        return
    for chunk, is_html in _chunk_html(html, 4096):
        if is_html:
            ok, code = _send_with_retry(
                token, chat_id, chunk, html_mode=True, label="HTML direct",
            )
            if not ok and code == 400:
                _send_with_retry(
                    token, chat_id, _strip_tags(chunk),
                    html_mode=False, label="plain fallback",
                )
        else:
            _send_with_retry(
                token, chat_id, chunk, html_mode=False, label="plain degrade",
            )
