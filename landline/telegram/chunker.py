"""Tag-aware HTML chunker for Telegram messages.

Splits HTML payloads into Telegram-safe chunks without cutting between ``<``
and ``>``, prefers ``\\n`` boundaries, closes + reopens simple formatting
tags across chunk boundaries, and degrades to plain text for any piece that
can't be split safely (e.g. an ``<a href="...">`` open at the only viable
cut). Also exposes ``send_html``, the transport-layer entry point that
drives the chunker and forwards each piece through ``_send_with_retry``.
"""

import html
import re
from typing import List, Optional, Tuple


def _utf16_len(s: str) -> int:
    """Length of ``s`` in UTF-16 code units — Telegram's real cap unit.

    Chars above U+FFFF (emoji) count as 2 units. ``len(s)`` under-counts
    them and blows past Telegram's 4096 cap → 400.
    """
    return len(s.encode("utf-16-le")) // 2


def chunk_text(text: str, limit: int = 4096) -> List[str]:
    if _utf16_len(text) <= limit:
        return [text]
    chunks: List[str] = []
    remaining = text
    while remaining:
        if _utf16_len(remaining) <= limit:
            chunks.append(remaining)
            break
        # Binary-search the largest code-point prefix whose UTF-16 len fits.
        lo, hi = 0, len(remaining)
        while lo < hi:
            mid = (lo + hi + 1) // 2
            if _utf16_len(remaining[:mid]) <= limit:
                lo = mid
            else:
                hi = mid - 1
        window_end = lo  # largest code-point count that fits
        window = remaining[:window_end]
        quarter = limit // 4  # threshold in UTF-16 units, matching the budget
        cut = window.rfind("\n\n")
        sep_len = 2
        if cut < 0 or _utf16_len(window[:cut]) <= quarter:
            cut = window.rfind("\n")
            sep_len = 1
        if cut < 0 or _utf16_len(window[:cut]) <= quarter:
            cut = window.rfind(" ")
            sep_len = 1
        if cut < 0 or _utf16_len(window[:cut]) <= quarter:
            cut = window_end
            sep_len = 0
        chunks.append(remaining[:cut])
        remaining = remaining[cut + sep_len:]
    return [c for c in chunks if c]


# Simple reopenable formatting tags. Attribute-bearing tags (``<a href=…>``,
# ``<code class=…>``) are treated as "complex" — degraded to plain text at
# any cut where one is still open, rather than reopened.
_REOPENABLE_SIMPLE_TAGS = ("pre", "code", "b", "i", "blockquote")

_TAG_RE = re.compile(r"<(/?)([a-zA-Z][a-zA-Z0-9]*)([^>]*)>")


def _index_inside_tag(s: str, idx: int) -> bool:
    """True iff ``idx`` falls strictly between ``<`` and its matching ``>``.

    A cut AT ``<`` or ``>`` is fine — it doesn't split a tag.
    """
    if idx <= 0 or idx >= len(s):
        return False
    # Most recent ``<`` vs most recent ``>`` before idx.
    last_lt = s.rfind("<", 0, idx)
    last_gt = s.rfind(">", 0, idx)
    return last_lt > last_gt


def _scan_open_tags_at(s: str, idx: int) -> Tuple[List[str], List[str]]:
    """Walk tags in ``s[0:idx]``; return ``(simple_stack, complex_stack)``.

    Both stacks list currently-open tags at ``idx`` in open order
    (outer→inner). Complex tags that closed before ``idx`` don't linger.
    """
    simple_stack: List[str] = []
    complex_stack: List[str] = []
    for m in _TAG_RE.finditer(s, 0, idx):
        is_close = m.group(1) == "/"
        name = m.group(2).lower()
        attrs = m.group(3).strip()
        if is_close:
            # Close tags carry no attrs — try complex stack first, then simple,
            # so ``<code class="…">…</code>`` balances against the complex open.
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
    """Simple reopenable tags open at ``idx``, or ``None`` if unsafe to split.

    Unsafe = a complex/attribute-bearing tag is open at ``idx``. A complex
    tag that closed BEFORE ``idx`` does not poison the cut.
    """
    simple_stack, complex_stack = _scan_open_tags_at(s, idx)
    if complex_stack:
        return None
    return simple_stack


def _strip_tags(s: str) -> str:
    """Strip HTML tags and decode entities — the plain-text degrade path.

    - Entity decode is required: Telegram does NOT decode entities in
      plain-text mode, so ``Tom &amp; Jerry`` would render as literal ``&amp;``.
      ``html.unescape`` is a no-op on entity-free payloads.
    - Single-pass by design (NOT idempotent): ``&amp;amp;`` decodes once to
      ``&amp;`` and must survive as such — matches browser rendering.
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
    # Complex tags open at a prior degrade cut, whose matching closes haven't
    # been consumed — MUST keep degrading until empty, else an orphan
    # ``</a>`` escapes into a later HTML chunk.
    pending_complex_closes: List[str] = []
    # Simple reopenable opens (``<b>``, ``<i>``) stripped from a prior degrade —
    # same rule: keep degrading until their matching closes are consumed.
    pending_simple_closes: List[str] = []

    while remaining:
        # Any in-flight tag from a prior degrade → stay in plain-text mode
        # until BOTH stacks empty.
        if pending_complex_closes or pending_simple_closes:
            # Walk to the position where BOTH stacks empty (or end-of-remaining).
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
                    # Close: complex-first, simple-second (mirror _scan_open_tags_at).
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
            # Cap the degraded piece by the size budget.
            cap = limit
            piece = remaining[:consume_end]
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
            # Rescan the actually-consumed span so both pending stacks stay
            # accurate when the size cap truncates before all opens/closes.
            new_pending_complex = list(pending_complex_closes)
            new_pending_simple = list(pending_simple_closes)
            for m in _TAG_RE.finditer(piece):
                is_close = m.group(1) == "/"
                name = m.group(2).lower()
                attrs = m.group(3).strip()
                if is_close:
                    # Close: complex-first, simple-second (same rule as above).
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

        # Reserve headroom for closing tags appended after the cut.
        # 64 UTF-16 units covers any realistic nested-tag close sequence
        # (``</blockquote>`` is 13 chars).
        closer_headroom = 64
        effective_limit = max(limit - closer_headroom, limit // 2)

        # Binary-search the largest code-point prefix that fits.
        lo, hi = 0, len(body)
        while lo < hi:
            mid = (lo + hi + 1) // 2
            if _utf16_len(body[:mid]) <= effective_limit:
                lo = mid
            else:
                hi = mid - 1
        window_end = lo
        if window_end <= len(reopen_prefix):
            # Pathological: reopen prefix already fills the budget → degrade.
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

        # Walk back for a cut: not inside a tag, ideally on ``\n``.
        # Try ``\n`` first, then any non-tag-interior position, else degrade.
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
        sep_len = 0  # keep the ``\n`` with the previous chunk
        if cut < 0:
            cut = _safe_cut(window_end)
        if cut < 0 or cut <= len(reopen_prefix):
            # No safe HTML cut — degrade at the size boundary.
            simple_at_end, complex_at_end = _scan_open_tags_at(body, window_end)
            if complex_at_end:
                pending_complex_closes = list(complex_at_end)
            if simple_at_end:
                pending_simple_closes = list(simple_at_end)
            chunks.append((_strip_tags(body[:window_end]), False))
            remaining = remaining[window_end - len(reopen_prefix):]
            reopen_prefix = ""
            continue

        # Inspect opens at the cut → close-here / reopen-next-chunk.
        simple_stack, complex_stack = _scan_open_tags_at(body, cut)
        if complex_stack:
            # Complex open at cut → degrade; remember both stacks so the
            # walker keeps degrading until orphan closes are consumed.
            pending_complex_closes = list(complex_stack)
            if simple_stack:
                pending_simple_closes = list(simple_stack)
            chunks.append((_strip_tags(body[:cut]), False))
            remaining = remaining[cut - len(reopen_prefix) + sep_len:]
            reopen_prefix = ""
            continue
        open_stack = simple_stack

        # Emit body[:cut] + closers in reverse order; next chunk reopens.
        closers = "".join("</%s>" % name for name in reversed(open_stack))
        chunk = body[:cut] + closers
        chunks.append((chunk, True))

        # body = reopen_prefix + remaining → consumed = cut - len(reopen_prefix).
        consumed_from_remaining = cut - len(reopen_prefix)
        remaining = remaining[consumed_from_remaining + sep_len:]

        reopen_prefix = "".join("<%s>" % name for name in open_stack)

    # Final cleanup: drop empty pieces.
    return [(c, h) for (c, h) in chunks if c]


def send_html(token: str, chat_id: str, html: str) -> None:
    """Send pre-formatted HTML to Telegram. No markdown conversion.

    Tag-aware chunking (see ``_chunk_html``): never cuts between ``<`` and
    ``>``, prefers ``\\n`` boundaries, close+reopen for simple tags, plain-text
    degrade for a complex tag open at the only viable cut.
    """
    # Lazy import — transport imports from this module (cycle break).
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
