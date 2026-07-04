"""Telegram formatting utilities — shared by daemon and delivery scripts.

Two layers:
  1. md_to_telegram_html(text) — full markdown-to-Telegram-HTML converter
  2. Programmatic helpers — bold(), italic(), code(), pre(), etc.
     All helpers auto-escape HTML entities in their input.

Python 3.9 compatible (no X | Y unions, no match statements).
"""

import re
from typing import List, Optional


def escape_html(text: str) -> str:
    """Escape &, <, > for Telegram HTML."""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def bold(text: str) -> str:
    return "<b>%s</b>" % escape_html(text)


def italic(text: str) -> str:
    return "<i>%s</i>" % escape_html(text)


def code(text: str) -> str:
    return "<code>%s</code>" % escape_html(text)


def pre(text: str, lang: str = "") -> str:
    if lang:
        return '<pre><code class="language-%s">%s</code></pre>' % (
            escape_html(lang), escape_html(text))
    return "<pre>%s</pre>" % escape_html(text)


def strikethrough(text: str) -> str:
    return "<s>%s</s>" % escape_html(text)


def link(label: str, url: str) -> str:
    safe_url = url.replace('"', "&quot;")
    return '<a href="%s">%s</a>' % (safe_url, escape_html(label))


def md_to_telegram_html(text: str) -> str:
    """Convert a subset of Markdown to Telegram-flavored HTML.

    Handles: fenced code blocks, inline code, headers, links, bold,
    bold-italic, italic (* and _), strikethrough, and backslash escapes.
    """
    # -- Phase 1: Stash code blocks so they're not processed as markdown --

    fences = []  # type: List[str]

    def _stash_fence(m: "re.Match[str]") -> str:
        fences.append(m.group(1))
        return "\x00FENCE%d\x00" % (len(fences) - 1)

    text = re.sub(r"```[a-zA-Z0-9_-]*\n?([\s\S]*?)```", _stash_fence, text)

    inlines = []  # type: List[str]

    def _stash_inline(m: "re.Match[str]") -> str:
        inlines.append(m.group(1))
        return "\x00INLINE%d\x00" % (len(inlines) - 1)

    text = re.sub(r"`([^`\n]+)`", _stash_inline, text)

    # -- Phase 2: Escape HTML-sensitive chars --
    text = escape_html(text)

    # -- Phase 3: Convert markdown to HTML tags --

    # Headers
    text = re.sub(r"(?m)^(#{1,6})\s+(.+?)\s*$",
                  lambda m: "<b>%s</b>" % m.group(2).strip(), text)

    # Links [label](url) — reject non-http URLs
    def _link(m: "re.Match[str]") -> str:
        label, url = m.group(1), m.group(2)
        if not re.match(r"^https?://", url):
            return m.group(0)
        safe_url = url.replace('"', "&quot;")
        return '<a href="%s">%s</a>' % (safe_url, label)

    text = re.sub(r"\[([^\]]+)\]\(([^)\s]+)\)", _link, text)

    # Resolve markdown backslash escapes (e.g. \[ \] from LLM output)
    text = re.sub(r"\\([\[\](){}*_~`#>|!])", r"\1", text)

    # Bold-italic ***x***
    text = re.sub(r"\*\*\*([^\n*]+?)\*\*\*", r"<b><i>\1</i></b>", text)
    # Bold **x**
    text = re.sub(r"\*\*([^\n*]+?)\*\*", r"<b>\1</b>", text)
    # Strikethrough ~~x~~
    text = re.sub(r"~~([^\n~]+?)~~", r"<s>\1</s>", text)
    # Italic *x* (asterisk)
    text = re.sub(r"(?<![\*\w])\*([^\n*]+?)\*(?![\*\w])", r"<i>\1</i>", text)
    # Italic _x_ (underscore)
    text = re.sub(
        r"(?<![A-Za-z0-9_])_([^_\n]+?)_(?![A-Za-z0-9_])", r"<i>\1</i>", text)

    # -- Phase 4: Restore stashed code blocks --

    def _restore_inline(m: "re.Match[str]") -> str:
        body = inlines[int(m.group(1))]
        return "<code>%s</code>" % escape_html(body)

    text = re.sub(r"\x00INLINE(\d+)\x00", _restore_inline, text)

    def _restore_fence(m: "re.Match[str]") -> str:
        body = fences[int(m.group(1))]
        return "<pre>%s</pre>" % escape_html(body)

    text = re.sub(r"\x00FENCE(\d+)\x00", _restore_fence, text)

    return text
