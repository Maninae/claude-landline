"""Tool-status formatters for Claude's stream-json output.

Each Claude tool_use block is mapped to a compact, HTML-safe status line
that the StreamSender batches and sends through Telegram's `send_html`
transport. Lines are pre-built HTML — they bypass the markdown converter.
"""

from pathlib import Path
from typing import Any, Dict, Optional

from landline.config import WORKSPACE
from landline.telegram.fmt import pre


def _extract_text_blocks(content: Any) -> str:
    if not isinstance(content, list):
        return ""
    parts = []
    for block in content:
        if isinstance(block, dict) and block.get("type") == "text":
            parts.append(block.get("text", "") or "")
    return "".join(parts)


_WORKSPACE_STR = str(WORKSPACE)
_HOME_STR = str(Path.home())
_WORKSPACE_BIN_PREFIX = str(WORKSPACE / "bin") + "/"
_MAX_CMD_LEN = 80


def _shorten_path(p: str) -> str:
    """Turn absolute paths into readable relative ones."""
    if p.startswith(_WORKSPACE_STR + "/"):
        return p[len(_WORKSPACE_STR) + 1:]
    if p.startswith(_HOME_STR + "/"):
        return "~/" + p[len(_HOME_STR) + 1:]
    return p


def _format_tool_status(block: Dict[str, Any]) -> Optional[str]:
    """Format a tool_use content block into a compact status line, or None to suppress."""
    name = block.get("name", "")
    inp = block.get("input")
    if not isinstance(inp, dict):
        inp = {}

    if name == "Bash":
        cmd = inp.get("command", "") or ""
        if not cmd:
            return None
        # Strip the workspace `bin/` prefix so the status shows just the tool
        # name + args, not the full absolute path.
        if cmd.startswith(_WORKSPACE_BIN_PREFIX):
            trimmed = cmd[len(_WORKSPACE_BIN_PREFIX):]
            display = trimmed if len(trimmed) <= _MAX_CMD_LEN else trimmed[:_MAX_CMD_LEN] + "…"
            return pre(display, "Shell")
        display = cmd if len(cmd) <= _MAX_CMD_LEN else cmd[:_MAX_CMD_LEN] + "…"
        return pre(display, "Shell")

    if name == "Read":
        fp = inp.get("file_path", "") or ""
        if not fp:
            return None
        short = _shorten_path(fp)
        if "/skills/" in fp:
            skill_name = fp.split("/skills/")[-1].split("/")[0]
            return pre(skill_name, "📖 Skill")
        if "/memory/" in fp:
            return pre(short, "📂 Read")
        if fp.endswith((".jpg", ".jpeg", ".png", ".gif", ".webp")):
            return "🖼 Reading image"
        return None

    if name == "Agent":
        desc = inp.get("description", "") or ""
        return "🔀 Subagent: \"%s\"" % desc if desc else "🔀 Subagent launched"

    if name == "Skill":
        skill = inp.get("skill", "") or ""
        return pre("/%s" % skill, "⚡ Skill") if skill else None

    if name == "Edit":
        fp = inp.get("file_path", "") or ""
        if fp:
            return pre(_shorten_path(fp), "✏️ Edit")
        return None

    if name == "Write":
        fp = inp.get("file_path", "") or ""
        if fp:
            return pre(_shorten_path(fp), "📝 Write")
        return None

    if name == "WebSearch":
        q = inp.get("query", "") or ""
        return pre(q[:_MAX_CMD_LEN], "🌐 Search") if q else None

    if name == "WebFetch":
        url = inp.get("url", "") or ""
        if url:
            try:
                domain = url.split("//", 1)[-1].split("/")[0]
            except Exception:
                domain = url[:40]
            return "🌐 %s" % domain
        return None

    return None


def _format_repeated_status(line: str, count: int) -> str:
    """Append an italic '(N times)' suffix when a status line was collapsed."""
    if count > 1:
        return "%s\n<i>(%d times)</i>" % (line, count)
    return line
