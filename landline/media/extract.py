"""External archive extractor — safe-unzip subprocess wrapper.

Pure functional wrapper: no daemon state; only side effects are the
subprocess call and the files the external tool drops under ``dest_dir``.

Contract with the extractor binary (identical for any deployer):

- CLI: ``<extractor> <archive> --json [--dest DIR]``.
- stdout on exit 0: a JSON manifest with ``dest`` (str) and ``extracted``
  (list of ``{"name": <str, relpath under dest>, "size": <int>, "type":
  <str>}``) and ``skipped`` (list of ``{"name": ..., "reason": ...}``).
  ``extracted`` MAY be empty when everything got skipped.
- Exit 0 = extracted successfully (possibly nothing).
- Exit 77 = archive rejected wholesale; stdout MAY carry a manifest with a
  ``reject_reason`` slug (e.g. ``path_traversal``, ``declared_size_bomb``,
  ``symlink_entry``). Missing / unparseable manifest → ``reject_reason=None``.
- Exit 78 = extractor error (corrupt zip, timeout, disk).

Anything else — a wall-clock timeout, a missing binary, a JSON parse
failure on exit 0, an unexpected exit code — collapses into an
``ExtractionResult(ok=False, error=<slug>)``. This wrapper MUST NEVER
raise; the caller (``document.dispatch_document``) turns each error
class into a user-facing notice and advances the cursor.

Privacy (load-bearing): MUST NOT log extracted filenames, the archive's
internal paths, or the archive filename. Only metadata (elapsed, exit
code, extracted / skipped counts, error class). Mirrors ``transcribe.py``'s
"never log transcript text" invariant — the archive's contents are the
operator's private content and log lines end up in daily memory.
"""

import json
import os
import subprocess
import time
from dataclasses import dataclass
from typing import List, Optional

from landline.runtime.logging import log


@dataclass(frozen=True)
class ExtractionResult:
    """Result of one safe-unzip subprocess call.

    Attributes:
        ok: True iff the extractor returned exit 0 AND its JSON manifest
            parsed cleanly. ``ok=True`` may still carry an empty
            ``extracted_paths`` (everything got skipped) — the caller
            decides whether that's a user-visible empty-archive notice.
        extracted_paths: Absolute filesystem paths of every extracted
            entry, computed from ``os.path.join(manifest["dest"], entry["name"])``.
            Empty on failure OR on an "extracted nothing" success.
        skipped_count: Count of members the extractor allowlist-refused
            (extension-not-allowed, type spoof, etc). Informational only.
        error: Short slug on failure; ``None`` on success. Slugs:
            ``rejected`` (exit 77), ``tool_error`` (exit 78), ``timeout``
            (wall-clock), ``extractor_missing`` (FileNotFoundError on
            spawn), ``bad_manifest`` (exit 0 but unparseable JSON),
            ``unknown`` (unexpected Exception).
        reject_reason: The manifest's ``reject_reason`` slug (only
            populated when ``error == "rejected"`` and the manifest was
            parseable). ``None`` otherwise — including on rejections
            whose manifest we couldn't parse.
    """

    ok: bool
    extracted_paths: List[str]
    skipped_count: int
    error: Optional[str]
    reject_reason: Optional[str]


def _has_control_char(s: str) -> bool:
    """True iff ``s`` contains any ASCII C0 control (``\\x00``-``\\x1f``) or DEL.

    Control chars in a path are adversarial (safe-unzip layer (a) rejects
    them wholesale; the wrapper drops individual entries defense-in-depth).
    Newlines specifically break out of the ``<archive_contents>`` frame
    Landline builds around each entry.
    """
    for ch in s:
        code = ord(ch)
        if code <= 0x1f or code == 0x7f:
            return True
    return False


def _entry_is_contained(joined: str, dest_real: str) -> bool:
    """True iff ``realpath(joined)`` is under ``dest_real`` (or equals it).

    Both args are trusted absolute paths (caller resolves ``dest_real``
    via ``realpath`` once). Uses ``os.path.commonpath`` for the check;
    a ``ValueError`` (mixed drives / empty) collapses to False. Symlinks
    inside ``dest`` that point OUT are caught because ``realpath``
    follows them before the comparison.
    """
    try:
        candidate_real = os.path.realpath(joined)
        common = os.path.commonpath([candidate_real, dest_real])
    except (ValueError, OSError):
        return False
    return common == dest_real


def _paths_from_manifest(manifest: dict, dest_dir) -> Optional[List[str]]:
    """Build the absolute-path list, enforcing containment.

    Layered defense: safe-unzip already sanitized names and paths, but
    this wrapper NEVER trusts a subprocess's output.

    Drops (silently, metadata-only log) entries whose:
      - shape is off (missing name, non-string, empty),
      - name or joined path contains any ASCII control char (``\\n``,
        ``\\r``, ``\\t``, etc — see ``_has_control_char``),
      - resolved absolute path escapes ``dest_dir`` (traversal / absolute
        entry name a lying extractor might return).

    Returns:
        List of surviving absolute paths on success, or ``None`` if the
        manifest's declared ``dest`` itself lies outside ``dest_dir`` —
        the caller MUST treat ``None`` as ``bad_manifest``. A lying dest
        means we can't trust any per-entry check the manifest suggested.
    """
    dest = manifest.get("dest") or ""
    extracted = manifest.get("extracted") or []

    # Anchor every containment check on the CALLER'S dest, not the
    # manifest's — a compromised extractor could rewrite `dest` to
    # `/etc/`. Realpath BOTH sides so a symlinked cache dir resolves
    # correctly.
    try:
        dest_dir_real = os.path.realpath(str(dest_dir))
    except (ValueError, OSError):
        log("archive_extract: dest_dir resolve failed")
        return None

    # If the manifest declared a different dest, it MUST still be
    # under the caller's dest. Otherwise the wrapper cannot honor the
    # extractor's path shape safely — treat as bad_manifest.
    manifest_dest_str = str(dest)
    if not manifest_dest_str:
        return []
    try:
        manifest_dest_real = os.path.realpath(manifest_dest_str)
    except (ValueError, OSError):
        log("archive_extract: manifest dest resolve failed")
        return None
    if manifest_dest_real != dest_dir_real:
        try:
            common = os.path.commonpath([manifest_dest_real, dest_dir_real])
        except (ValueError, OSError):
            log("archive_extract: manifest dest not under caller dest_dir")
            return None
        if common != dest_dir_real:
            log("archive_extract: manifest dest escapes caller dest_dir")
            return None

    dropped_ctrl = 0
    dropped_escape = 0
    paths: List[str] = []
    for entry in extracted:
        if not isinstance(entry, dict):
            continue
        name = entry.get("name")
        if not isinstance(name, str) or not name:
            continue
        if _has_control_char(name):
            dropped_ctrl += 1
            continue
        joined = os.path.join(manifest_dest_str, name)
        if _has_control_char(joined):
            dropped_ctrl += 1
            continue
        if not _entry_is_contained(joined, dest_dir_real):
            dropped_escape += 1
            continue
        paths.append(joined)

    if dropped_ctrl or dropped_escape:
        log(
            "archive_extract: dropped entries (control=%d, escape=%d)"
            % (dropped_ctrl, dropped_escape)
        )
    return paths


def _skipped_count(manifest: dict) -> int:
    """Length of the manifest's ``skipped`` list, defensively coerced.

    An extractor manifest without ``skipped`` (older tools, or manifest
    shape drift) reads as 0. Non-list values also read as 0.
    """
    skipped = manifest.get("skipped")
    if not isinstance(skipped, list):
        return 0
    return len(skipped)


def extract_archive(
    archive_path,
    extractor_bin: str,
    dest_dir,
    timeout_seconds: int,
) -> ExtractionResult:
    """Run the extractor on ``archive_path``, returning ExtractionResult. NEVER raises.

    Args:
        archive_path: path to the ``.zip`` (str or Path).
        extractor_bin: absolute path to the extractor executable (the
            deployer-configured ``ARCHIVE_EXTRACTOR``). Passed through
            unchanged; PATH lookup is the caller's problem.
        dest_dir: extraction destination (str or Path). MUST already
            exist at ``0o700`` — the wrapper doesn't create or chmod it.
            The extractor writes ``manifest["dest"]`` back to us; we use
            that value (not this arg) to build absolute paths, so a
            deployer whose extractor rewrites ``--dest`` still works.
        timeout_seconds: wall-clock cap on the subprocess.

    Returns:
        See ``ExtractionResult`` for the ok/failure shape.

    Metadata-only logging (elapsed, exit code, counts, error class).
    NEVER logs archive filenames or per-entry paths.
    """
    started_at = time.time()
    cmd = [
        extractor_bin,
        str(archive_path),
        "--json",
        "--dest", str(dest_dir),
    ]
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired:
        elapsed = time.time() - started_at
        log(
            "archive_extract: timeout after %.1fs (cap %ds)"
            % (elapsed, timeout_seconds)
        )
        return ExtractionResult(
            ok=False,
            extracted_paths=[],
            skipped_count=0,
            error="timeout",
            reject_reason=None,
        )
    except FileNotFoundError:
        elapsed = time.time() - started_at
        # Metadata-only; the extractor_bin path could arguably leak but
        # it's config value, not operator content — logging the class
        # only keeps the wrapper's discipline uniform.
        log(
            "archive_extract: extractor binary not found (after %.1fs)"
            % elapsed
        )
        return ExtractionResult(
            ok=False,
            extracted_paths=[],
            skipped_count=0,
            error="extractor_missing",
            reject_reason=None,
        )
    except Exception as e:
        elapsed = time.time() - started_at
        # Metadata-only: exception class, not message (which could embed
        # a path or filename).
        log(
            "archive_extract: unexpected exception %s after %.1fs"
            % (type(e).__name__, elapsed)
        )
        return ExtractionResult(
            ok=False,
            extracted_paths=[],
            skipped_count=0,
            error="unknown",
            reject_reason=None,
        )

    elapsed = time.time() - started_at
    exit_code = proc.returncode
    stdout = proc.stdout or ""

    if exit_code == 0:
        try:
            manifest = json.loads(stdout)
        except (ValueError, TypeError) as parse_err:
            log(
                "archive_extract: exit 0 but unparseable manifest "
                "(%s) after %.1fs" % (type(parse_err).__name__, elapsed)
            )
            return ExtractionResult(
                ok=False,
                extracted_paths=[],
                skipped_count=0,
                error="bad_manifest",
                reject_reason=None,
            )
        if not isinstance(manifest, dict):
            log(
                "archive_extract: exit 0 manifest not a JSON object "
                "after %.1fs" % elapsed
            )
            return ExtractionResult(
                ok=False,
                extracted_paths=[],
                skipped_count=0,
                error="bad_manifest",
                reject_reason=None,
            )
        paths = _paths_from_manifest(manifest, dest_dir)
        if paths is None:
            # ``None`` signals the manifest lied about its dest — treat
            # the whole thing as bad_manifest so the caller emits a
            # generic couldn't-open notice rather than a partial dispatch.
            log(
                "archive_extract: containment check failed after %.1fs"
                % elapsed
            )
            return ExtractionResult(
                ok=False,
                extracted_paths=[],
                skipped_count=0,
                error="bad_manifest",
                reject_reason=None,
            )
        skipped = _skipped_count(manifest)
        # PRIVACY: counts + elapsed only — no names, no paths.
        log(
            "archive_extract: OK: %d extracted, %d skipped in %.1fs"
            % (len(paths), skipped, elapsed)
        )
        return ExtractionResult(
            ok=True,
            extracted_paths=paths,
            skipped_count=skipped,
            error=None,
            reject_reason=None,
        )

    if exit_code == 77:
        # Rejected wholesale. Try to salvage a reason slug from the
        # manifest, but never fail on parse trouble — the failure branch
        # is what matters, not the diagnostic detail.
        reason: Optional[str] = None
        try:
            manifest = json.loads(stdout)
            if isinstance(manifest, dict):
                raw_reason = manifest.get("reject_reason")
                if isinstance(raw_reason, str) and raw_reason:
                    reason = raw_reason
        except (ValueError, TypeError):
            pass
        log(
            "archive_extract: rejected (reason=%s) after %.1fs"
            % (reason or "unknown", elapsed)
        )
        return ExtractionResult(
            ok=False,
            extracted_paths=[],
            skipped_count=0,
            error="rejected",
            reject_reason=reason,
        )

    if exit_code == 78:
        log(
            "archive_extract: tool_error (exit 78) after %.1fs" % elapsed
        )
        return ExtractionResult(
            ok=False,
            extracted_paths=[],
            skipped_count=0,
            error="tool_error",
            reject_reason=None,
        )

    # Any other exit code is unexpected — treat as tool_error so the
    # user gets a "couldn't open" notice rather than nothing.
    log(
        "archive_extract: unexpected exit %d after %.1fs"
        % (exit_code, elapsed)
    )
    return ExtractionResult(
        ok=False,
        extracted_paths=[],
        skipped_count=0,
        error="tool_error",
        reject_reason=None,
    )
