"""Tests for the ``landline.json`` config loader.

Two layers of testing keep the loader honest without polluting module state:

- ``TestLoadOverrides*`` calls ``landline.config._load_overrides(workspace)``
  directly as a pure function against a tmp workspace. These tests cover the
  JSON parse, allowlist, and type-validation logic — no module reload, no
  identity-check breakage for downstream modules.

- ``TestConstantsDerivedFromOverrides`` spawns a subprocess with
  ``LANDLINE_WORKSPACE=<tmp>`` and asserts on the constants ``landline.config``
  publishes at import time. Subprocesses are isolated, so cross-test module
  identity ("failure_tracker.X is config.X") is preserved for the rest of the
  suite.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Pure-function tests for _load_overrides
# ---------------------------------------------------------------------------


class TestLoadOverridesAbsentFile:
    def test_missing_landline_json_returns_empty_dict(self, tmp_path):
        """No landline.json in the workspace → loader returns ``{}``."""
        from landline.config import _load_overrides
        assert _load_overrides(tmp_path) == {}


class TestLoadOverridesApplies:
    def test_string_keys(self, tmp_path):
        (tmp_path / "landline.json").write_text(json.dumps({
            "keychain_account": "custom-account",
            "user_name": "Alice",
            "agent_name": "Nova",
            "launchd_label_prefix": "com.example.custom",
            "morning_brief_glob": "briefs/*.md",
            "whisper_model": "large-v3",
            "whisper_language": "fr",
            "rejection_mode": "reply",
            "claude_permission_mode": "acceptEdits",
        }))
        from landline.config import _load_overrides
        result = _load_overrides(tmp_path)
        assert result["keychain_account"] == "custom-account"
        assert result["user_name"] == "Alice"
        assert result["agent_name"] == "Nova"
        assert result["launchd_label_prefix"] == "com.example.custom"
        assert result["morning_brief_glob"] == "briefs/*.md"
        assert result["whisper_model"] == "large-v3"
        assert result["whisper_language"] == "fr"
        assert result["rejection_mode"] == "reply"
        assert result["claude_permission_mode"] == "acceptEdits"

    def test_bool_key(self, tmp_path):
        (tmp_path / "landline.json").write_text(json.dumps({
            "reaction_acks_enabled": False,
        }))
        from landline.config import _load_overrides
        assert _load_overrides(tmp_path)["reaction_acks_enabled"] is False

    def test_null_nullable_key(self, tmp_path):
        (tmp_path / "landline.json").write_text(json.dumps({
            "claude_model": None,
            "timezone": None,
            "morning_brief_glob": None,
        }))
        from landline.config import _load_overrides
        result = _load_overrides(tmp_path)
        assert result["claude_model"] is None
        assert result["timezone"] is None
        assert result["morning_brief_glob"] is None


class TestLoadOverridesPathExpansion:
    def test_expanduser_on_path_keys(self, tmp_path):
        (tmp_path / "landline.json").write_text(json.dumps({
            "claude_binary": "~/tools/claude",
            "whisper_bin": "~/tools/whisper",
            "whisper_model_dir": "~/cache/whisper",
        }))
        from landline.config import _load_overrides
        result = _load_overrides(tmp_path)
        home = os.path.expanduser("~")
        assert result["claude_binary"] == home + "/tools/claude"
        assert result["whisper_bin"] == home + "/tools/whisper"
        assert result["whisper_model_dir"] == home + "/cache/whisper"


class TestLoadOverridesFailFast:
    def test_unknown_key_raises_system_exit(self, tmp_path):
        """Fixed-allowlist policy — any unknown key fails fast. launchd's
        ThrottleInterval bounds the crash loop that a persistent typo
        would create."""
        (tmp_path / "landline.json").write_text(json.dumps({
            "keychain_account": "ok",
            "totally_unknown_key": "boom",
        }))
        from landline.config import _load_overrides
        with pytest.raises(SystemExit):
            _load_overrides(tmp_path)

    def test_malformed_json_raises_system_exit(self, tmp_path):
        (tmp_path / "landline.json").write_text("{not valid json,")
        from landline.config import _load_overrides
        with pytest.raises(SystemExit):
            _load_overrides(tmp_path)

    def test_non_object_top_level_raises_system_exit(self, tmp_path):
        (tmp_path / "landline.json").write_text(json.dumps(["oops"]))
        from landline.config import _load_overrides
        with pytest.raises(SystemExit):
            _load_overrides(tmp_path)

    def test_type_mismatch_string_key_raises_system_exit(self, tmp_path):
        (tmp_path / "landline.json").write_text(json.dumps({
            "user_name": 42,  # not a string
        }))
        from landline.config import _load_overrides
        with pytest.raises(SystemExit):
            _load_overrides(tmp_path)

    def test_type_mismatch_bool_key_raises_system_exit(self, tmp_path):
        (tmp_path / "landline.json").write_text(json.dumps({
            "reaction_acks_enabled": "true",  # string, not bool
        }))
        from landline.config import _load_overrides
        with pytest.raises(SystemExit):
            _load_overrides(tmp_path)

    def test_type_mismatch_nullable_key_raises_system_exit(self, tmp_path):
        (tmp_path / "landline.json").write_text(json.dumps({
            "claude_model": 3,
        }))
        from landline.config import _load_overrides
        with pytest.raises(SystemExit):
            _load_overrides(tmp_path)

    def test_null_whisper_model_dir_raises_system_exit(self, tmp_path):
        """``whisper_model_dir`` is a REQUIRED path (validator ``_v_path_str``,
        not ``_v_path_or_none``): a null must fail fast at startup rather
        than surface later as an opaque per-voice-note TypeError when the
        voice pipeline tries to feed the None into whisper's --model-dir."""
        (tmp_path / "landline.json").write_text(json.dumps({
            "whisper_model_dir": None,
        }))
        from landline.config import _load_overrides
        with pytest.raises(SystemExit):
            _load_overrides(tmp_path)


# ---------------------------------------------------------------------------
# Subprocess tests: constants derive from _cfg + overrides applied at import
# ---------------------------------------------------------------------------


def _repo_root() -> Path:
    """Path to the ``claude-landline`` repo root (landline/tests/…/tests
    → landline → repo)."""
    return Path(__file__).resolve().parent.parent.parent


def _run_config_probe(workspace: Path, probe: str) -> str:
    """Run ``probe`` in a fresh interpreter with LANDLINE_WORKSPACE set.

    The subprocess inherits PYTHONPATH so ``import landline.config`` works.
    Any SystemExit / assertion in the probe surfaces as a non-zero exit.
    """
    env = dict(os.environ)
    env["LANDLINE_WORKSPACE"] = str(workspace)
    env["PYTHONPATH"] = (
        str(_repo_root()) + os.pathsep + env.get("PYTHONPATH", "")
    )
    return subprocess.check_output(
        [sys.executable, "-c", probe],
        env=env,
        text=True,
        timeout=15,
    ).strip()


class TestConstantsDerivedFromOverrides:
    def test_absent_landline_json_yields_public_defaults(self, tmp_path):
        """The full happy path — no landline.json → every deployer-facing
        constant takes its documented default."""
        probe = (
            "from landline import config; "
            "import json, sys; "
            "sys.stdout.write(json.dumps({"
            "'ka': config.KEYCHAIN_ACCOUNT, "
            "'cl': config.CLAUDE, "
            "'cm': config.CLAUDE_MODEL, "
            "'pm': config.CLAUDE_PERMISSION_MODE, "
            "'un': config.USER_NAME, "
            "'an': config.AGENT_NAME, "
            "'lp': config.LAUNCHD_LABEL_PREFIX, "
            "'mb': config.MORNING_BRIEF_GLOB, "
            "'wb': config.WHISPER_BIN, "
            "'wm': config.WHISPER_MODEL, "
            "'wl': config.WHISPER_LANGUAGE, "
            "'re': config.REACTION_ACKS_ENABLED, "
            "'rm': config.REJECTION_MODE, "
            "}))"
        )
        out = _run_config_probe(tmp_path, probe)
        data = json.loads(out)
        assert data == {
            "ka": "landline",
            "cl": "claude",
            "cm": None,
            "pm": "bypassPermissions",
            "un": "User",
            "an": "Assistant",
            "lp": "com.landline",
            "mb": None,
            "wb": "whisper",
            "wm": "base",
            "wl": "en",
            "re": True,
            "rm": "silent",
        }

    def test_landline_json_overrides_applied(self, tmp_path):
        (tmp_path / "landline.json").write_text(json.dumps({
            "keychain_account": "custom-account",
            "user_name": "Alice",
            "agent_name": "Nova",
            "launchd_label_prefix": "com.example.custom",
            "morning_brief_glob": "briefs/*.md",
            "whisper_model": "large-v3",
            "reaction_acks_enabled": False,
        }))
        probe = (
            "from landline import config; "
            "import json, sys; "
            "sys.stdout.write(json.dumps({"
            "'ka': config.KEYCHAIN_ACCOUNT, "
            "'un': config.USER_NAME, "
            "'an': config.AGENT_NAME, "
            "'lp': config.LAUNCHD_LABEL_PREFIX, "
            "'mb': config.MORNING_BRIEF_GLOB, "
            "'wm': config.WHISPER_MODEL, "
            "'re': config.REACTION_ACKS_ENABLED, "
            "}))"
        )
        out = _run_config_probe(tmp_path, probe)
        assert json.loads(out) == {
            "ka": "custom-account",
            "un": "Alice",
            "an": "Nova",
            "lp": "com.example.custom",
            "mb": "briefs/*.md",
            "wm": "large-v3",
            "re": False,
        }

    def test_named_timezone_becomes_zoneinfo(self, tmp_path):
        (tmp_path / "landline.json").write_text(json.dumps({
            "timezone": "America/New_York",
        }))
        probe = (
            "from landline import config; "
            "import sys; "
            "sys.stdout.write(str(config.TIMEZONE))"
        )
        assert _run_config_probe(tmp_path, probe) == "America/New_York"

    def test_null_timezone_falls_back_to_system(self, tmp_path):
        """``timezone: null`` selects the system zone (or UTC fallback);
        the exact zone is host-dependent, so assert the type only."""
        (tmp_path / "landline.json").write_text(json.dumps({"timezone": None}))
        probe = (
            "from landline import config; "
            "from zoneinfo import ZoneInfo; "
            "import sys; "
            "sys.stdout.write(str(isinstance(config.TIMEZONE, ZoneInfo)))"
        )
        assert _run_config_probe(tmp_path, probe) == "True"

    def test_unknown_timezone_exits_at_import(self, tmp_path):
        (tmp_path / "landline.json").write_text(json.dumps({
            "timezone": "Not/A/Real_Zone_Name",
        }))
        with pytest.raises(subprocess.CalledProcessError):
            _run_config_probe(
                tmp_path,
                "from landline import config",  # importing raises SystemExit
            )
