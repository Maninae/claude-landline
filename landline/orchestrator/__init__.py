"""Orchestrator facade — delegates attribute r/w to landline.orchestrator.daemon.

The daemon coordinator (``TelegramDaemon``) and every module-scope name it
imports at the top of ``daemon.py`` (save_state, log_conversation,
download_file, drain_inject_queue, BackgroundPoller, classify_updates,
keychain_get, load_state, sweep_media_caches, time, WORKSPACE,
_is_pause_command; plus PauseFlag, _reset_persistent_claude_for_new,
_sweep_telegram_image_cache, INJECT_QUEUE_DIR) are the actual names test
patches like ``patch("landline.orchestrator.save_state")`` need to target
so that the class-method bodies inside ``daemon.py`` see the mock at call
time.

This facade replaces the package's module class with one whose
``__getattr__`` / ``__setattr__`` / ``__delattr__`` delegate to
``landline.orchestrator.daemon``. Effect: attribute reads and writes on
``landline.orchestrator`` are transparent aliases for the same operations
on the daemon module — so patches propagate into daemon.py's globals
where its function bodies look them up. Zero test-edit cost.

Submodule imports (``landline.orchestrator.batch`` etc. introduced in
later waves) still resolve normally: Python's import machinery goes
through ``sys.modules`` / ``__path__`` and does not fall back to the
custom ``__getattr__``.
"""

import sys
from types import ModuleType

# Load daemon eagerly so the delegated attributes exist by the time this
# module returns to the importer.
from landline.orchestrator import daemon as _daemon_module


class _OrchestratorFacade(ModuleType):
    """Module subclass — reads/writes/deletes forward to the daemon module."""

    def __getattr__(self, name):  # only called for names not in __dict__
        try:
            return getattr(_daemon_module, name)
        except AttributeError as exc:
            raise AttributeError(
                f"module {self.__name__!r} has no attribute {name!r}"
            ) from exc

    def __setattr__(self, name, value):
        setattr(_daemon_module, name, value)

    def __delattr__(self, name):
        delattr(_daemon_module, name)


sys.modules[__name__].__class__ = _OrchestratorFacade
