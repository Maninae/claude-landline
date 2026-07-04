"""Rotating file + stdout logging for the daemon."""

import logging
import logging.handlers
import os
import sys
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional

from landline.config import LOG_FILE, LOG_MAX_BYTES, LOG_BACKUP_COUNT, TIMEZONE

# Test seam: tests set LANDLINE_DAEMON_LOG to a tmp_path child via an autouse
# fixture in conftest.py and call _reset_logger_for_tests() between tests so
# no test ever attaches the real RotatingFileHandler to LOG_FILE.
_LOG_PATH_ENV = "LANDLINE_DAEMON_LOG"

_LOGGER: Optional[logging.Logger] = None
_LOGGER_LOCK = threading.Lock()


def _get_logger() -> logging.Logger:
    global _LOGGER
    if _LOGGER is not None:
        return _LOGGER
    with _LOGGER_LOCK:
        if _LOGGER is not None:
            return _LOGGER
        logger = logging.getLogger("landline.daemon")
        logger.setLevel(logging.INFO)
        logger.propagate = False
        try:
            log_path = Path(os.environ.get(_LOG_PATH_ENV) or str(LOG_FILE))
            log_path.parent.mkdir(parents=True, exist_ok=True)
            handler = logging.handlers.RotatingFileHandler(
                str(log_path),
                maxBytes=LOG_MAX_BYTES,
                backupCount=LOG_BACKUP_COUNT,
                encoding="utf-8",
            )
            handler.setFormatter(logging.Formatter("%(message)s"))
            logger.addHandler(handler)
        except Exception as handler_init_error:
            try:
                sys.stderr.write(
                    "landline.logging: failed to create RotatingFileHandler "
                    "for {0}: {1}: {2}\n".format(
                        LOG_FILE,
                        type(handler_init_error).__name__,
                        handler_init_error,
                    )
                )
                sys.stderr.flush()
            except Exception:
                pass
        _LOGGER = logger
        return logger


def log(msg: str) -> None:
    ts = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    try:
        _get_logger().info(line)
    except Exception as logger_call_error:
        try:
            sys.stderr.write(
                "landline.logging: logger.info failed ({0}): {1}\n".format(
                    type(logger_call_error).__name__,
                    line,
                )
            )
            sys.stderr.flush()
        except Exception:
            pass


def _reset_logger_for_tests() -> None:
    """Test-only: drop the singleton so the next log() rebuilds the handler.

    Used by the autouse conftest fixture together with the LANDLINE_DAEMON_LOG
    env override. Safe to call when no logger has been built yet.
    """
    global _LOGGER
    with _LOGGER_LOCK:
        if _LOGGER is not None:
            for h in list(_LOGGER.handlers):
                try:
                    h.close()
                except Exception:
                    pass
                _LOGGER.removeHandler(h)
        _LOGGER = None
