"""PauseFlag — generation-aware pause flag used to interrupt in-flight Claude calls.

Each Claude call increments the generation; ``request_pause`` records which
generation should be interrupted. The watchdog checks ``is_requested(gen)``
to decide whether the *current* call should be interrupted — stale pauses
from a previous generation can no longer fire.

Lives in its own module so the orchestrator and the dispatcher can both
import it without round-trips through the orchestrator's heavy import graph.
"""

import threading
from typing import Optional


class PauseFlag:
    """Generation-aware pause flag — prevents stale requests from interrupting the wrong call."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._generation = 0
        self._requested_gen: Optional[int] = None
        # Level-triggered Event mirroring "a pause is currently requested".
        # Watchdog threads wait on this for instant wake on /pause; the
        # generation guard (is_requested(gen)) is still the authority on
        # *which* call to interrupt — a wake on a stale generation is a
        # harmless no-op (interrupt_check returns False; loop falls back
        # through to its next wait).
        self._event = threading.Event()

    def new_call(self) -> int:
        with self._lock:
            self._generation += 1
            return self._generation

    def request_pause(self) -> None:
        with self._lock:
            self._requested_gen = self._generation
            self._event.set()

    def is_requested(self, generation: int) -> bool:
        with self._lock:
            return self._requested_gen == generation

    def clear(self) -> None:
        with self._lock:
            self._requested_gen = None
            self._event.clear()

    def is_set(self) -> bool:
        with self._lock:
            return self._requested_gen is not None

    def set(self) -> None:
        """Compat shim for tests that call .set() like threading.Event."""
        self.request_pause()

    def wait(self, timeout: Optional[float]) -> bool:
        """Block up to ``timeout`` seconds for a pause to be requested.

        Returns True if a pause was requested before the timeout, False otherwise.
        Callers MUST still call ``is_requested(my_generation)`` after wake — a
        wake from a previous generation's residual state is intentionally not
        filtered here (cheaper to re-check at the wake site than to track
        generation-specific Events).
        """
        return self._event.wait(timeout)
