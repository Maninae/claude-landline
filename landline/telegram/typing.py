"""Per-thread keep-alive HTTPS pool for the typing indicator ONLY.

- Pool is safe for ``sendChatAction`` because a duplicate typing indicator
  is invisible. It is UNSAFE for ``sendMessage`` — Telegram has no
  idempotency keys, so a stale-keep-alive lost-response would double-send.
- One connection per thread via ``threading.local`` (poller, each per-chat
  StreamSender worker, streaming's ``typing_loop``). Never shared.
"""

import http.client
import json
import threading

from landline.telegram.transport import telegram_api


_TELEGRAM_API_HOST = "api.telegram.org"
_TYPING_POOL_TIMEOUT = 10

_typing_conn_local = threading.local()


def _get_typing_conn() -> http.client.HTTPSConnection:
    conn = getattr(_typing_conn_local, "conn", None)
    if conn is None:
        conn = http.client.HTTPSConnection(
            _TELEGRAM_API_HOST, timeout=_TYPING_POOL_TIMEOUT,
        )
        _typing_conn_local.conn = conn
    return conn


def _reset_typing_conn() -> None:
    """Close and drop the current thread's pooled typing connection.

    Called after any exception on the pooled path so the next call starts fresh.
    """
    conn = getattr(_typing_conn_local, "conn", None)
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass
    _typing_conn_local.conn = None


def send_typing(token: str, chat_id: str) -> None:
    """Send a ``sendChatAction: typing`` request. Best-effort; silent on failure.

    - Fast path: per-thread pooled HTTPS connection (no TLS handshake every 4s).
    - On any pool exception or 5xx: drop pooled connection, fall back to the
      one-shot ``telegram_api`` path.
    """
    body = json.dumps({"chat_id": chat_id, "action": "typing"}).encode()
    headers = {"Content-Type": "application/json"}
    try:
        conn = _get_typing_conn()
        conn.request(
            "POST", f"/bot{token}/sendChatAction",
            body=body, headers=headers,
        )
        resp = conn.getresponse()
        # HTTP/1.1 keep-alive: unread body bytes poison the next request.
        resp.read()
        if resp.status >= 500:
            raise RuntimeError("typing 5xx %d" % resp.status)
        return
    except Exception:
        _reset_typing_conn()

    # Fallback: one-shot. Duplicate typing indicators are invisible, so a
    # retry on a fresh connection is safe.
    try:
        telegram_api(token, "sendChatAction", {
            "chat_id": chat_id,
            "action": "typing",
        }, timeout=10)
    except Exception:
        pass
