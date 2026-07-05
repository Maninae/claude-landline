import http.client
import json
import threading

from landline.telegram.transport import telegram_api


# -----------------------------------------------------------------------------
# Scoped HTTPS keep-alive for the typing indicator ONLY.
#
# Rationale: Telegram has no idempotency keys. On a stale-keep-alive
# connection, we can't distinguish "request never reached the server" from
# "request was processed but response was lost". Retrying a lost response on
# a fresh connection would then double-send the message. That risk is
# unacceptable for real sends but harmless for ``sendChatAction`` (typing) —
# a duplicate typing indicator is invisible.
#
# The connection is per-thread via ``threading.local`` — the poller thread,
# each per-chat StreamSender worker, and the streaming module's
# ``typing_loop`` each maintain their own connection. Never shared.
# -----------------------------------------------------------------------------

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

    Called after any exception on the pooled path so the next call starts
    on a fresh connection.
    """
    conn = getattr(_typing_conn_local, "conn", None)
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass
    _typing_conn_local.conn = None


def send_typing(token: str, chat_id: str) -> None:
    """Send a ``sendChatAction: typing`` request.

    Fast path: per-thread pooled HTTPS connection (no fresh TLS handshake
    every 4 seconds). If the pool call raises OR the server returns a 5xx,
    we drop the pooled connection and fall back to the one-shot
    ``telegram_api`` path so semantics match the pre-cluster behaviour
    exactly (best-effort, silently swallowed on failure).
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
        # Drain per HTTP/1.1 keep-alive rules — leaving unread body bytes
        # on the socket poisons the next request on the same connection.
        resp.read()
        if resp.status >= 500:
            raise RuntimeError("typing 5xx %d" % resp.status)
        return
    except Exception:
        _reset_typing_conn()

    # Fallback: one-shot, exactly matching the pre-cluster path (silent on
    # failure). Duplicate typing indicators are invisible, so retrying on a
    # fresh connection is safe here.
    try:
        telegram_api(token, "sendChatAction", {
            "chat_id": chat_id,
            "action": "typing",
        }, timeout=10)
    except Exception:
        pass
