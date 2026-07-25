"""Discord webhook logger — sends structured log messages to Discord.

Functions are fire-and-forget; failures are silently swallowed so they never
block the trading loop.
"""

import json
import logging
import threading
from typing import Optional

logger = logging.getLogger(__name__)

_webhook_url: Optional[str] = None
_bot_name: Optional[str] = None


def init(webhook_url: str):
    """Set the Discord webhook URL."""
    global _webhook_url
    _webhook_url = webhook_url
    logger.info("Discord logger initialized")


def set_bot_name(bot_name: str) -> None:
    """Tag every subsequent message with ``[bot_name]`` so a shared webhook can
    be used by several trader_bot.py processes at once and the user can still
    tell which one logged what."""
    global _bot_name
    _bot_name = bot_name or None


def is_configured() -> bool:
    return bool(_webhook_url)


def _tag(content: str) -> str:
    return f"**[{_bot_name}]** {content}" if _bot_name else content


def _send(content: str):
    """Fire-and-forget POST to Discord webhook."""
    if not _webhook_url:
        return
    content = _tag(content)
    def _post():
        try:
            import requests
            requests.post(_webhook_url, json={"content": content}, timeout=5)
        except Exception:
            pass
    threading.Thread(target=_post, daemon=True).start()


def _send_file(content: str, file_bytes: bytes, filename: str):
    """Fire-and-forget multipart POST with an attached file (e.g. PNG chart)."""
    if not _webhook_url:
        return
    content = _tag(content)

    def _post():
        try:
            import requests

            files = {"file": (filename, file_bytes, "image/png")}
            data = {"payload_json": json.dumps({"content": content})}
            requests.post(_webhook_url, data=data, files=files, timeout=10)
        except Exception:
            pass

    threading.Thread(target=_post, daemon=True).start()


def log(msg: str, level: str = "info"):
    """General log message."""
    prefix = {"error": "❌", "warning": "⚠️", "info": "ℹ️"}.get(level, "📝")
    _send(f"{prefix} {msg}")


def log_startup(msg: str):
    _send(f"🚀 {msg}")


def log_error(msg: str):
    _send(f"❌ {msg}")


def log_settlement(msg: str):
    _send(f"📊 {msg}")


def log_fill(msg: str):
    _send(f"✅ {msg}")


def log_order(msg: str):
    _send(f"📋 {msg}")


def log_latency(msg: str):
    _send(f"⏱️ {msg}")


def log_trade_chart(content: str, image_bytes: bytes, filename: str = "chart.png"):
    """Post a trade summary message with an attached PNG chart."""
    _send_file(content, image_bytes, filename)
