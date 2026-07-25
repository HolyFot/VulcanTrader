"""Cross-platform helper for launching a trader_bot.py subprocess in its own
visible terminal, so a fleet of bots (``bot.py autostart``, the web portal's
config manager, the legacy webserver-mode Start button) is inspectable/
killable by hand rather than running invisibly in the background.

Driven by two settings read from the specific bot config being launched -
not the shared system config - since different bots may want different
terminal-visibility behavior (see config_schema.py):

  show_terminal       : Windows only. True (default) opens a new console
                        window per subprocess; false launches with no window.
  tmux                : Linux/macOS only. True opens a new tmux window (in a
                        shared "VulcanTrader" session) per subprocess instead
                        of a plain, terminal-less background process. Falls
                        back to plain background if tmux isn't installed.
                        Default false.

Neither setting does anything on the other platform - Windows has no tmux
concept, Linux/macOS have no "new console window" API to call into from a
background process the way CREATE_NEW_CONSOLE does on Windows.
"""

from __future__ import annotations

import logging
import re
import shutil
import subprocess
import sys
from typing import Any

logger = logging.getLogger(__name__)

TMUX_SESSION = "VulcanTrader"


def _tmux_window_name(cmd: list[str]) -> str:
    """Tmux window name = this bot's --bot-name (i.e. the config's own
    bot_name - see config_schema.py), falling back to -c/--config if
    --bot-name isn't present. Sanitized to tmux-safe characters since
    bot_name may contain spaces ("Kraken Paper Test")."""
    name = None
    for flag in ("--bot-name", "-c", "--config"):
        if flag in cmd:
            idx = cmd.index(flag)
            if idx + 1 < len(cmd):
                name = cmd[idx + 1]
                break
    name = name or "vulcantrader-bot"
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", name)[:40]


def launch_detached(cmd: list[str], system_config: dict[str, Any] | None = None) -> subprocess.Popen:
    """Launch ``cmd`` as its own detached process, honouring the system
    config's ``show_terminal`` (Windows) / ``tmux`` (Linux/macOS)
    settings. Falls back to a plain background ``Popen`` otherwise."""
    cfg = system_config or {}

    if sys.platform == "win32":
        if cfg.get("show_terminal", True):
            return subprocess.Popen(cmd, creationflags=subprocess.CREATE_NEW_CONSOLE)
        return subprocess.Popen(cmd)

    if cfg.get("tmux", False):
        if shutil.which("tmux") is None:
            logger.warning(
                "system config requests tmux, but tmux isn't installed - "
                "launching %s as a plain background process instead.", cmd[0:1]
            )
            return subprocess.Popen(cmd)

        window_name = _tmux_window_name(cmd)
        target = f"{TMUX_SESSION}:{window_name}"
        # Kill any stale window left over from this same bot's previous run
        # (e.g. a crash that never closed its window) so restarting a bot
        # doesn't pile up duplicate windows under the same name. Harmless
        # no-op (non-zero exit, ignored) if no such window exists yet.
        subprocess.run(["tmux", "kill-window", "-t", target], capture_output=True)

        has_session = subprocess.run(
            ["tmux", "has-session", "-t", TMUX_SESSION],
            capture_output=True,
        ).returncode == 0
        if has_session:
            tmux_cmd = ["tmux", "new-window", "-t", TMUX_SESSION, "-n", window_name, *cmd]
        else:
            tmux_cmd = ["tmux", "new-session", "-d", "-s", TMUX_SESSION, "-n", window_name, *cmd]
        return subprocess.Popen(tmux_cmd)

    return subprocess.Popen(cmd)
