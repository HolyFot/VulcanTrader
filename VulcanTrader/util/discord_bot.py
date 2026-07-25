"""Discord bot with ``!`` prefix commands for VulcanTrader.

Runs in a background daemon thread. A single instance can log and control
*several* trader_bot.py processes at once — each one is identified by its
account name (the persistence DB stem under ``user_data/accounts/``, the same
name used by web_portal's ``/api/livebots``). Every command that touches a
bot's trades or state takes a required ``bot`` argument so it is always
explicit which trader_bot instance is being read or controlled.

One bot in the fleet is "attached" — the ``trading_bot`` object living in
this same process — and gets rich, live treatment (unrealized P&L, charts,
SNR, full session stats). Every other bot is "remote": data comes from its
account JSON snapshot via the attached ``WebPortal`` (no live rate or chart),
and control actions (force-exit, stop) are handed off through small sentinel
files next to that bot's persistence DB, which its own process polls — see
``WebPortal._request_account_force_exit`` / ``_request_account_stop``.

Requires discord.py >= 2.0:  pip install discord.py

IMPORTANT - Message Content Intent: prefix commands need the bot to read raw
message text, which requires the privileged "MESSAGE CONTENT INTENT" toggle
enabled for this application in the Discord Developer Portal (Bot page), in
addition to ``intents.message_content = True`` set here in code. Without it,
the bot logs in and appears online but never sees/responds to any ``!``
command.

Prefix commands (default prefix ``!``)
---------------------------------------
  !help                List available commands.
  !myid                Show your Discord user ID (whitelist setup helper).
  !bots                List every detected trader_bot instance and its status.
  !opentrades <bot>    Show open trades for one bot, with unrealized metrics
                       + charts when it's the attached bot.
  !exit <bot> <pair>   Schedule a force exit on the given pair for that bot.
  !stats <bot>         Session-wide performance metrics for that bot.
  !stop <bot>          Pause the attached bot, or terminate a remote bot's process.

``<bot>`` is the account name shown in brackets by ``!bots`` (e.g.
``ParabolicSquaredReturnReversionSpot``) - use that, not the friendly display
name, since the latter may contain spaces or collide across configs.

Configuration (under the ``discord`` config key)
-------------------------------------------------
  bot_token        : Discord bot token (required for commands to work).
  allowed_user_ids : List of Discord user-ID integers that may use commands.
                     Empty list → everyone can use commands.
  webhook_url      : Existing outbound-only webhook (unchanged).
"""

from __future__ import annotations

import asyncio
import io
import logging
import threading
from typing import TYPE_CHECKING, Any

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fmt_duration(dur: Any) -> str:
    """Format a timedelta as 'Xd Xh Xm'."""
    total_secs = int(dur.total_seconds())
    d, rem = divmod(total_secs, 86400)
    h, rem = divmod(rem, 3600)
    m, _ = divmod(rem, 60)
    parts = []
    if d:
        parts.append(f"{d}d")
    if h:
        parts.append(f"{h}h")
    parts.append(f"{m}m")
    return " ".join(parts) or "0m"


def _pf_str(profit_factor: float) -> str:
    if profit_factor == float("inf"):
        return "∞"
    return f"{profit_factor:.2f}"


def _bot_status_emoji(info: dict) -> str:
    if info.get("running"):
        return "🟢"
    if info.get("crashed"):
        return "⚠️"
    return "🔴"


class _VoiceNoiseFilter(logging.Filter):
    """Suppress discord.py's PyNaCl/davey "voice will NOT be supported"
    warnings - both are about voice-channel audio (encryption / Discord's
    newer DAVE E2EE protocol), which this bot never touches; it only ever
    posts/reads text in text channels."""

    def filter(self, record: logging.LogRecord) -> bool:  # type: ignore[override]
        msg = record.getMessage()
        return "voice will NOT be supported" not in msg


# ---------------------------------------------------------------------------
# DiscordBot
# ---------------------------------------------------------------------------

class DiscordBot:
    """Wraps a discord.py prefix-commands bot in a background daemon thread.

    Lifecycle
    ---------
    1. Instantiate after config is loaded.
    2. Call :meth:`start` to launch the background thread.
    3. Call :meth:`stop` on shutdown.
    """

    def __init__(self, config: dict, trading_bot: Any | None = None, portal: Any | None = None) -> None:
        self.config = config
        self.trading_bot = trading_bot
        self.portal = portal

        discord_conf = config.get("discord") or {}
        self._token: str = discord_conf.get("bot_token", "")
        raw_ids = discord_conf.get("allowed_user_ids", [])
        self._allowed_ids: set[int] = {int(x) for x in raw_ids if str(x).strip().lstrip("-").isdigit()}

        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._client: Any = None  # discord.ext.commands.Bot

        self._own_account_name: str | None = self._resolve_own_account_name()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def set_trading_bot(self, trading_bot: Any) -> None:
        """Attach the live VulcanTraderBot after it is constructed."""
        self.trading_bot = trading_bot
        self._own_account_name = self._resolve_own_account_name()

    def start(self) -> None:
        """Launch the background daemon thread. No-op if bot_token is missing."""
        if not self._token:
            logger.info("Discord bot_token not configured — commands disabled.")
            return
        self._thread = threading.Thread(target=self._run, name="DiscordBot", daemon=True)
        self._thread.start()
        logger.info("Discord bot thread started.")

    def stop(self) -> None:
        """Signal the bot to shut down gracefully."""
        if self._loop and self._client:
            try:
                asyncio.run_coroutine_threadsafe(self._client.close(), self._loop)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Multi-bot resolution
    # ------------------------------------------------------------------

    def _resolve_own_account_name(self) -> str | None:
        """Account name (db stem) of the trader_bot attached in this process, if any."""
        db_url = self.config.get("db_url")
        if not db_url:
            return None
        try:
            from VulcanTrader.persistence.models import _path_from_db_url
            return _path_from_db_url(db_url).stem
        except Exception:
            return None

    def _list_bots(self) -> list[dict]:
        """Every detected trader_bot account, via the attached WebPortal's scan.

        Without a portal (``--no-web``) there is no cross-process visibility -
        degrade to a single synthesized entry for the attached bot, if any, so
        the ``bot`` argument still resolves to something.
        """
        if self.portal is not None:
            try:
                return self.portal._scan_live_bots()
            except Exception:
                logger.exception("bot listing via portal failed")
        if self.trading_bot is not None and self._own_account_name:
            strat = getattr(self.trading_bot, "strategy", None)
            return [{
                "name": self._own_account_name,
                "bot_name": self.config.get("bot_name") or self._own_account_name,
                "running": True,
                "crashed": False,
                "mode": "dry_run" if self.config.get("dry_run") else "live",
                "strategy": type(strat).__name__ if strat is not None else None,
                "exchange": (self.config.get("exchange") or {}).get("name"),
                "open_trades": None,
                "closed_trades": None,
                "closed_profit_abs": None,
            }]
        return []

    @staticmethod
    def _resolve_bot(bots: list[dict], query: str) -> dict | None:
        """Match a user-supplied ``bot`` argument to a scanned account.

        Accepts either the account name (unique, used for control) or the
        friendly ``bot_name`` label (may collide across configs — first match wins).
        """
        q = query.strip().lower()
        for b in bots:
            if b["name"].lower() == q:
                return b
        for b in bots:
            if (b.get("bot_name") or "").lower() == q:
                return b
        return None

    # ------------------------------------------------------------------
    # Internal: background thread entry point
    # ------------------------------------------------------------------

    def _run(self) -> None:
        try:
            import discord
            from discord.ext import commands
        except ImportError:
            logger.error(
                "discord.py is not installed — Discord commands are unavailable. "
                "Install with:  pip install discord.py"
            )
            return

        logging.getLogger("discord.client").addFilter(_VoiceNoiseFilter())

        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)

        intents = discord.Intents.default()
        intents.message_content = True  # required to read "!command" text - enable
        # the privileged "MESSAGE CONTENT INTENT" toggle for this app too (see module docstring).
        bot_client = commands.Bot(command_prefix="!", intents=intents, help_command=None)
        self._client = bot_client

        # capture in closure
        allowed_ids = self._allowed_ids
        bot_self = self

        def _authorised(ctx: "commands.Context") -> bool:
            return (not allowed_ids) or (ctx.author.id in allowed_ids)

        # ── !help ────────────────────────────────────────────────────

        @bot_client.command(name="help")
        async def cmd_help(ctx: "commands.Context") -> None:
            if not _authorised(ctx):
                await ctx.send("❌ Not authorised.")
                return
            embed = discord.Embed(title="VulcanTrader Bot", color=0x00BFFF)
            embed.add_field(name="!help", value="Show this message.", inline=False)
            embed.add_field(
                name="!myid",
                value="Show your Discord user ID — useful for adding yourself to the whitelist.",
                inline=False,
            )
            embed.add_field(
                name="!bots",
                value="List every detected trader_bot instance and its status.",
                inline=False,
            )
            embed.add_field(
                name="!opentrades <bot>",
                value="List open trades for one bot, with unrealized P&L, ROI and charts "
                      "when it's the attached bot.",
                inline=False,
            )
            embed.add_field(
                name="!exit <bot> <pair>",
                value="Force-exit the open trade on the given pair for that bot "
                      "(e.g. `BTC/USDC:USDC`).",
                inline=False,
            )
            embed.add_field(
                name="!stats <bot>",
                value=(
                    "Session-wide metrics for that bot: Trades, Win rate, PnL, ROI, CAGR, "
                    "Sharpe, Profit Factor, Expectancy R, Avg Win/Loss "
                    "(remote bots get a reduced set — no live CAGR/Sharpe/ROI)."
                ),
                inline=False,
            )
            embed.add_field(
                name="!stop <bot>",
                value="Pause the attached bot, or terminate a remote bot's process.",
                inline=False,
            )
            embed.set_footer(text="<bot> is the account name from !bots, e.g. MyStrategy_dry_run")
            await ctx.send(embed=embed)

        # ── !bots ────────────────────────────────────────────────────

        @bot_client.command(name="bots")
        async def cmd_bots(ctx: "commands.Context") -> None:
            if not _authorised(ctx):
                await ctx.send("❌ Not authorised.")
                return

            bots = bot_self._list_bots()
            if not bots:
                await ctx.send(
                    "📭 No trader_bot accounts detected "
                    "(attach a web portal for cross-process visibility)."
                )
                return

            embed = discord.Embed(title="VulcanTrader — Bots", color=0x00BFFF)
            for b in bots:
                status = _bot_status_emoji(b)
                status_word = "running" if b.get("running") else ("crashed" if b.get("crashed") else "stopped")
                mine = "  *(this bot)*" if b["name"] == bot_self._own_account_name else ""
                closed_pnl = b.get("closed_profit_abs")
                lines = (
                    f"{status} {status_word}{mine}\n"
                    f"Strategy: {b.get('strategy') or '?'}\n"
                    f"Exchange: {b.get('exchange') or '?'}   Mode: {b.get('mode') or '?'}\n"
                    f"Open: {b.get('open_trades')}   Closed: {b.get('closed_trades')}"
                    + (f"  (PnL: {closed_pnl:+.2f})" if isinstance(closed_pnl, (int, float)) else "")
                )
                title = b.get("bot_name") or b["name"]
                if title != b["name"]:
                    title += f"  [{b['name']}]"
                embed.add_field(name=title, value=f"```\n{lines}\n```", inline=False)
            await ctx.send(embed=embed)

        # ── !opentrades ──────────────────────────────────────────────

        @bot_client.command(name="opentrades")
        async def cmd_opentrades(ctx: "commands.Context", bot: str) -> None:
            if not _authorised(ctx):
                await ctx.send("❌ Not authorised.")
                return

            info = bot_self._resolve_bot(bot_self._list_bots(), bot)
            if info is None:
                await ctx.send(f"⚠️ No bot named `{bot}` found — see !bots.")
                return
            label = info.get("bot_name") or info["name"]

            if info["name"] == bot_self._own_account_name and bot_self.trading_bot is not None:
                tb = bot_self.trading_bot
                try:
                    from VulcanTrader.persistence import Trade
                    open_trades = Trade.get_open_trades()
                except Exception as exc:
                    await ctx.send(f"❌ Error fetching trades: {exc}")
                    return
                if not open_trades:
                    await ctx.send(f"📭 No open trades on `{label}`.")
                    return
                stake_ccy = bot_self.config.get("stake_currency", "")
                for trade in open_trades:
                    try:
                        await bot_self._send_open_trade(ctx, trade, tb, stake_ccy, label)
                    except Exception as exc:
                        logger.warning(
                            "Error sending open trade %s: %s",
                            getattr(trade, "pair", "?"),
                            exc,
                        )
                return

            # Remote bot: JSON snapshot only, via the attached WebPortal.
            if bot_self.portal is None:
                await ctx.send("⚠️ Web portal not attached — cannot inspect remote bots.")
                return
            try:
                dash = bot_self.portal._account_dashboard(info["name"], closed_limit=1)
            except Exception as exc:
                detail = getattr(exc, "detail", str(exc))
                await ctx.send(f"❌ {detail}")
                return
            open_trades = dash["open_trades"]
            if not open_trades:
                await ctx.send(f"📭 No open trades on `{label}`.")
                return
            for t in open_trades:
                await ctx.send(bot_self._format_json_open_trade(t, label))

        # ── !exit ────────────────────────────────────────────────────

        @bot_client.command(name="exit")
        async def cmd_exit(ctx: "commands.Context", bot: str, pair: str) -> None:
            if not _authorised(ctx):
                await ctx.send("❌ Not authorised.")
                return

            info = bot_self._resolve_bot(bot_self._list_bots(), bot)
            if info is None:
                await ctx.send(f"⚠️ No bot named `{bot}` found — see !bots.")
                return
            label = info.get("bot_name") or info["name"]
            pair = pair.strip()

            if info["name"] == bot_self._own_account_name and bot_self.trading_bot is not None:
                tb = bot_self.trading_bot
                try:
                    from VulcanTrader.persistence import Trade
                    open_trades = Trade.get_open_trades()
                    matched = [t for t in open_trades if t.pair.lower() == pair.lower()]
                except Exception as exc:
                    await ctx.send(f"❌ Error: {exc}")
                    return
                if not matched:
                    await ctx.send(f"⚠️ No open trade found for `{pair}` on `{label}`.")
                    return
                for trade in matched:
                    tb._schedule_force_exit(trade.id)
                count = len(matched)
                await ctx.send(
                    f"📋 `{label}`: force-exit scheduled for `{pair}` "
                    f"({count} trade{'s' if count != 1 else ''}) — "
                    "will execute on the next process cycle."
                )
                return

            if bot_self.portal is None:
                await ctx.send("⚠️ Web portal not attached — cannot control remote bots.")
                return
            try:
                result = bot_self.portal._request_account_force_exit(info["name"], pair)
            except Exception as exc:
                detail = getattr(exc, "detail", str(exc))
                await ctx.send(f"❌ {detail}")
                return
            await ctx.send(f"📋 `{label}`: {result['detail']}")

        # ── !myid ────────────────────────────────────────────────────

        @bot_client.command(name="myid")
        async def cmd_myid(ctx: "commands.Context") -> None:
            # Open to everyone — no authorisation check.
            uid = ctx.author.id
            in_whitelist = (not allowed_ids) or (uid in allowed_ids)
            status = "✅ whitelisted" if in_whitelist else "⛔ not whitelisted"
            await ctx.send(f"Your Discord user ID is `{uid}` — {status}.")

        # ── !stats ───────────────────────────────────────────────────

        @bot_client.command(name="stats")
        async def cmd_stats(ctx: "commands.Context", bot: str) -> None:
            if not _authorised(ctx):
                await ctx.send("❌ Not authorised.")
                return

            info = bot_self._resolve_bot(bot_self._list_bots(), bot)
            if info is None:
                await ctx.send(f"⚠️ No bot named `{bot}` found — see !bots.")
                return
            label = info.get("bot_name") or info["name"]

            if info["name"] == bot_self._own_account_name and bot_self.trading_bot is not None:
                tb = bot_self.trading_bot
                stake_ccy = bot_self.config.get("stake_currency", "")
                try:
                    text = bot_self._build_stats_text(tb, stake_ccy, label)
                    await ctx.send(text)
                except Exception as exc:
                    await ctx.send(f"❌ Error computing stats: {exc}")
                return

            if bot_self.portal is None:
                await ctx.send("⚠️ Web portal not attached — cannot inspect remote bots.")
                return
            try:
                dash = bot_self.portal._account_dashboard(info["name"], closed_limit=100000)
            except Exception as exc:
                detail = getattr(exc, "detail", str(exc))
                await ctx.send(f"❌ {detail}")
                return
            await ctx.send(bot_self._build_stats_text_remote(label, dash))

        # ── !stop ────────────────────────────────────────────────────

        @bot_client.command(name="stop")
        async def cmd_stop(ctx: "commands.Context", bot: str) -> None:
            if not _authorised(ctx):
                await ctx.send("❌ Not authorised.")
                return

            info = bot_self._resolve_bot(bot_self._list_bots(), bot)
            if info is None:
                await ctx.send(f"⚠️ No bot named `{bot}` found — see !bots.")
                return
            label = info.get("bot_name") or info["name"]

            if info["name"] == bot_self._own_account_name and bot_self.trading_bot is not None:
                try:
                    from VulcanTrader.enums import State
                    bot_self.trading_bot.state = State.PAUSED
                except Exception as exc:
                    await ctx.send(f"❌ Error: {exc}")
                    return
                await ctx.send(f"⏸️ `{label}` paused (trading halted; process keeps running).")
                return

            if bot_self.portal is None:
                await ctx.send("⚠️ Web portal not attached — cannot control remote bots.")
                return
            try:
                result = bot_self.portal._request_account_stop(info["name"])
            except Exception as exc:
                detail = getattr(exc, "detail", str(exc))
                await ctx.send(f"❌ {detail}")
                return
            await ctx.send(f"🛑 `{label}`: {result['detail']}")

        # ── Bot events ───────────────────────────────────────────────

        @bot_client.event
        async def on_ready() -> None:
            logger.info("Discord bot logged in as %s — prefix commands ready.", bot_client.user)
            # Posted here (not right after .start()) because that only means
            # the background thread launched, not that login actually
            # succeeded - this fires once the whole control plane (dashboard
            # + interactive Discord commands) is genuinely up and usable.
            try:
                from VulcanTrader.util import discord_logger
                webhook_url = (bot_self.config.get("discord") or {}).get("webhook_url")
                if webhook_url:
                    if not discord_logger.is_configured():
                        discord_logger.init(webhook_url)
                    discord_logger.set_bot_name("Bot Manager")
            except Exception:
                logger.exception("Failed to post bot-manager-started message to Discord")
            # A single, unmissable line marking the moment the whole control
            # plane (dashboard + Discord commands) is actually up - everything
            # above this (discord.client/discord.gateway's own login chatter)
            # is plumbing a glance at the console shouldn't need to parse.
            logger.info("VulcanTrader Bot Manager Started!")

        @bot_client.event
        async def on_command_error(ctx: "commands.Context", error: Exception) -> None:
            if isinstance(error, commands.CommandNotFound):
                return
            if isinstance(error, commands.MissingRequiredArgument):
                await ctx.send(f"⚠️ Missing argument: `{error.param.name}`. See `!help`.")
                return
            logger.exception("Discord command %r raised", ctx.command, exc_info=error)
            await ctx.send(f"❌ Error: {error}")

        try:
            self._loop.run_until_complete(bot_client.start(self._token))
        except discord.errors.PrivilegedIntentsRequired:
            app_id = self._app_id_from_token()
            url = (
                f"https://discord.com/developers/applications/{app_id}/bot"
                if app_id else "https://discord.com/developers/applications"
            )
            logger.error(
                "Discord bot could not start: MESSAGE CONTENT INTENT is not enabled for "
                "this bot application. Prefix ('!') commands need to read raw message text, "
                "which Discord gates behind this privileged intent. Fix: go to %s, scroll to "
                "'Privileged Gateway Intents', enable 'MESSAGE CONTENT INTENT', save, then "
                "restart this bot. (Webhook logging is unaffected - only the interactive "
                "'!' client needs this.)",
                url,
            )
        except discord.errors.LoginFailure:
            logger.error(
                "Discord bot could not start: login failed - discord.bot_token is invalid, "
                "expired, or was reset in the Developer Portal. Copy the current token from "
                "the app's Bot page and update discord.bot_token."
            )
        except Exception:
            logger.exception("Discord bot crashed.")

    def _app_id_from_token(self) -> str | None:
        """Decode the application/bot user ID from the first segment of a
        Discord bot token, so error messages can link straight to that
        specific app's Bot page instead of the generic applications list."""
        try:
            import base64
            first_segment = self._token.split(".")[0]
            padded = first_segment + "=" * (-len(first_segment) % 4)
            return base64.b64decode(padded).decode()
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Command helpers — attached (in-process) bot
    # ------------------------------------------------------------------

    async def _send_open_trade(
        self,
        ctx: Any,
        trade: Any,
        tb: Any,
        stake_ccy: str,
        bot_label: str,
    ) -> None:
        """Send a single open-trade summary card (text + optional chart)."""
        import discord as _discord

        pair = trade.pair
        direction = "Short" if trade.is_short else "Long"
        open_rate = trade.open_rate
        stake_amount = trade.stake_amount
        lev = getattr(trade, "leverage", 1) or 1

        # Unrealized P&L
        current_rate = open_rate
        unrealized_pnl = 0.0
        unrealized_ratio = 0.0
        try:
            current_rate = tb.exchange.get_rate(
                pair, side="exit", is_short=trade.is_short, refresh=False
            )
            profit_result = trade.calculate_profit(current_rate)
            unrealized_pnl = profit_result.profit_abs
            unrealized_ratio = profit_result.profit_ratio
        except Exception:
            pass

        # Duration
        dur_str = "n/a"
        try:
            from datetime import UTC, datetime
            dur_str = _fmt_duration(datetime.now(UTC) - trade.open_date_utc)
        except Exception:
            pass

        # SNR
        snr_str = "n/a"
        df = None
        try:
            df, _ = tb.dataprovider.get_analyzed_dataframe(pair, tb.strategy.timeframe)
            snr = tb._compute_entry_snr(df)
            if snr is not None:
                snr_str = f"{snr:.2f} ({tb._snr_quality(snr)})"
        except Exception:
            pass

        pnl_emoji = "📈" if unrealized_pnl >= 0 else "📉"
        tag = getattr(trade, "enter_tag", None) or ""
        tag_line = f"Tag:         {tag}\n" if tag else ""

        body = (
            f"{pnl_emoji} **[{bot_label}]** **{pair}** `{direction}` ×{lev}\n"
            f"```\n"
            f"Open rate:   {open_rate}\n"
            f"Current:     {current_rate}\n"
            f"Stake:       {stake_amount:.2f} {stake_ccy}\n"
            f"Unrealized:  {unrealized_pnl:+.4f} {stake_ccy}  "
            f"({unrealized_ratio * 100:+.2f}%)\n"
            f"Duration:    {dur_str}\n"
            f"SNR:         {snr_str}\n"
            f"{tag_line}"
            "```"
        )

        # Chart
        png: bytes | None = None
        try:
            from VulcanTrader.util.trade_chart import render_trade_chart
            if df is not None and len(df) > 0:
                png = render_trade_chart(
                    df,
                    pair=pair,
                    timeframe=tb.strategy.timeframe,
                    open_date=trade.open_date_utc,
                    open_rate=open_rate,
                    is_short=trade.is_short,
                    title_suffix="OPEN",
                )
        except Exception:
            pass

        if png:
            fname = f"{pair.replace('/', '_').replace(':', '_')}_open.png"
            await ctx.send(body, file=_discord.File(io.BytesIO(png), filename=fname))
        else:
            await ctx.send(body)

    def _build_stats_text(self, tb: Any, stake_ccy: str, bot_label: str) -> str:
        """Compute session-wide stats from closed trades and return a formatted string."""
        from VulcanTrader.persistence import Trade

        closed = Trade.get_trades_proxy(is_open=False)
        if not closed:
            return f"📊 No closed trades yet on `{bot_label}`."

        import pandas as pd
        from VulcanTrader.data.metrics import calculate_cagr, calculate_expectancy, calculate_sharpe

        rows = []
        for t in closed:
            pa = getattr(t, "close_profit_abs", None)
            pr = getattr(t, "close_profit", None)
            if pa is None:
                continue
            rows.append(
                {
                    "profit_abs": float(pa),
                    "profit_ratio": float(pr) if pr is not None else 0.0,
                    "open_date": t.open_date,
                    "close_date": t.close_date,
                }
            )

        if not rows:
            return f"📊 No closed trades with P&L data yet on `{bot_label}`."

        tdf = pd.DataFrame(rows)
        total_trades = len(tdf)
        wins = tdf[tdf["profit_abs"] > 0]
        losses = tdf[tdf["profit_abs"] < 0]
        win_count = len(wins)
        loss_count = len(losses)
        winrate = win_count / total_trades if total_trades else 0.0

        total_pnl = tdf["profit_abs"].sum()
        profit_sum = wins["profit_abs"].sum()
        loss_sum = abs(losses["profit_abs"].sum())
        profit_factor = (
            profit_sum / loss_sum if loss_sum > 0 else (float("inf") if profit_sum > 0 else 0.0)
        )

        starting_balance = float(
            (self.config.get("dry_run_wallet") or 0)
            or ((self.config.get("tradable_balance_ratio") or 1.0) * 1000.0)
        )
        starting_balance = max(starting_balance, 1.0)

        min_d = tdf["open_date"].min()
        max_d = tdf["close_date"].max()
        sharpe = calculate_sharpe(tdf, min_d, max_d, starting_balance)
        _, expectancy_r = calculate_expectancy(tdf)

        days = max(1, (max_d - min_d).days) if (max_d and min_d) else 1
        final_balance = starting_balance + total_pnl
        cagr = calculate_cagr(days, starting_balance, final_balance)
        roi_pct = total_pnl / starting_balance * 100

        avg_win = profit_sum / win_count if win_count else 0.0
        avg_loss = loss_sum / loss_count if loss_count else 0.0

        # Open trades unrealized PnL
        open_unrealized = 0.0
        open_count = 0
        try:
            from VulcanTrader.persistence import Trade as _Trade
            for ot in _Trade.get_open_trades():
                try:
                    rate = tb.exchange.get_rate(
                        ot.pair, side="exit", is_short=ot.is_short, refresh=False
                    )
                    open_unrealized += ot.calculate_profit(rate).profit_abs
                    open_count += 1
                except Exception:
                    pass
        except Exception:
            pass

        unrealized_line = (
            f"Open positions: {open_count}  (unrealized: {open_unrealized:+.2f} {stake_ccy})\n"
            if open_count
            else ""
        )

        return (
            f"📊 **Session Stats — `{bot_label}`**\n"
            f"```\n"
            f"Closed trades:  {total_trades}  "
            f"(W: {win_count} / L: {loss_count})\n"
            f"Win rate:       {winrate * 100:.1f}%\n"
            f"Total PnL:      {total_pnl:+.2f} {stake_ccy}\n"
            f"ROI:            {roi_pct:+.2f}%\n"
            f"CAGR:           {cagr * 100:+.2f}%\n"
            f"Sharpe:         {sharpe:.2f}\n"
            f"Profit Factor:  {_pf_str(profit_factor)}\n"
            f"Expectancy R:   {expectancy_r:+.2f}R\n"
            f"Avg Win:        {avg_win:+.4f} {stake_ccy}\n"
            f"Avg Loss:       -{avg_loss:.4f} {stake_ccy}\n"
            f"Period (days):  {days}\n"
            f"{unrealized_line}"
            "```"
        )

    # ------------------------------------------------------------------
    # Command helpers — remote bots (JSON account snapshot, via WebPortal)
    # ------------------------------------------------------------------

    def _format_json_open_trade(self, t: dict, bot_label: str) -> str:
        """Format one open trade taken from a remote bot's account JSON.

        No live exchange connection here, so unrealized P&L and charts aren't
        available — only what was persisted at entry.
        """
        pair = t.get("pair", "?")
        direction = "Short" if t.get("is_short") else "Long"
        lev = t.get("leverage") or 1
        open_rate = t.get("open_rate")
        stake_amount = t.get("stake_amount") or 0.0
        stake_ccy = t.get("stake_currency", "")
        tag = t.get("enter_tag") or ""
        open_date = t.get("open_date", "n/a")
        tag_line = f"Tag:         {tag}\n" if tag else ""

        return (
            f"📌 **[{bot_label}]** **{pair}** `{direction}` ×{lev}\n"
            f"```\n"
            f"Open rate:   {open_rate}\n"
            f"Stake:       {stake_amount:.2f} {stake_ccy}\n"
            f"Opened:      {open_date}\n"
            f"{tag_line}"
            "(live rate / unrealized P&L unavailable — remote bot)\n"
            "```"
        )

    def _build_stats_text_remote(self, bot_label: str, dash: dict) -> str:
        """Stats computed purely from a remote bot's account JSON snapshot.

        No CAGR/Sharpe/ROI: those need a starting balance this process has no
        reliable way to know for a bot it isn't attached to.
        """
        closed = dash.get("closed_trades") or []
        if not closed:
            return f"📊 No closed trades yet on `{bot_label}`."

        import pandas as pd

        rows = []
        for t in closed:
            pa = t.get("close_profit_abs")
            if pa is None:
                continue
            rows.append(
                {
                    "profit_abs": float(pa),
                    "profit_ratio": float(t.get("close_profit") or 0.0),
                }
            )
        if not rows:
            return f"📊 No closed trades with P&L data yet on `{bot_label}`."

        tdf = pd.DataFrame(rows)
        total_trades = len(tdf)
        wins = tdf[tdf["profit_abs"] > 0]
        losses = tdf[tdf["profit_abs"] < 0]
        win_count, loss_count = len(wins), len(losses)
        winrate = win_count / total_trades if total_trades else 0.0

        total_pnl = tdf["profit_abs"].sum()
        profit_sum = wins["profit_abs"].sum()
        loss_sum = abs(losses["profit_abs"].sum())
        profit_factor = (
            profit_sum / loss_sum if loss_sum > 0 else (float("inf") if profit_sum > 0 else 0.0)
        )
        avg_win = profit_sum / win_count if win_count else 0.0
        avg_loss = loss_sum / loss_count if loss_count else 0.0
        stake_ccy = closed[-1].get("stake_currency", "") if closed else ""
        open_count = len(dash.get("open_trades") or [])

        return (
            f"📊 **Session Stats — `{bot_label}`** _(remote — no live CAGR/Sharpe/ROI)_\n"
            f"```\n"
            f"Closed trades:  {total_trades}  (W: {win_count} / L: {loss_count})\n"
            f"Win rate:       {winrate * 100:.1f}%\n"
            f"Total PnL:      {total_pnl:+.2f} {stake_ccy}\n"
            f"Profit Factor:  {_pf_str(profit_factor)}\n"
            f"Avg Win:        {avg_win:+.4f} {stake_ccy}\n"
            f"Avg Loss:       -{avg_loss:.4f} {stake_ccy}\n"
            f"Open positions: {open_count}\n"
            "```"
        )
