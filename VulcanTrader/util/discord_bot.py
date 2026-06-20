"""Discord bot with slash commands for VulcanTrader.

Runs in a background daemon thread alongside the trading engine.
Requires discord.py >= 2.0:  pip install discord.py

Slash commands
--------------
  /help          List available commands.
  /myid          Show your Discord user ID (whitelist setup helper).
  /opentrades    Show open trades with unrealized metrics + charts.
  /exit <pair>   Schedule a force exit on the given pair.
  /stats         Session-wide performance metrics.

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


# ---------------------------------------------------------------------------
# DiscordBot
# ---------------------------------------------------------------------------

class DiscordBot:
    """Wraps a discord.py application-commands client in a background daemon thread.

    Lifecycle
    ---------
    1. Instantiate after config is loaded.
    2. Call :meth:`set_trading_bot` once VulcanTraderBot is ready.
    3. Call :meth:`start` to launch the background thread.
    4. Call :meth:`stop` on shutdown.
    """

    def __init__(self, config: dict, trading_bot: Any | None = None) -> None:
        self.config = config
        self.trading_bot = trading_bot

        discord_conf = config.get("discord") or {}
        self._token: str = discord_conf.get("bot_token", "")
        raw_ids = discord_conf.get("allowed_user_ids", [])
        self._allowed_ids: set[int] = {int(x) for x in raw_ids if str(x).strip().lstrip("-").isdigit()}

        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._client: Any = None  # discord.Client

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def set_trading_bot(self, trading_bot: Any) -> None:
        """Attach the live VulcanTraderBot after it is constructed."""
        self.trading_bot = trading_bot

    def start(self) -> None:
        """Launch the background daemon thread. No-op if bot_token is missing."""
        if not self._token:
            logger.info("Discord bot_token not configured — slash commands disabled.")
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
    # Internal: background thread entry point
    # ------------------------------------------------------------------

    def _run(self) -> None:
        try:
            import discord
        except ImportError:
            logger.error(
                "discord.py is not installed — Discord commands are unavailable. "
                "Install with:  pip install discord.py"
            )
            return

        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)

        intents = discord.Intents.default()
        client = discord.Client(intents=intents)
        tree = discord.app_commands.CommandTree(client)
        self._client = client

        # capture in closure
        allowed_ids = self._allowed_ids
        bot_self = self

        def _authorised(interaction: discord.Interaction) -> bool:
            return (not allowed_ids) or (interaction.user.id in allowed_ids)

        # ── /help ────────────────────────────────────────────────────

        @tree.command(name="help", description="Show available VulcanTrader bot commands.")
        async def cmd_help(interaction: discord.Interaction) -> None:
            if not _authorised(interaction):
                await interaction.response.send_message("❌ Not authorised.", ephemeral=True)
                return
            embed = discord.Embed(title="VulcanTrader Bot", color=0x00BFFF)
            embed.add_field(name="/help", value="Show this message.", inline=False)
            embed.add_field(
                name="/myid",
                value="Show your Discord user ID — useful for adding yourself to the whitelist.",
                inline=False,
            )
            embed.add_field(
                name="/opentrades",
                value="List open trades with unrealized P&L, ROI and charts.",
                inline=False,
            )
            embed.add_field(
                name="/exit <pair>",
                value="Force-exit the open trade on the given pair (e.g. `BTC/USDC:USDC`).",
                inline=False,
            )
            embed.add_field(
                name="/stats",
                value=(
                    "Session-wide metrics: Trades, Win rate, PnL, ROI, CAGR, "
                    "Sharpe, Profit Factor, Expectancy R, Avg Win/Loss."
                ),
                inline=False,
            )
            await interaction.response.send_message(embed=embed)

        # ── /opentrades ──────────────────────────────────────────────

        @tree.command(
            name="opentrades",
            description="Show open trades with unrealized metrics and charts.",
        )
        async def cmd_opentrades(interaction: discord.Interaction) -> None:
            if not _authorised(interaction):
                await interaction.response.send_message("❌ Not authorised.", ephemeral=True)
                return
            await interaction.response.defer()

            tb = bot_self.trading_bot
            if tb is None:
                await interaction.followup.send("⚠️ Trading bot not attached.")
                return

            try:
                from VulcanTrader.persistence import Trade
                open_trades = Trade.get_open_trades()
            except Exception as exc:
                await interaction.followup.send(f"❌ Error fetching trades: {exc}")
                return

            if not open_trades:
                await interaction.followup.send("📭 No open trades.")
                return

            stake_ccy = bot_self.config.get("stake_currency", "")
            for trade in open_trades:
                try:
                    await bot_self._send_open_trade(interaction, trade, tb, stake_ccy)
                except Exception as exc:
                    logger.warning(
                        "Error sending open trade %s: %s",
                        getattr(trade, "pair", "?"),
                        exc,
                    )

        # ── /exit ────────────────────────────────────────────────────

        @tree.command(name="exit", description="Force-exit an open trade by pair.")
        @discord.app_commands.describe(pair="The pair to exit, e.g. BTC/USDC:USDC")
        async def cmd_exit(interaction: discord.Interaction, pair: str) -> None:
            if not _authorised(interaction):
                await interaction.response.send_message("❌ Not authorised.", ephemeral=True)
                return

            tb = bot_self.trading_bot
            if tb is None:
                await interaction.response.send_message(
                    "⚠️ Trading bot not attached.", ephemeral=True
                )
                return

            pair = pair.strip()
            try:
                from VulcanTrader.persistence import Trade
                open_trades = Trade.get_open_trades()
                matched = [t for t in open_trades if t.pair.lower() == pair.lower()]
            except Exception as exc:
                await interaction.response.send_message(f"❌ Error: {exc}", ephemeral=True)
                return

            if not matched:
                await interaction.response.send_message(
                    f"⚠️ No open trade found for `{pair}`.", ephemeral=True
                )
                return

            for trade in matched:
                tb._schedule_force_exit(trade.id)

            count = len(matched)
            await interaction.response.send_message(
                f"📋 Force-exit scheduled for `{pair}` "
                f"({count} trade{'s' if count != 1 else ''}) — "
                "will execute on the next process cycle."
            )

        # ── /myid ────────────────────────────────────────────────────

        @tree.command(name="myid", description="Show your Discord user ID (useful for whitelist setup).")
        async def cmd_myid(interaction: discord.Interaction) -> None:
            # Open to everyone — no authorisation check.
            uid = interaction.user.id
            in_whitelist = (not allowed_ids) or (uid in allowed_ids)
            status = "✅ whitelisted" if in_whitelist else "⛔ not whitelisted"
            await interaction.response.send_message(
                f"Your Discord user ID is `{uid}` — {status}.",
                ephemeral=True,
            )

        # ── /stats ───────────────────────────────────────────────────

        @tree.command(name="stats", description="Show session-wide performance metrics.")
        async def cmd_stats(interaction: discord.Interaction) -> None:
            if not _authorised(interaction):
                await interaction.response.send_message("❌ Not authorised.", ephemeral=True)
                return
            await interaction.response.defer()

            tb = bot_self.trading_bot
            stake_ccy = bot_self.config.get("stake_currency", "")
            try:
                text = bot_self._build_stats_text(tb, stake_ccy)
                await interaction.followup.send(text)
            except Exception as exc:
                await interaction.followup.send(f"❌ Error computing stats: {exc}")

        # ── Bot events ───────────────────────────────────────────────

        @client.event
        async def on_ready() -> None:
            await tree.sync()
            logger.info(
                "Discord bot logged in as %s — slash commands synced.", client.user
            )

        try:
            self._loop.run_until_complete(client.start(self._token))
        except Exception:
            logger.exception("Discord bot crashed.")

    # ------------------------------------------------------------------
    # Command helpers
    # ------------------------------------------------------------------

    async def _send_open_trade(
        self,
        interaction: Any,
        trade: Any,
        tb: Any,
        stake_ccy: str,
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
            f"{pnl_emoji} **{pair}** `{direction}` ×{lev}\n"
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
            await interaction.followup.send(body, file=_discord.File(io.BytesIO(png), filename=fname))
        else:
            await interaction.followup.send(body)

    def _build_stats_text(self, tb: Any, stake_ccy: str) -> str:
        """Compute session-wide stats from closed trades and return a formatted string."""
        from VulcanTrader.persistence import Trade

        closed = Trade.get_trades_proxy(is_open=False)
        if not closed:
            return "📊 No closed trades yet."

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
            return "📊 No closed trades with P&L data yet."

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
            f"📊 **Session Stats**\n"
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
