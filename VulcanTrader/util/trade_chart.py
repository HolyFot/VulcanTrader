"""Render a candlestick chart with entry/exit markers for a single trade.

Used by :mod:`VulcanTrader.util.discord_logger` to attach a visual snapshot
to entry/exit notifications.
"""

from __future__ import annotations

import io
import logging
from datetime import datetime, timedelta
from typing import Iterable

logger = logging.getLogger(__name__)


def _ensure_datetime_index(df):
    """Return a DataFrame with a DatetimeIndex named 'Date' suitable for mplfinance."""
    import pandas as pd

    work = df.copy()
    if "date" in work.columns:
        work["date"] = pd.to_datetime(work["date"], utc=True).dt.tz_convert(None)
        work = work.set_index("date")
    work.index.name = "Date"
    # mplfinance expects capitalised column names
    rename_map = {c: c.capitalize() for c in ("open", "high", "low", "close", "volume") if c in work.columns}
    work = work.rename(columns=rename_map)
    return work[[c for c in ("Open", "High", "Low", "Close", "Volume") if c in work.columns]]


def _slice_window(df, center_dates: Iterable[datetime], pad_candles: int = 30):
    """Slice df to a window around the given dates with `pad_candles` of context."""
    import pandas as pd

    if df.empty:
        return df
    centers = [pd.Timestamp(c).tz_convert(None) if pd.Timestamp(c).tz else pd.Timestamp(c) for c in center_dates if c is not None]
    if not centers:
        return df.tail(120)
    lo = min(centers)
    hi = max(centers)
    # Indices nearest to lo / hi
    idx = df.index
    try:
        lo_pos = max(0, idx.searchsorted(lo) - pad_candles)
        hi_pos = min(len(idx), idx.searchsorted(hi) + pad_candles)
    except Exception:
        return df.tail(120)
    return df.iloc[lo_pos:hi_pos]


def render_trade_chart(
    df,
    *,
    pair: str,
    timeframe: str,
    open_date: datetime | None,
    open_rate: float | None,
    close_date: datetime | None = None,
    close_rate: float | None = None,
    is_short: bool = False,
    title_suffix: str = "",
) -> bytes | None:
    """Render a PNG of a candlestick chart with entry/exit markers.

    Returns the PNG bytes, or None if rendering failed (e.g. empty data).
    """
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import mplfinance as mpf
        import pandas as pd
    except ImportError:
        logger.debug("matplotlib/mplfinance not installed; skipping chart render")
        return None

    if df is None or len(df) == 0:
        return None

    try:
        ohlc = _ensure_datetime_index(df)
        if ohlc.empty or "Close" not in ohlc.columns:
            return None

        ohlc = _slice_window(ohlc, [open_date, close_date])
        if ohlc.empty:
            return None

        # Build entry/exit marker series aligned to ohlc index
        entry_series = pd.Series(index=ohlc.index, dtype="float64")
        exit_series = pd.Series(index=ohlc.index, dtype="float64")

        def _nearest_idx(ts):
            if ts is None:
                return None
            t = pd.Timestamp(ts)
            if t.tz is not None:
                t = t.tz_convert(None)
            pos = ohlc.index.searchsorted(t)
            if pos >= len(ohlc.index):
                pos = len(ohlc.index) - 1
            return ohlc.index[pos]

        if open_date is not None and open_rate is not None:
            i = _nearest_idx(open_date)
            if i is not None:
                entry_series.loc[i] = float(open_rate)
        if close_date is not None and close_rate is not None:
            i = _nearest_idx(close_date)
            if i is not None:
                exit_series.loc[i] = float(close_rate)

        addplots = []
        entry_marker = "v" if is_short else "^"
        exit_marker = "^" if is_short else "v"
        if entry_series.notna().any():
            addplots.append(
                mpf.make_addplot(entry_series, type="scatter", marker=entry_marker, markersize=180, color="#26a69a")
            )
        if exit_series.notna().any():
            addplots.append(
                mpf.make_addplot(exit_series, type="scatter", marker=exit_marker, markersize=180, color="#ef5350")
            )

        title = f"{pair} {timeframe}"
        if title_suffix:
            title += f"  {title_suffix}"

        style = mpf.make_mpf_style(
            base_mpf_style="nightclouds",
            rc={"axes.labelsize": 9, "axes.titlesize": 11, "font.size": 9},
        )

        buf = io.BytesIO()
        mpf.plot(
            ohlc,
            type="candle",
            style=style,
            addplot=addplots if addplots else None,
            volume="Volume" in ohlc.columns,
            title=title,
            figsize=(10, 5.5),
            tight_layout=True,
            savefig=dict(fname=buf, format="png", dpi=120, bbox_inches="tight"),
        )
        plt.close("all")
        return buf.getvalue()
    except Exception as e:
        logger.debug("render_trade_chart failed for %s: %s", pair, e)
        return None
