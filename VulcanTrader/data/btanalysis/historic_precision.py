import numpy as np
from pandas import DataFrame, Series


def _significant_decimals(values: Series) -> np.ndarray:
    """Number of significant decimal places for each value.

    Vectorised equivalent of the original
    ``round(14).apply("{:.15f}".format).str.extract(r"\\.(\\d*[1-9])").str.len()``
    — i.e. the position of the LAST non-zero digit within the first 15 decimal
    places (0 when there is no fractional part).

    The string/regex version formatted and regex-scanned every OHLC value and
    dominated ``generate_backtest_stats`` (4.3s of a 4.9s stats phase on a
    45-pair run). This walks the 15 decimal places with numpy instead, which is
    the same computation without per-row Python objects.
    """
    v = np.asarray(values, dtype=np.float64)
    if v.size == 0:
        return np.zeros(0, dtype=np.int64)

    # The original formatted with "{:.15f}" (15 decimal places, half-even
    # rounding) and found the last non-zero digit. Reproduce that by taking the
    # fractional part as a scaled integer: frac_int = round(frac * 1e15), then
    # strip trailing zeros. Working in integers avoids the drift that a
    # digit-by-digit float walk accumulates.
    finite = np.isfinite(v)
    scaled = np.where(finite, np.round(v, 14), 0.0)
    frac = scaled - np.floor(scaled)
    frac_int = np.rint(frac * 1e15).astype(np.int64)

    # Rounding the fraction up to a whole unit means no fractional digits left.
    frac_int = np.where(frac_int >= 10**15, 0, frac_int)

    last_nonzero = np.zeros(v.shape, dtype=np.int64)
    nonzero = frac_int != 0
    # Strip trailing zeros: each division that leaves no remainder removes one
    # decimal place from the right. 15 passes covers the full formatted width.
    work = frac_int.copy()
    places = np.full(v.shape, 15, dtype=np.int64)
    for _ in range(15):
        divisible = nonzero & (work % 10 == 0) & (places > 0)
        if not divisible.any():
            break
        work = np.where(divisible, work // 10, work)
        places = np.where(divisible, places - 1, places)
    last_nonzero = np.where(nonzero, places, 0)

    # NaN prices carry no precision information.
    last_nonzero[~finite] = 0
    return last_nonzero


def get_tick_size_over_time(candles: DataFrame) -> Series:
    """
    Calculate the number of significant digits for candles over time.
    It's using the Monthly maximum of the number of significant digits for each month.
    :param candles: DataFrame with OHLCV data
    :return: Series with the average number of significant digits for each month
    """
    # An empty candle set has no precision to infer. Guarded explicitly because
    # the previous string-based implementation raised
    # "Can only use .str accessor with string values" here — an empty column
    # formats to a float64 Series, so `.str` was invalid (hit by pairs whose
    # feather file has 0 rows, e.g. a delisted symbol).
    if candles.empty:
        return Series(dtype="float64")

    counts = np.zeros(len(candles), dtype=np.int64)
    for col in ["open", "high", "low", "close"]:
        counts = np.maximum(counts, _significant_decimals(candles[col]))

    # A value with no fractional digits yielded NaN in the original (its regex
    # simply didn't match), and NaN propagates through the monthly max. Preserve
    # that rather than treating "no decimals" as 0 -> tick size 1.0, which would
    # change the reported precision for whole-number prices (e.g. BTC at 95000.0).
    counts_f = counts.astype("float64")
    counts_f[counts == 0] = np.nan
    candles = candles.assign(max_count=counts_f)

    candles1 = candles.set_index("date", drop=True)
    # Group by month and calculate the average number of significant digits
    monthly_count_avg1 = candles1["max_count"].resample("MS").max()
    # convert monthly_open_count_avg from 5.0 to 0.00001, 4.0 to 0.0001, ...
    monthly_open_count_avg = 1 / 10**monthly_count_avg1

    return monthly_open_count_avg
