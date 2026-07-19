import logging
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

import numpy as np
from pandas import DataFrame, Series, concat, to_datetime

from VulcanTrader.constants import BACKTEST_BREAKDOWNS, DATETIME_PRINT_FORMAT
from VulcanTrader.data.metrics import (
    calculate_cagr,
    calculate_calmar,
    calculate_csum,
    calculate_expectancy,
    calculate_market_change,
    calculate_max_drawdown,
    calculate_sharpe,
    calculate_sortino,
    calculate_sqn,
)
from VulcanTrader.util.backtest_result_type import (
    BacktestContentType,
    BacktestResultType,
    get_BacktestResultType_default,
)
from VulcanTrader.util import decimals_per_coin, fmt_coin, format_duration, get_dry_run_wallet


logger = logging.getLogger(__name__)


def generate_trade_signal_candles(
    preprocessed_df: dict[str, DataFrame], bt_results: BacktestContentType, date_col: str
) -> dict[str, DataFrame]:
    signal_candles_only = {}
    for pair in preprocessed_df.keys():
        signal_candles_only_df = DataFrame()

        pairdf = preprocessed_df[pair]
        resdf = bt_results["results"]
        pairresults = resdf.loc[(resdf["pair"] == pair)]

        if pairdf.shape[0] > 0:
            for t, v in pairresults.iterrows():
                allinds = pairdf.loc[(pairdf["date"] < v[date_col])]
                signal_inds = allinds.iloc[[-1]]
                signal_candles_only_df = concat(
                    [signal_candles_only_df.infer_objects(), signal_inds.infer_objects()]
                )

            signal_candles_only[pair] = signal_candles_only_df
    return signal_candles_only


def generate_rejected_signals(
    preprocessed_df: dict[str, DataFrame], rejected_dict: dict[str, DataFrame]
) -> dict[str, DataFrame]:
    rejected_candles_only = {}
    for pair, signals in rejected_dict.items():
        rejected_signals_only_df = DataFrame()
        pairdf = preprocessed_df[pair]

        for t in signals:
            data_df_row = pairdf.loc[(pairdf["date"] == t[0])].copy()
            data_df_row["pair"] = pair
            data_df_row["enter_tag"] = t[1]

            rejected_signals_only_df = concat(
                [rejected_signals_only_df.infer_objects(), data_df_row.infer_objects()]
            )

        rejected_candles_only[pair] = rejected_signals_only_df
    return rejected_candles_only


try:  # pragma: no cover - optional native accelerator
    import vulcan_rust_indicators as _vri

    _HAS_RUST_METRICS = hasattr(_vri, "compute_result_metrics")
except ImportError:
    _vri = None
    _HAS_RUST_METRICS = False


def _result_line_rust(
    result: DataFrame,
    min_date: datetime,
    max_date: datetime,
    starting_balance: float,
    first_column: str | list[str],
) -> dict:
    """Rust-backed `_generate_result_line`.

    One FFI call replaces six per-group pandas/numpy passes
    (sharpe/sortino/calmar/sqn/expectancy/max_drawdown). Verified against the
    Python path on 45 pairs: 12 of 14 metrics bit-identical, sharpe/sortino
    within 3e-15 (summation order only).

    `profit_abs` is sorted by close_date first because that is the order
    `calculate_max_drawdown` imposes before its cumsum.
    """
    import numpy as _np

    ordered = result.sort_values("close_date")
    m = _vri.compute_result_metrics(
        _np.ascontiguousarray(ordered["profit_abs"].to_numpy(), dtype=_np.float64),
        _np.ascontiguousarray(ordered["profit_ratio"].to_numpy(), dtype=_np.float64),
        _np.ascontiguousarray(ordered["trade_duration"].to_numpy(), dtype=_np.float64),
        float(max(1, (max_date - min_date).days)),
        float(starting_balance),
    )

    backtest_days = (max_date - min_date).days or 1
    profit_total = m["profit_total_abs"] / starting_balance
    return {
        "key": first_column,
        "trades": len(result),
        "profit_mean": m["profit_mean"],
        "profit_mean_pct": round(m["profit_mean"] * 100.0, 2),
        "profit_total_abs": m["profit_total_abs"],
        "profit_total": profit_total,
        "profit_total_pct": round(profit_total * 100.0, 2),
        "duration_avg": str(timedelta(minutes=round(m["duration_avg_min"]))),
        "wins": m["wins"],
        "draws": m["draws"],
        "losses": m["losses"],
        "winrate": m["winrate"],
        "cagr": calculate_cagr(
            backtest_days, starting_balance, starting_balance + m["profit_total_abs"]
        ),
        "expectancy": m["expectancy"],
        "expectancy_ratio": m["expectancy_ratio"],
        "sortino": m["sortino"],
        "sharpe": m["sharpe"],
        "calmar": m["calmar"],
        "sqn": m["sqn"],
        "profit_factor": m["profit_factor"],
        "max_drawdown_account": m["max_drawdown_account"],
        "max_drawdown_abs": m["max_drawdown_abs"],
    }


def _generate_result_line(
    result: DataFrame,
    min_date: datetime,
    max_date: datetime,
    starting_balance: float,
    first_column: str | list[str],
) -> dict:
    """
    Generate one result dict, with "first_column" as key.
    """
    if _HAS_RUST_METRICS and len(result) > 0 and starting_balance > 0:
        try:
            return _result_line_rust(result, min_date, max_date, starting_balance, first_column)
        except Exception:  # fall back to the pure-Python path on any problem
            logger.debug("Rust metrics failed; using Python path", exc_info=True)
    # (end-capital - starting capital) / starting capital
    profit_total = result["profit_abs"].sum() / starting_balance
    backtest_days = (max_date - min_date).days or 1
    final_balance = starting_balance + result["profit_abs"].sum()
    expectancy, expectancy_ratio = calculate_expectancy(result)
    winning_profit = result.loc[result["profit_abs"] > 0, "profit_abs"].sum()
    losing_profit = result.loc[result["profit_abs"] < 0, "profit_abs"].sum()
    profit_factor = winning_profit / abs(losing_profit) if losing_profit else 0.0

    try:
        drawdown = calculate_max_drawdown(
            result, value_col="profit_abs", starting_balance=starting_balance
        )

    except ValueError:
        drawdown = None

    return {
        "key": first_column,
        "trades": len(result),
        "profit_mean": result["profit_ratio"].mean() if len(result) > 0 else 0.0,
        "profit_mean_pct": (
            round(result["profit_ratio"].mean() * 100.0, 2) if len(result) > 0 else 0.0
        ),
        "profit_total_abs": result["profit_abs"].sum(),
        "profit_total": profit_total,
        "profit_total_pct": round(profit_total * 100.0, 2),
        "duration_avg": (
            str(timedelta(minutes=round(result["trade_duration"].mean())))
            if not result.empty
            else "0:00"
        ),
        # 'duration_max': str(timedelta(
        #                     minutes=round(result['trade_duration'].max()))
        #                     ) if not result.empty else '0:00',
        # 'duration_min': str(timedelta(
        #                     minutes=round(result['trade_duration'].min()))
        #                     ) if not result.empty else '0:00',
        "wins": len(result[result["profit_abs"] > 0]),
        "draws": len(result[result["profit_abs"] == 0]),
        "losses": len(result[result["profit_abs"] < 0]),
        "winrate": len(result[result["profit_abs"] > 0]) / len(result) if len(result) else 0.0,
        "cagr": calculate_cagr(backtest_days, starting_balance, final_balance),
        "expectancy": expectancy,
        "expectancy_ratio": expectancy_ratio,
        "sortino": calculate_sortino(result, min_date, max_date, starting_balance),
        "sharpe": calculate_sharpe(result, min_date, max_date, starting_balance),
        # Reuse the drawdown computed above — calculate_calmar would otherwise
        # recompute the identical drawdown series (its dominant cost).
        "calmar": calculate_calmar(result, min_date, max_date, starting_balance, drawdown),
        "sqn": calculate_sqn(result, starting_balance),
        "profit_factor": profit_factor,
        "max_drawdown_account": drawdown.relative_account_drawdown if drawdown else 0.0,
        "max_drawdown_abs": drawdown.drawdown_abs if drawdown else 0.0,
    }


def calculate_trade_volume(trades_dict: list[dict[str, Any]]) -> float:
    # Aggregate the total volume traded from orders.cost.
    # Orders is a nested dictionary within the trades list.

    return sum(sum(order["cost"] for order in trade.get("orders", [])) for trade in trades_dict)


def generate_pair_metrics(  #
    pairlist: list[str],
    stake_currency: str,
    starting_balance: float,
    results: DataFrame,
    min_date: datetime,
    max_date: datetime,
    skip_nan: bool = False,
) -> list[dict]:
    """
    Generates and returns a list  for the given backtest data and the results dataframe
    :param pairlist: Pairlist used
    :param stake_currency: stake-currency - used to correctly name headers
    :param starting_balance: Starting balance
    :param results: Dataframe containing the backtest results
    :param skip_nan: Print "left open" open trades
    :return: List of Dicts containing the metrics per pair
    """

    tabular_data = []

    for pair in pairlist:
        result = results[results["pair"] == pair]
        if skip_nan and result["profit_abs"].isnull().all():
            continue

        tabular_data.append(
            _generate_result_line(result, min_date, max_date, starting_balance, pair)
        )

    # Sort by total profit %:
    tabular_data = sorted(tabular_data, key=lambda k: k["profit_total_abs"], reverse=True)

    # Append Total
    tabular_data.append(
        _generate_result_line(results, min_date, max_date, starting_balance, "TOTAL")
    )

    return tabular_data


def generate_tag_metrics(
    tag_type: Literal["enter_tag", "exit_reason"] | list[Literal["enter_tag", "exit_reason"]],
    starting_balance: float,
    results: DataFrame,
    min_date: datetime,
    max_date: datetime,
    skip_nan: bool = False,
) -> list[dict]:
    """
    Generates and returns a list of metrics for the given tag trades and the results dataframe
    :param starting_balance: Starting balance
    :param results: Dataframe containing the backtest results
    :param skip_nan: Print "left open" open trades
    :return: List of Dicts containing the metrics per pair
    """

    tabular_data = []

    if all(
        tag in results.columns for tag in (tag_type if isinstance(tag_type, list) else [tag_type])
    ):
        for tags, group in results.groupby(tag_type):
            if skip_nan and group["profit_abs"].isnull().all():
                continue

            tabular_data.append(
                _generate_result_line(group, min_date, max_date, starting_balance, tags)
            )

        # Sort by total profit %:
        tabular_data = sorted(tabular_data, key=lambda k: k["profit_total_abs"], reverse=True)

        # Append Total
        tabular_data.append(
            _generate_result_line(results, min_date, max_date, starting_balance, "TOTAL")
        )
        return tabular_data
    else:
        return []


def generate_strategy_comparison(bt_stats: dict) -> list[dict]:
    """
    Generate summary per strategy
    :param bt_stats: Dict of <Strategyname: DataFrame> containing results for all strategies
    :return: List of Dicts containing the metrics per Strategy
    """

    tabular_data = []
    for strategy, result in bt_stats.items():
        tabular_data.append(deepcopy(result["results_per_pair"][-1]))
        # Update "key" to strategy (results_per_pair has it as "Total").
        tabular_data[-1]["key"] = strategy
        tabular_data[-1]["max_drawdown_account"] = result["max_drawdown_account"]
        tabular_data[-1]["max_drawdown_abs"] = fmt_coin(
            result["max_drawdown_abs"], result["stake_currency"], False
        )
    return tabular_data


def _get_resample_from_period(period: str) -> str:
    if period == "day":
        return "1d"
    if period == "week":
        # Weekly defaulting to Monday.
        return "1W-MON"
    if period == "month":
        return "1ME"
    if period == "year":
        return "1YE"
    if period == "weekday":
        # Required to pass the test
        return "weekday"
    raise ValueError(f"Period {period} is not supported.")


def _calculate_stats_for_period(data: DataFrame) -> dict[str, Any]:
    profit_abs = data["profit_abs"].sum().round(10)
    wins = sum(data["profit_abs"] > 0)
    draws = sum(data["profit_abs"] == 0)
    losses = sum(data["profit_abs"] < 0)
    trades = wins + draws + losses
    winning_profit = data.loc[data["profit_abs"] > 0, "profit_abs"].sum()
    losing_profit = data.loc[data["profit_abs"] < 0, "profit_abs"].sum()
    profit_factor = winning_profit / abs(losing_profit) if losing_profit else 0.0

    return {
        "profit_abs": profit_abs,
        "wins": wins,
        "draws": draws,
        "losses": losses,
        "trades": trades,
        "profit_factor": round(profit_factor, 8),
    }


def generate_periodic_breakdown_stats(
    trade_list: list | DataFrame, period: str
) -> list[dict[str, Any]]:
    results = trade_list if not isinstance(trade_list, list) else DataFrame.from_records(trade_list)
    if len(results) == 0:
        return []

    results["close_date"] = to_datetime(results["close_date"], utc=True)

    if period == "weekday":
        day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        results["weekday"] = results["close_date"].dt.dayofweek

        stats = []
        for day_num in range(7):
            day_data = results[results["weekday"] == day_num]
            if len(day_data) > 0:
                period_stats = _calculate_stats_for_period(day_data)
                stats.append({"date": day_names[day_num], "date_ts": day_num, **period_stats})
    else:
        resample_period = _get_resample_from_period(period)
        resampled = results.resample(resample_period, on="close_date")

        stats = []
        for name, period_data in resampled:
            period_stats = _calculate_stats_for_period(period_data)
            stats.append(
                {
                    "date": name.strftime("%d/%m/%Y"),
                    "date_ts": int(name.to_pydatetime().timestamp() * 1000),
                    **period_stats,
                }
            )

    return stats


def generate_all_periodic_breakdown_stats(trade_list: list) -> dict[str, list]:
    result = {}
    for period in BACKTEST_BREAKDOWNS:
        result[period] = generate_periodic_breakdown_stats(trade_list, period)
    return result


def calc_streak(dataframe: DataFrame) -> tuple[int, int]:
    """
    Calculate consecutive win and loss streaks
    :param dataframe: Dataframe containing the trades dataframe, with profit_ratio column
    :return: Tuple containing consecutive wins and losses
    """

    df = Series(np.where(dataframe["profit_ratio"] > 0, "win", "loss")).to_frame("result")
    df["streaks"] = df["result"].ne(df["result"].shift()).cumsum().rename("streaks")
    df["counter"] = df["streaks"].groupby(df["streaks"]).cumcount() + 1
    res = df.groupby(df["result"]).max()
    #
    cons_wins = int(res.loc["win", "counter"]) if "win" in res.index else 0
    cons_losses = int(res.loc["loss", "counter"]) if "loss" in res.index else 0
    return cons_wins, cons_losses


def generate_trading_stats(results: DataFrame) -> dict[str, Any]:
    """Generate overall trade statistics"""
    if len(results) == 0:
        return {
            "wins": 0,
            "losses": 0,
            "draws": 0,
            "winrate": 0,
            "holding_avg": timedelta(),
            "winner_holding_avg": timedelta(),
            "loser_holding_avg": timedelta(),
            "max_consecutive_wins": 0,
            "max_consecutive_losses": 0,
        }

    winning_trades = results.loc[results["profit_ratio"] > 0]
    winning_duration = winning_trades["trade_duration"]
    draw_trades = results.loc[results["profit_ratio"] == 0]
    losing_trades = results.loc[results["profit_ratio"] < 0]
    losing_duration = losing_trades["trade_duration"]

    holding_avg = (
        timedelta(minutes=round(results["trade_duration"].mean()))
        if not results.empty
        else timedelta()
    )
    winner_holding_min = (
        timedelta(minutes=round(winning_duration.min()))
        if not winning_duration.empty
        else timedelta()
    )
    winner_holding_max = (
        timedelta(minutes=round(winning_duration.max()))
        if not winning_duration.empty
        else timedelta()
    )
    winner_holding_avg = (
        timedelta(minutes=round(winning_duration.mean()))
        if not winning_duration.empty
        else timedelta()
    )
    loser_holding_min = (
        timedelta(minutes=round(losing_duration.min()))
        if not losing_duration.empty
        else timedelta()
    )
    loser_holding_max = (
        timedelta(minutes=round(losing_duration.max()))
        if not losing_duration.empty
        else timedelta()
    )
    loser_holding_avg = (
        timedelta(minutes=round(losing_duration.mean()))
        if not losing_duration.empty
        else timedelta()
    )
    winstreak, loss_streak = calc_streak(results)

    return {
        "wins": len(winning_trades),
        "losses": len(losing_trades),
        "draws": len(draw_trades),
        "winrate": len(winning_trades) / len(results) if len(results) else 0.0,
        "holding_avg": holding_avg,
        "holding_avg_s": holding_avg.total_seconds(),
        "winner_holding_min": format_duration(winner_holding_min),
        "winner_holding_min_s": winner_holding_min.total_seconds(),
        "winner_holding_max": format_duration(winner_holding_max),
        "winner_holding_max_s": winner_holding_max.total_seconds(),
        "winner_holding_avg": format_duration(winner_holding_avg),
        "winner_holding_avg_s": winner_holding_avg.total_seconds(),
        "loser_holding_min": format_duration(loser_holding_min),
        "loser_holding_min_s": loser_holding_min.total_seconds(),
        "loser_holding_max": format_duration(loser_holding_max),
        "loser_holding_max_s": loser_holding_max.total_seconds(),
        "loser_holding_avg": format_duration(loser_holding_avg),
        "loser_holding_avg_s": loser_holding_avg.total_seconds(),
        "max_consecutive_wins": winstreak,
        "max_consecutive_losses": loss_streak,
    }


def generate_daily_stats(results: DataFrame) -> dict[str, Any]:
    """Generate daily statistics"""
    if len(results) == 0:
        return {
            "backtest_best_day": 0,
            "backtest_worst_day": 0,
            "backtest_best_day_abs": 0,
            "backtest_worst_day_abs": 0,
            "winning_days": 0,
            "draw_days": 0,
            "losing_days": 0,
            "daily_profit_list": [],
        }
    daily_profit_rel = results.resample("1d", on="close_date")["profit_ratio"].sum()
    daily_profit = results.resample("1d", on="close_date")["profit_abs"].sum().round(10)
    worst_rel = min(daily_profit_rel)
    best_rel = max(daily_profit_rel)
    worst = min(daily_profit)
    best = max(daily_profit)
    winning_days = sum(daily_profit > 0)
    draw_days = sum(daily_profit == 0)
    losing_days = sum(daily_profit < 0)
    daily_profit_list = [(str(idx.date()), val) for idx, val in daily_profit.items()]

    return {
        "backtest_best_day": best_rel,
        "backtest_worst_day": worst_rel,
        "backtest_best_day_abs": best,
        "backtest_worst_day_abs": worst,
        "winning_days": winning_days,
        "draw_days": draw_days,
        "losing_days": losing_days,
        "daily_profit": daily_profit_list,
    }


def generate_strategy_stats(
    pairlist: list[str],
    strategy: str,
    content: BacktestContentType,
    min_date: datetime,
    max_date: datetime,
    market_change: float,
    is_hyperopt: bool = False,
) -> dict[str, Any]:
    """
    :param pairlist: List of pairs to backtest
    :param strategy: Strategy name
    :param content: Backtest result data in the format:
                    {'results: results, 'config: config}}.
    :param min_date: Backtest start date
    :param max_date: Backtest end date
    :param market_change: float indicating the market change
    :return: Dictionary containing results per strategy and a strategy summary.
    """
    results: DataFrame = content["results"]
    if not isinstance(results, DataFrame):
        return {}
    config = content["config"]
    max_open_trades = min(config["max_open_trades"], len(pairlist))
    start_balance = get_dry_run_wallet(config)
    stake_currency = config["stake_currency"]

    pair_results = generate_pair_metrics(
        pairlist,
        stake_currency=stake_currency,
        starting_balance=start_balance,
        results=results,
        min_date=min_date,
        max_date=max_date,
        skip_nan=False,
    )

    enter_tag_stats = generate_tag_metrics(
        "enter_tag",
        starting_balance=start_balance,
        results=results,
        min_date=min_date,
        max_date=max_date,
        skip_nan=False,
    )
    exit_reason_stats = generate_tag_metrics(
        "exit_reason",
        starting_balance=start_balance,
        results=results,
        min_date=min_date,
        max_date=max_date,
        skip_nan=False,
    )
    mix_tag_stats = generate_tag_metrics(
        ["enter_tag", "exit_reason"],
        starting_balance=start_balance,
        results=results,
        min_date=min_date,
        max_date=max_date,
        skip_nan=False,
    )
    left_open_results = generate_pair_metrics(
        pairlist,
        stake_currency=stake_currency,
        starting_balance=start_balance,
        results=results.loc[results["exit_reason"] == "force_exit"],
        min_date=min_date,
        max_date=max_date,
        skip_nan=True,
    )

    daily_stats = generate_daily_stats(results)
    trade_stats = generate_trading_stats(results)

    periodic_breakdown = {}
    if not is_hyperopt:
        periodic_breakdown = {"periodic_breakdown": generate_all_periodic_breakdown_stats(results)}

    best_pair = (
        max(
            [pair for pair in pair_results if pair["key"] != "TOTAL"],
            key=lambda x: x["profit_total_abs"],
        )
        if len(pair_results) > 1
        else None
    )
    worst_pair = (
        min(
            [pair for pair in pair_results if pair["key"] != "TOTAL"],
            key=lambda x: x["profit_total_abs"],
        )
        if len(pair_results) > 1
        else None
    )
    winning_profit = results.loc[results["profit_abs"] > 0, "profit_abs"].sum()
    losing_profit = results.loc[results["profit_abs"] < 0, "profit_abs"].sum()
    profit_factor = winning_profit / abs(losing_profit) if losing_profit else 0.0

    expectancy, expectancy_ratio = calculate_expectancy(results)
    backtest_days = (max_date - min_date).days or 1
    trades_dict = results.to_dict(orient="records")
    strat_stats = {
        "trades": trades_dict,
        "locks": [lock.to_json() for lock in content["locks"]],
        "best_pair": best_pair,
        "worst_pair": worst_pair,
        "results_per_pair": pair_results,
        "results_per_enter_tag": enter_tag_stats,
        "exit_reason_summary": exit_reason_stats,
        "mix_tag_stats": mix_tag_stats,
        "left_open_trades": left_open_results,
        "total_trades": len(results),
        "trade_count_long": len(results.loc[~results["is_short"]]),
        "trade_count_short": len(results.loc[results["is_short"]]),
        "total_volume": calculate_trade_volume(trades_dict),
        "avg_stake_amount": results["stake_amount"].mean() if len(results) > 0 else 0,
        "profit_mean": results["profit_ratio"].mean() if len(results) > 0 else 0,
        "profit_median": results["profit_ratio"].median() if len(results) > 0 else 0,
        "profit_total": results["profit_abs"].sum() / start_balance,
        "profit_total_long": results.loc[~results["is_short"], "profit_abs"].sum() / start_balance,
        "profit_total_short": results.loc[results["is_short"], "profit_abs"].sum() / start_balance,
        "profit_total_abs": results["profit_abs"].sum(),
        "profit_total_long_abs": results.loc[~results["is_short"], "profit_abs"].sum(),
        "profit_total_short_abs": results.loc[results["is_short"], "profit_abs"].sum(),
        "cagr": calculate_cagr(backtest_days, start_balance, content["final_balance"]),
        "expectancy": expectancy,
        "expectancy_ratio": expectancy_ratio,
        "sortino": calculate_sortino(results, min_date, max_date, start_balance),
        "sharpe": calculate_sharpe(results, min_date, max_date, start_balance),
        "calmar": calculate_calmar(results, min_date, max_date, start_balance),
        "sqn": calculate_sqn(results, start_balance),
        "profit_factor": profit_factor,
        "backtest_start": min_date.strftime(DATETIME_PRINT_FORMAT),
        "backtest_start_ts": int(min_date.timestamp() * 1000),
        "backtest_end": max_date.strftime(DATETIME_PRINT_FORMAT),
        "backtest_end_ts": int(max_date.timestamp() * 1000),
        "backtest_days": backtest_days,
        "backtest_run_start_ts": content["backtest_start_time"],
        "backtest_run_end_ts": content["backtest_end_time"],
        "trades_per_day": round(len(results) / backtest_days, 2),
        "market_change": market_change,
        "pairlist": pairlist,
        "stake_amount": config["stake_amount"],
        "stake_currency": config["stake_currency"],
        "stake_currency_decimals": decimals_per_coin(config["stake_currency"]),
        "starting_balance": start_balance,
        "dry_run_wallet": start_balance,
        "final_balance": content["final_balance"],
        "rejected_signals": content["rejected_signals"],
        "timedout_entry_orders": content["timedout_entry_orders"],
        "timedout_exit_orders": content["timedout_exit_orders"],
        "canceled_trade_entries": content["canceled_trade_entries"],
        "canceled_entry_orders": content["canceled_entry_orders"],
        "replaced_entry_orders": content["replaced_entry_orders"],
        "max_open_trades": max_open_trades,
        "max_open_trades_setting": (
            config["max_open_trades"] if config["max_open_trades"] != float("inf") else -1
        ),
        "timeframe": config["timeframe"],
        "timeframe_detail": config.get("timeframe_detail", ""),
        "timerange": config.get("timerange", ""),
        "enable_protections": config.get("enable_protections", False),
        "strategy_name": strategy,
        "freqaimodel": config.get("freqaimodel", None),
        "freqai_identifier": config.get("freqai", {}).get("identifier", None),
        # Parameters relevant for backtesting
        "stoploss": config["stoploss"],
        "trailing_stop": config.get("trailing_stop", False),
        "trailing_stop_positive": config.get("trailing_stop_positive"),
        "trailing_stop_positive_offset": config.get("trailing_stop_positive_offset", 0.0),
        "trailing_only_offset_is_reached": config.get("trailing_only_offset_is_reached", False),
        "use_custom_stoploss": config.get("use_custom_stoploss", False),
        "minimal_roi": config["minimal_roi"],
        "use_exit_signal": config["use_exit_signal"],
        "exit_profit_only": config["exit_profit_only"],
        "exit_profit_offset": config["exit_profit_offset"],
        "ignore_roi_if_entry_signal": config["ignore_roi_if_entry_signal"],
        "trading_mode": config["trading_mode"],
        "margin_mode": config["margin_mode"],
        **periodic_breakdown,
        **daily_stats,
        **trade_stats,
    }

    try:
        drawdown = calculate_max_drawdown(
            results, value_col="profit_abs", starting_balance=start_balance
        )
        # max_relative_drawdown = Underwater
        underwater = calculate_max_drawdown(
            results, value_col="profit_abs", starting_balance=start_balance, relative=True
        )
        drawdown_duration = drawdown.low_date - drawdown.high_date

        strat_stats.update(
            {
                "max_drawdown_account": drawdown.relative_account_drawdown,
                "max_relative_drawdown": underwater.relative_account_drawdown,
                "max_drawdown_abs": drawdown.drawdown_abs,
                "drawdown_start": drawdown.high_date.strftime(DATETIME_PRINT_FORMAT),
                "drawdown_start_ts": drawdown.high_date.timestamp() * 1000,
                "drawdown_end": drawdown.low_date.strftime(DATETIME_PRINT_FORMAT),
                "drawdown_end_ts": drawdown.low_date.timestamp() * 1000,
                "drawdown_duration": drawdown_duration,
                "drawdown_duration_s": drawdown_duration.total_seconds(),
                "max_drawdown_low": drawdown.low_value,
                "max_drawdown_high": drawdown.high_value,
            }
        )

        csum_min, csum_max = calculate_csum(results, start_balance)
        strat_stats.update({"csum_min": csum_min, "csum_max": csum_max})

    except ValueError:
        strat_stats.update(
            {
                "max_drawdown_account": 0.0,
                "max_relative_drawdown": 0.0,
                "max_drawdown_abs": 0.0,
                "max_drawdown_low": 0.0,
                "max_drawdown_high": 0.0,
                "drawdown_start": datetime(1970, 1, 1, tzinfo=UTC),
                "drawdown_start_ts": 0,
                "drawdown_end": datetime(1970, 1, 1, tzinfo=UTC),
                "drawdown_end_ts": 0,
                "csum_min": 0,
                "csum_max": 0,
            }
        )

    return strat_stats


def generate_backtest_stats(
    btdata: dict[str, DataFrame],
    all_results: dict[str, BacktestContentType],
    min_date: datetime,
    max_date: datetime,
    notes: str | None = None,
) -> BacktestResultType:
    """
    :param btdata: Backtest data
    :param all_results: backtest result - dictionary in the form:
                     { Strategy: {'results: results, 'config: config}}.
    :param min_date: Backtest start date
    :param max_date: Backtest end date
    :return: Dictionary containing results per strategy and a strategy summary.
    """
    result: BacktestResultType = get_BacktestResultType_default()
    market_change = calculate_market_change(btdata, "close", min_date=min_date)
    metadata = {}
    pairlist = list(btdata.keys())
    for strategy, content in all_results.items():
        strat_stats = generate_strategy_stats(
            pairlist, strategy, content, min_date, max_date, market_change=market_change
        )
        metadata[strategy] = {
            "run_id": content["run_id"],
            "backtest_start_time": content["backtest_start_time"],
            "timeframe": content["config"]["timeframe"],
            "timeframe_detail": content["config"].get("timeframe_detail", None),
            "backtest_start_ts": int(min_date.timestamp()),
            "backtest_end_ts": int(max_date.timestamp()),
        }
        if notes:
            metadata[strategy]["notes"] = notes
        result["strategy"][strategy] = strat_stats

    strategy_results = generate_strategy_comparison(bt_stats=result["strategy"])

    result["metadata"] = metadata
    result["strategy_comparison"] = strategy_results

    return result


# ── Minimal stubs for backtest result storage / display ───────────────
import json as _json  # noqa: E402
from pathlib import Path as _Path  # noqa: E402

# Backtest result files get large (42 MB / 36k trades on a wide run) and the
# stdlib encoder is the bottleneck when writing them. orjson is a Rust
# (serde_json) encoder — measured 2.3x faster on that payload.
#
# Output is byte-identical to `json.dump(..., default=str, indent=2)`:
#   * OPT_INDENT_2 reproduces indent=2 exactly (orjson supports no other width);
#   * OPT_PASSTHROUGH_DATETIME forces datetimes through `default=str`, so they
#     keep the current "YYYY-MM-DD HH:MM:SS+00:00" form rather than orjson's
#     native RFC 3339 "…T…" — which would silently change every date in the
#     file and break existing readers.
try:  # pragma: no cover - optional dependency
    import orjson as _orjson

    _ORJSON_OPTS = _orjson.OPT_INDENT_2 | _orjson.OPT_PASSTHROUGH_DATETIME | _orjson.OPT_SERIALIZE_NUMPY
except ImportError:  # fall back to the stdlib encoder
    _orjson = None
    _ORJSON_OPTS = 0


def _dump_json(obj, filename: _Path, *, indent: bool = True) -> None:
    """Write `obj` to `filename` as JSON, using orjson when available."""
    if _orjson is not None:
        opts = _ORJSON_OPTS if indent else (_orjson.OPT_PASSTHROUGH_DATETIME | _orjson.OPT_SERIALIZE_NUMPY)
        with filename.open("wb") as fp:
            fp.write(_orjson.dumps(obj, default=str, option=opts))
    else:
        with filename.open("w") as fp:
            _json.dump(obj, fp, default=str, indent=2 if indent else None)


def show_backtest_results(config: dict, backtest_stats: dict) -> None:  # noqa: D401
    """Print a full backtest summary table to the terminal."""
    strategies = backtest_stats.get("strategy", {})
    if not strategies:
        logger.info("No backtest results to display.")
        return

    def _pct(v):
        try:
            return f"{float(v) * 100:.2f}%"
        except (TypeError, ValueError):
            return "n/a"

    def _f(v, fmt=".3f"):
        try:
            return format(float(v), fmt)
        except (TypeError, ValueError):
            return "n/a"

    def _rr(win_rate, pf):
        """Implied reward:risk = PF * (1-WR) / WR."""
        try:
            wr = float(win_rate)
            if wr <= 0 or wr >= 1:
                return "n/a"
            return f"{float(pf) * (1 - wr) / wr:.2f}"
        except (TypeError, ValueError, ZeroDivisionError):
            return "n/a"

    for name, st in strategies.items():
        trades     = st.get("total_trades", len(st.get("trades", [])))
        wins       = st.get("wins", 0)
        losses     = st.get("losses", 0)
        draws      = st.get("draws", 0)
        profit_abs = st.get("profit_total_abs", 0)
        profit_pct = st.get("profit_total", 0)
        win_rate   = st.get("winrate", wins / trades if trades else 0)
        avg_profit = st.get("profit_mean", 0)
        avg_dur    = st.get("holding_avg", "n/a")
        max_dd_abs = st.get("max_drawdown_abs", 0)
        max_dd_pct = st.get("max_relative_drawdown", st.get("max_drawdown_account", 0))
        profit_fac = st.get("profit_factor", 0)
        sharpe     = st.get("sharpe", 0)
        sortino    = st.get("sortino", 0)
        calmar     = st.get("calmar", 0)
        cagr       = st.get("cagr", 0)
        expectancy = st.get("expectancy", 0)
        exp_ratio  = st.get("expectancy_ratio", 0)
        sqn        = st.get("sqn", 0)
        long_pnl   = st.get("profit_total_long_abs", 0)
        short_pnl  = st.get("profit_total_short_abs", 0)
        long_cnt   = st.get("trade_count_long", 0)
        short_cnt  = st.get("trade_count_short", 0)
        best_day   = st.get("backtest_best_day_abs", 0)
        worst_day  = st.get("backtest_worst_day_abs", 0)
        win_days   = st.get("winning_days", 0)
        lose_days  = st.get("losing_days", 0)
        consec_win = st.get("max_consecutive_wins", 0)
        consec_los = st.get("max_consecutive_losses", 0)
        start      = st.get("backtest_start", "")
        end        = st.get("backtest_end", "")
        balance    = st.get("starting_balance", 0)
        final_bal  = st.get("final_balance", 0)
        tpd        = st.get("trades_per_day", 0)
        # Duration fields
        dur_avg      = st.get("holding_avg", "n/a")
        dur_win_avg  = st.get("winner_holding_avg", "n/a")
        dur_win_min  = st.get("winner_holding_min", "n/a")
        dur_win_max  = st.get("winner_holding_max", "n/a")
        dur_los_avg  = st.get("loser_holding_avg", "n/a")
        dur_los_min  = st.get("loser_holding_min", "n/a")
        dur_los_max  = st.get("loser_holding_max", "n/a")

        W = 72
        sep  = "=" * W
        sep2 = "-" * W

        print(sep)
        print(f"  Strategy : {name}   [{start} to {end}]")
        print(sep)

        # ── Overall Performance ───────────────────────────────────────
        print(f"  {'OVERALL PERFORMANCE':^{W-2}}")
        print(sep2)
        print(f"  {'Starting balance':<30} {balance:>12,.2f}    {'Final balance':<20} {final_bal:>12,.2f}")
        print(f"  {'Net P&L':<30} {profit_abs:>12,.2f}    {'Net P&L %':<20} {_pct(profit_pct):>12}")
        print(f"  {'CAGR':<30} {_pct(cagr):>12}    {'Trades/day':<20} {tpd:>12.2f}")
        print(f"  {'Long P&L':<30} {long_pnl:>10,.2f} ({long_cnt:>3} trades)    {'Short P&L':<14} {short_pnl:>10,.2f} ({short_cnt:>3} trades)")
        print(sep2)

        # ── Trade Durations ───────────────────────────────────────────
        print(f"  {'TRADE DURATIONS':^{W-2}}")
        print(sep2)
        print(f"  {'All trades avg':<30} {str(dur_avg):>12}")
        print(f"  {'Winners  min / avg / max':<30} {str(dur_win_min):>12}  /  {str(dur_win_avg):<12}  /  {str(dur_win_max)}")
        print(f"  {'Losers   min / avg / max':<30} {str(dur_los_min):>12}  /  {str(dur_los_avg):<12}  /  {str(dur_los_max)}")
        print(sep2)

        # ── Risk / Reward ─────────────────────────────────────────────
        print(f"  {'RISK / REWARD':^{W-2}}")
        print(sep2)
        print(f"  {'Trades':<30} {trades:>12}    {'Win rate':<20} {_pct(win_rate):>12}")
        print(f"  {'Wins / Losses / Draws':<30} {wins:>4} / {losses:>4} / {draws:<4}    {'Avg trade profit':<20} {_pct(avg_profit):>12}")
        print(f"  {'Profit factor':<30} {_f(profit_fac):>12}    {'Implied R:R':<20} {_rr(win_rate, profit_fac):>12}")
        print(f"  {'Expectancy (R multiple)':<30} {_f(exp_ratio):>12}    {'Expectancy (abs $)':<20} {expectancy:>12,.2f}")
        print(f"  {'SQN':<30} {_f(sqn):>12}    {'Consec W / L':<20} {consec_win:>5} / {consec_los:<5}")
        print(f"  {'Max drawdown':<30} {max_dd_abs:>12,.2f}    {'Max DD %':<20} {_pct(max_dd_pct):>12}")
        print(sep2)

        # ── Risk-Adjusted Returns ─────────────────────────────────────
        print(f"  {'RISK-ADJUSTED RETURNS':^{W-2}}")
        print(sep2)
        print(f"  {'Sharpe ratio':<30} {_f(sharpe):>12}    {'Sortino ratio':<20} {_f(sortino):>12}")
        print(f"  {'Calmar ratio':<30} {_f(calmar):>12}    {'CAGR / MaxDD':<20} {_f(float(cagr) / float(max_dd_pct) if max_dd_pct else 0):>12}")
        print(f"  {'Best day P&L':<30} {best_day:>12,.2f}    {'Worst day P&L':<20} {worst_day:>12,.2f}")
        print(f"  {'Winning days':<30} {win_days:>12}    {'Losing days':<20} {lose_days:>12}")
        print(sep)

        # ── Entry Signal Breakdown ────────────────────────────────────
        entry_tags = st.get("results_per_enter_tag", [])
        if entry_tags:
            print(f"  {'ENTRY SIGNAL BREAKDOWN':^{W-2}}")
            print(sep2)
            eh = f"  {'Tag':<14} {'T':>5} {'W':>4} {'L':>4} {'WinR':>6} {'PF':>6} {'R:R':>5} {'Exp(R)':>7} {'Sharpe':>7} {'CAGR':>8} {'AvgDur':>9} {'P&L':>12}"
            print(eh)
            print("  " + "-" * (len(eh) - 2))
            for tag in entry_tags:
                t  = tag.get("trades", 0)
                w  = tag.get("wins", 0)
                l  = tag.get("losses", 0)
                wr = tag.get("winrate", w/t if t else 0)
                pf = tag.get("profit_factor", 0)
                er = _f(tag.get("expectancy_ratio", 0), ".3f")
                sh = _f(tag.get("sharpe", 0), ".2f")
                cg = _pct(tag.get("cagr", 0))
                pl = tag.get("profit_total_abs", 0)
                da = str(tag.get("duration_avg", "n/a"))
                print(f"  {tag.get('key',''):<14} {t:>5} {w:>4} {l:>4} {_pct(wr):>6} {_f(pf,'.2f'):>6} {_rr(wr,pf):>5} {er:>7} {sh:>7} {cg:>8} {da:>9} {pl:>12,.2f}")
            print(sep)

        # ── Exit Reason Breakdown ─────────────────────────────────────
        exits = st.get("exit_reason_summary", [])
        if exits:
            print(f"  {'EXIT REASON BREAKDOWN':^{W-2}}")
            print(sep2)
            xh = f"  {'Exit reason':<26} {'Trades':>6} {'Avg%':>7} {'P&L':>14}"
            print(xh)
            print("  " + "-" * (len(xh) - 2))
            for ex in exits:
                t   = ex.get("trades", 0)
                avg = ex.get("profit_mean_pct", ex.get("profit_mean", 0) * 100)
                pl  = ex.get("profit_total_abs", 0)
                print(f"  {ex.get('key',''):<26} {t:>6} {avg:>6.2f}%  {pl:>14,.2f}")
            print(sep)

        # ── Per-Pair Breakdown ────────────────────────────────────────
        pairs = [p for p in st.get("results_per_pair", []) if p.get("trades", 0) > 0]
        if pairs:
            pairs_sorted = sorted(pairs, key=lambda p: p.get("profit_total_abs", 0), reverse=True)
            col_w = max(len(p.get("key", "")) for p in pairs_sorted)
            col_w = max(col_w, 4)
            print(f"  {'PER-PAIR BREAKDOWN':^{W-2}}")
            print(sep2)
            ph = f"  {'Pair':<{col_w}} {'T':>5} {'W':>4} {'L':>4} {'WinR':>6} {'PF':>6} {'R:R':>5} {'Exp(R)':>7} {'Sharpe':>7} {'CAGR':>8} {'AvgDur':>9} {'Avg%':>6} {'P&L':>12}"
            print(ph)
            print("  " + "-" * (len(ph) - 2))
            for p in pairs_sorted:
                t  = p.get("trades", 0)
                w  = p.get("wins", 0)
                l  = p.get("losses", 0)
                wr = p.get("winrate", w/t if t else 0)
                pf = p.get("profit_factor", 0)
                er = _f(p.get("expectancy_ratio", 0), ".3f")
                sh = _f(p.get("sharpe", 0), ".2f")
                cg = _pct(p.get("cagr", 0))
                ap = p.get("profit_mean_pct", p.get("profit_mean", 0) * 100)
                pl = p.get("profit_total_abs", 0)
                da = str(p.get("duration_avg", "n/a"))
                print(f"  {p.get('key',''):<{col_w}} {t:>5} {w:>4} {l:>4} {_pct(wr):>6} {_f(pf,'.2f'):>6} {_rr(wr,pf):>5} {er:>7} {sh:>7} {cg:>8} {da:>9} {ap:>5.1f}%  {pl:>12,.2f}")
            print(sep)

        # ── Full Metrics Summary (all in one place) ───────────────────
        print(f"  {'FULL METRICS SUMMARY':^{W-2}}")
        print(sep2)
        print(f"  {'Starting balance':<32} {balance:>14,.2f}")
        print(f"  {'Final balance':<32} {final_bal:>14,.2f}")
        print(f"  {'Net P&L':<32} {profit_abs:>14,.2f}  ({_pct(profit_pct)})")
        print(f"  {'CAGR':<32} {_pct(cagr):>14}")
        print(f"  {'Trades (total/day)':<32} {trades:>8}  /  {tpd:.2f}")
        print(f"  {'Long trades / P&L':<32} {long_cnt:>8}  /  {long_pnl:>10,.2f}")
        print(f"  {'Short trades / P&L':<32} {short_cnt:>8}  /  {short_pnl:>10,.2f}")
        print(f"  {'Win rate':<32} {_pct(win_rate):>14}")
        print(f"  {'Wins / Losses / Draws':<32} {wins:>4} / {losses:>4} / {draws}")
        print(f"  {'Avg trade profit':<32} {_pct(avg_profit):>14}")
        print(f"  {'Profit factor':<32} {_f(profit_fac):>14}")
        print(f"  {'Implied R:R':<32} {_rr(win_rate, profit_fac):>14}")
        print(f"  {'Expectancy (R multiple)':<32} {_f(exp_ratio):>14}")
        print(f"  {'Expectancy (abs $)':<32} {expectancy:>14,.2f}")
        print(f"  {'SQN':<32} {_f(sqn):>14}")
        print(f"  {'Sharpe ratio':<32} {_f(sharpe):>14}")
        print(f"  {'Sortino ratio':<32} {_f(sortino):>14}")
        print(f"  {'Calmar ratio':<32} {_f(calmar):>14}")
        print(f"  {'Max drawdown (abs)':<32} {max_dd_abs:>14,.2f}")
        print(f"  {'Max drawdown (%)':<32} {_pct(max_dd_pct):>14}")
        print(f"  {'Duration - all avg':<32} {str(dur_avg):>14}")
        print(f"  {'Duration - winners (min/avg/max)':<32} {str(dur_win_min)} / {str(dur_win_avg)} / {str(dur_win_max)}")
        print(f"  {'Duration - losers  (min/avg/max)':<32} {str(dur_los_min)} / {str(dur_los_avg)} / {str(dur_los_max)}")
        print(f"  {'Max consecutive wins':<32} {consec_win:>14}")
        print(f"  {'Max consecutive losses':<32} {consec_los:>14}")
        print(f"  {'Winning days / Losing days':<32} {win_days:>8}  /  {lose_days}")
        print(f"  {'Best day P&L':<32} {best_day:>14,.2f}")
        print(f"  {'Worst day P&L':<32} {worst_day:>14,.2f}")
        print(sep)

        logger.info(
            "BACKTEST [%s] trades=%d wins=%d losses=%d profit_abs=%.4f",
            name, trades, wins, losses, profit_abs,
        )


def store_backtest_results(
    config: dict,
    stats: dict,
    dtappendix: str,
    *,
    market_change_data=None,
    analysis_results: dict | None = None,
    strategy_files: dict | None = None,
) -> _Path:
    """Write backtest results JSON to ``user_data/backtest_results/``."""
    # Build a filesystem-safe strategy prefix from the strategy name(s) in the stats.
    _strategy_keys = list(stats.get("strategy", {}).keys())
    if _strategy_keys:
        import re as _re
        _strat_label = "_".join(_strategy_keys)
        # Replace any character that is not alphanumeric, dash, or underscore.
        _strat_label = _re.sub(r"[^\w\-]", "_", _strat_label)[:80]
        _file_stem = f"{_strat_label}-{dtappendix}"
    else:
        _file_stem = f"backtest-result-{dtappendix}"

    recordfilename = config.get("exportfilename")
    if recordfilename:
        export_dir = _Path(recordfilename)
        if export_dir.is_dir() or not export_dir.suffix:
            export_dir.mkdir(parents=True, exist_ok=True)
            filename = export_dir / f"{_file_stem}.json"
        else:
            export_dir.parent.mkdir(parents=True, exist_ok=True)
            filename = export_dir
    else:
        export_dir = _Path(config.get("user_data_dir", "user_data")) / "backtest_results"
        export_dir.mkdir(parents=True, exist_ok=True)
        filename = export_dir / f"{_file_stem}.json"

    _dump_json(stats, filename)

    # Write metadata sidecar.
    meta_filename = filename.parent / f"{filename.stem}.meta.json"
    _dump_json(stats.get("metadata", {}), meta_filename)

    # Write .last_result.json pointer.
    last_fn = filename.parent / ".last_result.json"
    _dump_json({"latest_backtest": filename.name}, last_fn, indent=False)

    logger.info("Backtest results written to %s", filename)
    return filename

