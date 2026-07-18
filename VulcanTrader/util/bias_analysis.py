import logging
import numbers
import shutil
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from pandas import DataFrame

from VulcanTrader.config.timerange import TimeRange
from VulcanTrader.data.history import get_timerange
from VulcanTrader.util.exceptions import ConfigurationError
from VulcanTrader.exchange import timeframe_to_minutes
from VulcanTrader.util.logger import (
    reduce_verbosity_for_bias_tester,
    restore_verbosity_for_bias_tester,
)

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from VulcanTrader.backtesting import Backtesting


logger = logging.getLogger(__name__)


class VarHolder:
    """Container for one analysis pass: data, indicators, results, timerange."""

    timerange: Any
    data: DataFrame
    indicators: dict[str, DataFrame]
    result: DataFrame
    compared: DataFrame
    from_dt: datetime
    to_dt: datetime
    compared_dt: datetime
    timeframe: str
    startup_candle: int


class BaseAnalysis:
    def __init__(self, config: dict[str, Any], strategy_obj: dict):
        self.failed_bias_check = True
        self.full_varHolder = VarHolder()
        self.exchange: Any | None = None
        self._fee = None

        # pull variables into the scope of this analysis instance
        self.local_config = deepcopy(config)
        self.local_config["strategy"] = strategy_obj["name"]
        self.strategy_obj = strategy_obj

    @staticmethod
    def dt_to_timestamp(dt: datetime) -> int:
        return int(dt.replace(tzinfo=UTC).timestamp())

    def fill_full_varholder(self):
        self.full_varHolder = VarHolder()

        parsed_timerange = TimeRange.parse_timerange(self.local_config["timerange"])

        if parsed_timerange.startdt is None:
            self.full_varHolder.from_dt = datetime.fromtimestamp(0, tz=UTC)
        else:
            self.full_varHolder.from_dt = parsed_timerange.startdt

        if parsed_timerange.stopdt is None:
            self.full_varHolder.to_dt = datetime.now(UTC)
        else:
            self.full_varHolder.to_dt = parsed_timerange.stopdt

        self.prepare_data(self.full_varHolder, self.local_config["pairs"])

    def prepare_data(self, varholder: "VarHolder", pairs_to_load: list[str]):
        raise NotImplementedError

    def start(self) -> None:
        self.fill_full_varholder()


def is_number(variable):
    return isinstance(variable, numbers.Number) and not isinstance(variable, bool)


class Analysis:
    def __init__(self) -> None:
        self.total_signals = 0
        self.false_entry_signals = 0
        self.false_exit_signals = 0
        self.false_indicators: list[str] = []
        self.has_bias = False


class LookaheadAnalysis(BaseAnalysis):
    def __init__(self, config: dict[str, Any], strategy_obj: dict):
        super().__init__(config, strategy_obj)

        self.entry_varHolders: list[VarHolder] = []
        self.exit_varHolders: list[VarHolder] = []

        self.current_analysis = Analysis()
        self.minimum_trade_amount = config["minimum_trade_amount"]
        self.targeted_trade_amount = config["targeted_trade_amount"]

    @staticmethod
    def get_result(backtesting: "Backtesting", processed: DataFrame):
        min_date, max_date = get_timerange(processed)

        result = backtesting.backtest(
            processed=deepcopy(processed), start_date=min_date, end_date=max_date
        )
        return result

    @staticmethod
    def report_signal(result: dict, column_name: str, checked_timestamp: datetime):
        df = result["results"]
        row_count = df[column_name].shape[0]

        if row_count == 0:
            return False
        else:
            df_cut = df[(df[column_name] == checked_timestamp)]
            if df_cut[column_name].shape[0] == 0:
                return False
            else:
                return True
        return False

    # analyzes two data frames with processed indicators and shows differences between them.
    def analyze_indicators(self, full_vars: VarHolder, cut_vars: VarHolder, current_pair: str):
        # extract dataframes
        cut_df: DataFrame = cut_vars.indicators[current_pair]
        full_df: DataFrame = full_vars.indicators[current_pair]

        # trim full_df to the same index and length as cut_df
        cut_full_df = full_df.loc[cut_df.index]
        compare_df = cut_full_df.compare(cut_df)

        if compare_df.shape[0] > 0:
            for col_name in compare_df:
                col_idx = compare_df.columns.get_loc(col_name)
                compare_df_row = compare_df.iloc[0]
                # compare_df now comprises tuples with [1] having either 'self' or 'other'
                if "other" in col_name[1]:
                    continue
                self_value = compare_df_row.iloc[col_idx]
                other_value = compare_df_row.iloc[col_idx + 1]

                # output differences
                if self_value != other_value:
                    if not self.current_analysis.false_indicators.__contains__(col_name[0]):
                        self.current_analysis.false_indicators.append(col_name[0])
                        logger.info(
                            f"=> found look ahead bias in column "
                            f"{col_name[0]}. "
                            f"{str(self_value)} != {str(other_value)}"
                        )

    def prepare_data(self, varholder: VarHolder, pairs_to_load: list[DataFrame]):
        if "freqai" in self.local_config and "identifier" in self.local_config["freqai"]:
            # purge previous data if the freqai model is defined
            # (to be sure nothing is carried over from older backtests)
            path_to_current_identifier = Path(
                f"{self.local_config['user_data_dir']}/models/"
                f"{self.local_config['freqai']['identifier']}"
            ).resolve()
            # remove folder and its contents
            if Path.exists(path_to_current_identifier):
                shutil.rmtree(path_to_current_identifier)

        prepare_data_config = deepcopy(self.local_config)
        prepare_data_config["timerange"] = (
            str(self.dt_to_timestamp(varholder.from_dt))
            + "-"
            + str(self.dt_to_timestamp(varholder.to_dt))
        )
        prepare_data_config["exchange"]["pair_whitelist"] = pairs_to_load

        if self._fee is not None:
            # Don't re-calculate fee per pair, as fee might differ per pair.
            prepare_data_config["fee"] = self._fee

        from VulcanTrader.backtesting import Backtesting

        backtesting = Backtesting(prepare_data_config, self.exchange)
        self.exchange = backtesting.exchange
        self.local_config["candle_type_def"] = prepare_data_config["candle_type_def"]
        self._fee = backtesting.fee
        backtesting._set_strategy(backtesting.strategylist[0])

        varholder.data, varholder.timerange = backtesting.load_bt_data()
        varholder.timeframe = backtesting.timeframe

        temp_indicators = backtesting.strategy.advise_all_indicators(varholder.data)
        filled_indicators = dict()
        for pair, dataframe in temp_indicators.items():
            filled_indicators[pair] = backtesting.strategy.trader_advise_signals(
                dataframe, {"pair": pair}
            )
        varholder.indicators = filled_indicators
        varholder.result = self.get_result(backtesting, varholder.indicators)

    def fill_entry_and_exit_varHolders(self, result_row):
        # entry_varHolder
        entry_varHolder = VarHolder()
        self.entry_varHolders.append(entry_varHolder)
        entry_varHolder.from_dt = self.full_varHolder.from_dt
        entry_varHolder.compared_dt = result_row["open_date"]
        # to_dt needs +1 candle since it won't buy on the last candle
        entry_varHolder.to_dt = result_row["open_date"] + timedelta(
            minutes=timeframe_to_minutes(self.full_varHolder.timeframe)
        )
        self.prepare_data(entry_varHolder, [result_row["pair"]])

        # exit_varHolder
        exit_varHolder = VarHolder()
        self.exit_varHolders.append(exit_varHolder)
        # to_dt needs +1 candle since it will always exit/force-exit trades on the last candle
        exit_varHolder.from_dt = self.full_varHolder.from_dt
        exit_varHolder.to_dt = result_row["close_date"] + timedelta(
            minutes=timeframe_to_minutes(self.full_varHolder.timeframe)
        )
        exit_varHolder.compared_dt = result_row["close_date"]
        self.prepare_data(exit_varHolder, [result_row["pair"]])

    # now we analyze a full trade of full_varholder and look for analyze its bias
    def analyze_row(self, idx: int, result_row):
        # if force-sold, ignore this signal since here it will unconditionally exit.
        if result_row.close_date == self.dt_to_timestamp(self.full_varHolder.to_dt):
            return

        # keep track of how many signals are processed at total
        self.current_analysis.total_signals += 1

        # fill entry_varHolder and exit_varHolder
        self.fill_entry_and_exit_varHolders(result_row)

        # this will trigger a logger-message
        entry_or_exit_biased: bool = False

        # register if buy signal is broken
        if not self.report_signal(
            self.entry_varHolders[idx].result, "open_date", self.entry_varHolders[idx].compared_dt
        ):
            self.current_analysis.false_entry_signals += 1
            entry_or_exit_biased = True

        # register if buy or sell signal is broken
        if not self.report_signal(
            self.exit_varHolders[idx].result, "close_date", self.exit_varHolders[idx].compared_dt
        ):
            self.current_analysis.false_exit_signals += 1
            entry_or_exit_biased = True

        if entry_or_exit_biased:
            logger.info(
                f"found lookahead-bias in trade "
                f"pair: {result_row['pair']}, "
                f"timerange:{result_row['open_date']} - {result_row['close_date']}, "
                f"idx: {idx}"
            )

        # check if the indicators themselves contain biased data
        self.analyze_indicators(self.full_varHolder, self.entry_varHolders[idx], result_row["pair"])
        self.analyze_indicators(self.full_varHolder, self.exit_varHolders[idx], result_row["pair"])

    def start(self) -> None:
        super().start()

        reduce_verbosity_for_bias_tester()

        # check if requirements have been met of full_varholder
        found_signals: int = self.full_varHolder.result["results"].shape[0] + 1
        if found_signals >= self.targeted_trade_amount:
            logger.info(
                f"Found {found_signals} trades, calculating {self.targeted_trade_amount} trades."
            )
        elif self.targeted_trade_amount >= found_signals >= self.minimum_trade_amount:
            logger.info(f"Only found {found_signals} trades. Calculating all available trades.")
        else:
            logger.info(
                f"found {found_signals} trades "
                f"which is less than minimum_trade_amount {self.minimum_trade_amount}. "
                f"Cancelling this backtest lookahead bias test."
            )
            return

        # now we loop through all signals
        # starting from the same datetime to avoid miss-reports of bias
        for idx, result_row in self.full_varHolder.result["results"].iterrows():
            if self.current_analysis.total_signals == self.targeted_trade_amount:
                logger.info(f"Found targeted trade amount = {self.targeted_trade_amount} signals.")
                break
            if found_signals < self.minimum_trade_amount:
                logger.info(
                    f"only found {found_signals} "
                    f"which is smaller than "
                    f"minimum trade amount = {self.minimum_trade_amount}. "
                    f"Exiting this lookahead-analysis"
                )
                return None
            if "force_exit" in result_row["exit_reason"]:
                logger.info(
                    f"found force-exit in pair: {result_row['pair']}, "
                    f"timerange:{result_row['open_date']}-{result_row['close_date']}, "
                    f"idx: {idx}, skipping this one to avoid a false-positive."
                )

                # just to keep the IDs of both full, entry and exit varholders the same
                # to achieve a better debugging experience
                self.entry_varHolders.append(VarHolder())
                self.exit_varHolders.append(VarHolder())
                continue

            self.analyze_row(idx, result_row)

        if len(self.entry_varHolders) < self.minimum_trade_amount:
            logger.info(
                f"only found {found_signals} after skipping forced exits "
                f"which is smaller than "
                f"minimum trade amount = {self.minimum_trade_amount}. "
                f"Exiting this lookahead-analysis"
            )

        # Restore verbosity, so it's not too quiet for the next strategy
        restore_verbosity_for_bias_tester()
        # check and report signals
        if self.current_analysis.total_signals < self.local_config["minimum_trade_amount"]:
            logger.info(
                f" -> {self.local_config['strategy']} : too few trades. "
                f"We only found {self.current_analysis.total_signals} trades. "
                f"Hint: Extend the timerange "
                f"to get at least {self.local_config['minimum_trade_amount']} "
                f"or lower the value of minimum_trade_amount."
            )
            self.failed_bias_check = True
        elif (
            self.current_analysis.false_entry_signals > 0
            or self.current_analysis.false_exit_signals > 0
            or len(self.current_analysis.false_indicators) > 0
        ):
            logger.info(f" => {self.local_config['strategy']} : bias detected!")
            self.current_analysis.has_bias = True
            self.failed_bias_check = False
        else:
            logger.info(self.local_config["strategy"] + ": no bias detected")
            self.failed_bias_check = False


class RecursiveAnalysis(BaseAnalysis):
    def __init__(self, config: dict[str, Any], strategy_obj: dict):
        self._startup_candle = list(
            map(int, config.get("startup_candle", [199, 399, 499, 999, 1999]))
        )

        super().__init__(config, strategy_obj)

        self.partial_varHolder_array: list[VarHolder] = []
        self.partial_varHolder_lookahead_array: list[VarHolder] = []

        self.dict_recursive: dict[str, Any] = dict()

        self.pair_to_used: str | None = None
        self._strat_scc: int | None = None

    # For recursive bias check
    # analyzes two data frames with processed indicators and shows differences between them.
    def analyze_indicators(self):
        pair_to_check = self.pair_to_used
        logger.info("Start checking for recursive bias")

        # check and report signals
        base_last_row = self.full_varHolder.indicators[pair_to_check].iloc[-1]

        for part in self.partial_varHolder_array:
            part_last_row = part.indicators[pair_to_check].iloc[-1]

            compare_df = base_last_row.compare(part_last_row)
            if compare_df.shape[0] > 0:
                # print(compare_df)
                for col_name, values in compare_df.items():
                    # print(col_name)
                    if "other" == col_name:
                        continue
                    indicators = values.index

                    for indicator in indicators:
                        if indicator not in self.dict_recursive:
                            self.dict_recursive[indicator] = {}

                        values_diff = compare_df.loc[indicator]
                        values_diff_self = values_diff.loc["self"]
                        values_diff_other = values_diff.loc["other"]

                        if (
                            values_diff_self
                            and values_diff_other
                            and is_number(values_diff_self)
                            and is_number(values_diff_other)
                        ):
                            diff = (values_diff_other - values_diff_self) / values_diff_self * 100
                            str_diff = f"{diff:.3f}%"
                        else:
                            str_diff = "NaN"
                        self.dict_recursive[indicator][part.startup_candle] = str_diff

            else:
                logger.info("No variance on indicator(s) found due to recursive formula.")
                break

    # For lookahead bias check
    # analyzes two data frames with processed indicators and shows differences between them.
    def analyze_indicators_lookahead(self):
        pair_to_check = self.pair_to_used
        logger.info("Start checking for lookahead bias on indicators only")

        part = self.partial_varHolder_lookahead_array[0]
        part_last_row = part.indicators[pair_to_check].iloc[-1]
        date_to_check = part_last_row["date"]
        index_to_get = self.full_varHolder.indicators[pair_to_check]["date"] == date_to_check
        base_row_check = self.full_varHolder.indicators[pair_to_check].loc[index_to_get].iloc[-1]

        check_time = part.to_dt.strftime("%Y-%m-%dT%H:%M:%S")

        logger.info(f"Check indicators at {check_time}")
        # logger.info(f"vs {part_timerange} with {part.startup_candle} startup candle")

        compare_df = base_row_check.compare(part_last_row)
        if compare_df.shape[0] > 0:
            # print(compare_df)
            for col_name, values in compare_df.items():
                # print(col_name)
                if "other" == col_name:
                    continue
                indicators = values.index

                for indicator in indicators:
                    logger.info(f"=> found lookahead in indicator {indicator}")
                    # logger.info("base value {:.5f}".format(values_diff_self))
                    # logger.info("part value {:.5f}".format(values_diff_other))

        else:
            logger.info("No lookahead bias on indicators found.")

    def prepare_data(self, varholder: VarHolder, pairs_to_load: list[DataFrame]):
        if "freqai" in self.local_config and "identifier" in self.local_config["freqai"]:
            # purge previous data if the freqai model is defined
            # (to be sure nothing is carried over from older backtests)
            path_to_current_identifier = Path(
                f"{self.local_config['user_data_dir']}/models/"
                f"{self.local_config['freqai']['identifier']}"
            ).resolve()
            # remove folder and its contents
            if Path.exists(path_to_current_identifier):
                shutil.rmtree(path_to_current_identifier)

        prepare_data_config = deepcopy(self.local_config)
        prepare_data_config["timerange"] = (
            str(self.dt_to_timestamp(varholder.from_dt))
            + "-"
            + str(self.dt_to_timestamp(varholder.to_dt))
        )
        prepare_data_config["exchange"]["pair_whitelist"] = pairs_to_load

        from VulcanTrader.backtesting import Backtesting

        backtesting = Backtesting(prepare_data_config, self.exchange)
        self.exchange = backtesting.exchange
        if self.pair_to_used is None:
            self.pair_to_used = backtesting.pairlists.whitelist[0]
            logger.info(
                f"Using pair {self.pair_to_used} only for recursive analysis. Replacing whitelist."
            )
        self.local_config["candle_type_def"] = prepare_data_config["candle_type_def"]
        backtesting.pairlists._whitelist = [self.pair_to_used]
        backtesting._set_strategy(backtesting.strategylist[0])

        strat = backtesting.strategy
        if self._strat_scc is None:
            self._strat_scc = strat.startup_candle_count

        if self._strat_scc < 1:
            raise ConfigurationError(
                f"The strategy defines invalid startup candle count of {self._strat_scc}. "
                f"This will lead to recursive issues on some indicators. "
                f"Please define a proper startup_candle_count in the strategy."
            )

        if self._strat_scc not in self._startup_candle:
            self._startup_candle.append(self._strat_scc)
        self._startup_candle.sort()

        varholder.data, varholder.timerange = backtesting.load_bt_data()
        varholder.timeframe = backtesting.timeframe

        varholder.indicators = backtesting.strategy.advise_all_indicators(varholder.data)

    def fill_partial_varholder(self, start_date, startup_candle):
        logger.info(f"Calculating indicators using startup candle of {startup_candle}.")
        partial_varHolder = VarHolder()

        partial_varHolder.from_dt = start_date
        partial_varHolder.to_dt = self.full_varHolder.to_dt
        partial_varHolder.startup_candle = startup_candle

        self.local_config["startup_candle_count"] = startup_candle

        self.prepare_data(partial_varHolder, self.local_config["pairs"])

        self.partial_varHolder_array.append(partial_varHolder)

    def fill_partial_varholder_lookahead(self, end_date):
        logger.info("Calculating indicators to test lookahead on indicators.")

        partial_varHolder = VarHolder()

        partial_varHolder.from_dt = self.full_varHolder.from_dt
        partial_varHolder.to_dt = end_date

        self.prepare_data(partial_varHolder, self.local_config["pairs"])

        self.partial_varHolder_lookahead_array.append(partial_varHolder)

    def start(self) -> None:
        super().start()

        reduce_verbosity_for_bias_tester()
        start_date_full = self.full_varHolder.from_dt
        end_date_full = self.full_varHolder.to_dt

        timeframe_minutes = timeframe_to_minutes(self.full_varHolder.timeframe)

        end_date_partial = start_date_full + timedelta(minutes=int(timeframe_minutes * 10))

        self.fill_partial_varholder_lookahead(end_date_partial)

        # restore_verbosity_for_bias_tester()

        start_date_partial = end_date_full - timedelta(minutes=int(timeframe_minutes))

        for startup_candle in self._startup_candle:
            self.fill_partial_varholder(start_date_partial, startup_candle)

        # Restore verbosity, so it's not too quiet for the next strategy
        restore_verbosity_for_bias_tester()

        self.analyze_indicators()
        self.analyze_indicators_lookahead()


# ============================================================================
#  Sub-functions used by the `lookahead-analysis` and `recursive-analysis`
#  CLI subcommands.  Mirrors freqtrade.optimize.analysis.{lookahead,recursive}_helpers.
# ============================================================================
import time
import pandas as _pd
from rich.text import Text as _RichText

from VulcanTrader.util.dry_run_wallet import get_dry_run_wallet
from VulcanTrader.util.exceptions import OperationalException
from VulcanTrader.util.rich_tables import print_rich_table


class LookaheadAnalysisSubFunctions:
    @staticmethod
    def text_table_lookahead_analysis_instances(
        config: dict[str, Any],
        lookahead_instances: list["LookaheadAnalysis"],
        caption: str | None = None,
    ):
        headers = [
            "filename",
            "strategy",
            "has_bias",
            "total_signals",
            "biased_entry_signals",
            "biased_exit_signals",
            "biased_indicators",
        ]
        data = []
        for inst in lookahead_instances:
            if config["minimum_trade_amount"] > inst.current_analysis.total_signals:
                data.append(
                    [
                        inst.strategy_obj["location"].parts[-1],
                        inst.strategy_obj["name"],
                        f"too few trades ({inst.current_analysis.total_signals}/"
                        f"{config['minimum_trade_amount']}). Test failed.",
                    ]
                )
            elif inst.failed_bias_check:
                data.append(
                    [
                        inst.strategy_obj["location"].parts[-1],
                        inst.strategy_obj["name"],
                        "error while checking",
                    ]
                )
            else:
                data.append(
                    [
                        inst.strategy_obj["location"].parts[-1],
                        inst.strategy_obj["name"],
                        _RichText("Yes", style="bold red")
                        if inst.current_analysis.has_bias
                        else _RichText("No", style="bold green"),
                        inst.current_analysis.total_signals,
                        inst.current_analysis.false_entry_signals,
                        inst.current_analysis.false_exit_signals,
                        ", ".join(inst.current_analysis.false_indicators),
                    ]
                )

        print_rich_table(
            data, headers, summary="Lookahead Analysis", table_kwargs={"caption": caption}
        )
        return data

    @staticmethod
    def export_to_csv(config: dict[str, Any], lookahead_analysis: list["LookaheadAnalysis"]):
        def add_or_update_row(df, row_data):
            if (
                (df["filename"] == row_data["filename"]) & (df["strategy"] == row_data["strategy"])
            ).any():
                pd_series = _pd.DataFrame([row_data])
                df.loc[
                    (df["filename"] == row_data["filename"])
                    & (df["strategy"] == row_data["strategy"])
                ] = pd_series
            else:
                df = _pd.concat([df, _pd.DataFrame([row_data], columns=df.columns)])
            return df

        if Path(config["lookahead_analysis_exportfilename"]).exists():
            csv_df = _pd.read_csv(config["lookahead_analysis_exportfilename"])
        else:
            csv_df = _pd.DataFrame(
                columns=[
                    "filename",
                    "strategy",
                    "has_bias",
                    "total_signals",
                    "biased_entry_signals",
                    "biased_exit_signals",
                    "biased_indicators",
                ],
                index=None,
            )

        for inst in lookahead_analysis:
            if (
                inst.current_analysis.total_signals > config["minimum_trade_amount"]
                and inst.failed_bias_check is not True
            ):
                new_row_data = {
                    "filename": inst.strategy_obj["location"].parts[-1],
                    "strategy": inst.strategy_obj["name"],
                    "has_bias": inst.current_analysis.has_bias,
                    "total_signals": int(inst.current_analysis.total_signals),
                    "biased_entry_signals": int(inst.current_analysis.false_entry_signals),
                    "biased_exit_signals": int(inst.current_analysis.false_exit_signals),
                    "biased_indicators": ",".join(inst.current_analysis.false_indicators),
                }
                csv_df = add_or_update_row(csv_df, new_row_data)

        csv_df["total_signals"] = csv_df["total_signals"].astype("int64").fillna(0)
        csv_df["biased_entry_signals"] = csv_df["biased_entry_signals"].astype("int64").fillna(0)
        csv_df["biased_exit_signals"] = csv_df["biased_exit_signals"].astype("int64").fillna(0)

        logger.info(f"saving {config['lookahead_analysis_exportfilename']}")
        csv_df.to_csv(config["lookahead_analysis_exportfilename"], index=False)

    @staticmethod
    def calculate_config_overrides(config: dict[str, Any]) -> dict[str, Any]:
        if config.get("enable_protections", False):
            config["enable_protections"] = False
            logger.info(
                "Protections were enabled. Disabling them now since they can produce "
                "false positives."
            )
        if not config.get("lookahead_allow_limit_orders", False):
            logger.info("Forced order_types to market orders.")
            config["order_types"] = {
                "entry": "market",
                "exit": "market",
                "stoploss": "market",
                "stoploss_on_exchange": False,
            }

        if config.get("targeted_trade_amount", 20) < config.get("minimum_trade_amount", 10):
            raise OperationalException(
                "Targeted trade amount can't be smaller than minimum trade amount."
            )
        config["max_open_trades"] = -1
        logger.info("Forced max_open_trades to -1 (same amount as there are pairs)")

        min_dry_run_wallet = 1_000_000_000
        if get_dry_run_wallet(config) < min_dry_run_wallet:
            logger.info(
                "Dry run wallet was not set to 1 billion, pushing it up there to avoid "
                "false positives."
            )
            config["dry_run_wallet"] = min_dry_run_wallet

        if "timerange" not in config:
            raise OperationalException(
                "Please set a timerange. Usually a few months are enough depending on "
                "your needs and strategy."
            )
        logger.info("fixing stake_amount to 10k")
        config["stake_amount"] = 10000

        if config.get("backtest_cache") is None:
            config["backtest_cache"] = "none"
        elif config["backtest_cache"] != "none":
            logger.info(
                f"backtest_cache = {config['backtest_cache']} detected. "
                f"Inside lookahead-analysis it is enforced to be 'none'. Changed it to 'none'."
            )
            config["backtest_cache"] = "none"
        return config

    @staticmethod
    def initialize_single_lookahead_analysis(config: dict[str, Any], strategy_obj: dict[str, Any]):
        logger.info(f"Bias test of {Path(strategy_obj['location']).name} started.")
        start = time.perf_counter()
        current_instance = LookaheadAnalysis(config, strategy_obj)
        current_instance.start()
        elapsed = time.perf_counter() - start
        logger.info(
            f"Checking look ahead bias via backtests of "
            f"{Path(strategy_obj['location']).name} took {elapsed:.0f} seconds."
        )
        return current_instance

    @staticmethod
    def start(config: dict[str, Any]):
        from VulcanTrader.resolvers.strategy_resolver import StrategyResolver

        config = LookaheadAnalysisSubFunctions.calculate_config_overrides(config)

        strategy_objs = StrategyResolver.search_all_objects(
            config, enum_failed=False, recursive=config.get("recursive_strategy_search", True)
        )

        lookaheadAnalysis_instances = []
        if not (strategy_list := config.get("strategy_list", [])):
            if config.get("strategy") is None:
                raise OperationalException(
                    "No Strategy specified. Please specify a strategy via --strategy or "
                    "--strategy-list"
                )
            strategy_list = [config["strategy"]]

        for strat in strategy_list:
            for strategy_obj in strategy_objs:
                if strategy_obj["name"] == strat and strategy_obj not in strategy_list:
                    lookaheadAnalysis_instances.append(
                        LookaheadAnalysisSubFunctions.initialize_single_lookahead_analysis(
                            config, strategy_obj
                        )
                    )
                    break

        if lookaheadAnalysis_instances:
            caption: str | None = None
            if any(
                any(
                    indicator.startswith("&")
                    for indicator in inst.current_analysis.false_indicators
                )
                for inst in lookaheadAnalysis_instances
            ):
                caption = (
                    "Any indicators in 'biased_indicators' which are used within "
                    "set_freqai_targets() can be ignored."
                )
            LookaheadAnalysisSubFunctions.text_table_lookahead_analysis_instances(
                config, lookaheadAnalysis_instances, caption=caption
            )
            if config.get("lookahead_analysis_exportfilename") is not None:
                LookaheadAnalysisSubFunctions.export_to_csv(config, lookaheadAnalysis_instances)
        else:
            logger.error(
                "There were no strategies specified neither through --strategy nor through "
                "--strategy-list, or timeframe was not specified."
            )


class RecursiveAnalysisSubFunctions:
    @staticmethod
    def text_table_recursive_analysis_instances(recursive_instances: list["RecursiveAnalysis"]):
        startups = recursive_instances[0]._startup_candle
        strat_scc = getattr(recursive_instances[0], "_strat_scc", 0) or 0
        headers = ["Indicators"]
        for candle in startups:
            if candle == strat_scc:
                headers.append(f"{candle} (from strategy)")
            else:
                headers.append(str(candle))

        data = []
        for inst in recursive_instances:
            if len(inst.dict_recursive) > 0:
                for indicator, values in inst.dict_recursive.items():
                    temp_data = [indicator]
                    for candle in startups:
                        temp_data.append(values.get(int(candle), "-"))
                    data.append(temp_data)

        if len(data) > 0:
            print_rich_table(data, headers, summary="Recursive Analysis")
            return data

        return data

    @staticmethod
    def calculate_config_overrides(config: dict[str, Any]) -> dict[str, Any]:
        if "timerange" not in config:
            raise OperationalException(
                "Please set a timerange. A timerange of 5000 candles is enough for "
                "recursive analysis."
            )
        if config.get("backtest_cache") is None:
            config["backtest_cache"] = "none"
        elif config["backtest_cache"] != "none":
            logger.info(
                f"backtest_cache = {config['backtest_cache']} detected. "
                f"Inside recursive-analysis it is enforced to be 'none'. Changed it to 'none'."
            )
            config["backtest_cache"] = "none"
        return config

    @staticmethod
    def initialize_single_recursive_analysis(config: dict[str, Any], strategy_obj: dict[str, Any]):
        logger.info(f"Recursive test of {Path(strategy_obj['location']).name} started.")
        start = time.perf_counter()
        current_instance = RecursiveAnalysis(config, strategy_obj)
        current_instance.start()
        elapsed = time.perf_counter() - start
        logger.info(
            f"Checking recursive and indicator-only lookahead bias of indicators of "
            f"{Path(strategy_obj['location']).name} took {elapsed:.0f} seconds."
        )
        return current_instance

    @staticmethod
    def start(config: dict[str, Any]):
        from VulcanTrader.resolvers.strategy_resolver import StrategyResolver

        config = RecursiveAnalysisSubFunctions.calculate_config_overrides(config)

        strategy_objs = StrategyResolver.search_all_objects(
            config, enum_failed=False, recursive=config.get("recursive_strategy_search", True)
        )

        RecursiveAnalysis_instances = []
        if not (strategy_list := config.get("strategy_list", [])):
            if config.get("strategy") is None:
                raise OperationalException(
                    "No Strategy specified. Please specify a strategy via --strategy"
                )
            strategy_list = [config["strategy"]]

        for strat in strategy_list:
            for strategy_obj in strategy_objs:
                if strategy_obj["name"] == strat and strategy_obj not in strategy_list:
                    RecursiveAnalysis_instances.append(
                        RecursiveAnalysisSubFunctions.initialize_single_recursive_analysis(
                            config, strategy_obj
                        )
                    )
                    break

        if RecursiveAnalysis_instances:
            RecursiveAnalysisSubFunctions.text_table_recursive_analysis_instances(
                RecursiveAnalysis_instances
            )
        else:
            logger.error(
                "There was no strategy specified through --strategy or timeframe was not specified."
            )