"""
PairList manager class
"""

import logging
import re
from functools import partial
from pathlib import Path

from cachetools import LRUCache, cached

from VulcanTrader.constants import Config, ListPairsWithTimeframes
from VulcanTrader.data.dataprovider import DataProvider
from VulcanTrader.enums import CandleType
from VulcanTrader.enums.runmode import RunMode
from VulcanTrader.util.exceptions import OperationalException
from VulcanTrader.exchange.exchange_types import Tickers
from VulcanTrader.util.logger import LoggingMixin
from VulcanTrader.pairlist.IPairList import IPairList, SupportsBacktesting
from VulcanTrader.pairlist.pairlist_helpers import expand_pairlist
from VulcanTrader.resolvers import PairListResolver
from VulcanTrader.util import FtTTLCache


logger = logging.getLogger(__name__)


class PairListManager(LoggingMixin):
    def __init__(self, exchange, config: Config, dataprovider: DataProvider | None = None) -> None:
        self._exchange = exchange
        self._config = config
        self._whitelist = self._config["exchange"].get("pair_whitelist")
        self._blacklist = self._config["exchange"].get("pair_blacklist", [])
        self._pairlist_handlers: list[IPairList] = []
        self._tickers_needed = False
        self._dataprovider: DataProvider | None = dataprovider
        for pairlist_handler_config in self._config.get("pairlists", []):
            pairlist_handler = PairListResolver.load_pairlist(
                pairlist_handler_config["method"],
                exchange=exchange,
                pairlistmanager=self,
                config=config,
                pairlistconfig=pairlist_handler_config,
                pairlist_pos=len(self._pairlist_handlers),
            )
            self._tickers_needed |= pairlist_handler.needstickers
            self._pairlist_handlers.append(pairlist_handler)

        if not self._pairlist_handlers:
            raise OperationalException("No Pairlist Handlers defined")

        if self._tickers_needed and not self._exchange.exchange_has("fetchTickers"):
            invalid = ". ".join([p.name for p in self._pairlist_handlers if p.needstickers])

            raise OperationalException(
                "Exchange does not support fetchTickers, therefore the following pairlists "
                "cannot be used. Please edit your config and restart the bot.\n"
                f"{invalid}."
            )

        self._check_backtest()
        self._not_expiring_cache: LRUCache = LRUCache(maxsize=1)

        refresh_period = config.get("pairlist_refresh_period", 3600)
        LoggingMixin.__init__(self, logger, refresh_period)

    def _discover_pairs_from_datadir(self) -> list[str]:
        """
        Scan the configured data directory for OHLCV feather files and derive pair names.

        Filename convention: {BASE}_{STAKE}-{TIMEFRAME}.feather
        For futures mode the settle currency (same as stake) is appended: BASE/STAKE:SETTLE
        """
        datadir: Path | None = self._config.get("datadir")
        if not datadir or not Path(datadir).is_dir():
            return []

        timeframe = self._config.get("timeframe", "15m")
        stake = self._config.get("stake_currency", "USDC")
        is_futures = self._config.get("trading_mode", "spot") in ("futures", "margin")

        pattern = re.compile(
            rf"^(.+)_{re.escape(stake)}-{re.escape(timeframe)}\.feather$",
            re.IGNORECASE,
        )
        pairs: list[str] = []
        try:
            for fname in sorted(Path(datadir).iterdir()):
                m = pattern.match(fname.name)
                if m:
                    base = m.group(1)
                    pairs.append(
                        f"{base}/{stake}:{stake}" if is_futures else f"{base}/{stake}"
                    )
        except OSError:
            pass
        return pairs

    def _check_backtest(self) -> None:
        if self._config["runmode"] not in (RunMode.BACKTEST, RunMode.HYPEROPT):
            return

        pairlist_errors: list[str] = []
        noaction_pairlists: list[str] = []
        biased_pairlists: list[str] = []
        for pairlist_handler in self._pairlist_handlers:
            if pairlist_handler.supports_backtesting == SupportsBacktesting.NO:
                pairlist_errors.append(pairlist_handler.name)
            if pairlist_handler.supports_backtesting == SupportsBacktesting.NO_ACTION:
                noaction_pairlists.append(pairlist_handler.name)
            if pairlist_handler.supports_backtesting == SupportsBacktesting.BIASED:
                biased_pairlists.append(pairlist_handler.name)

        if noaction_pairlists:
            logger.warning(
                f"Pairlist Handlers {', '.join(noaction_pairlists)} do not generate "
                "any changes during backtesting. While it's safe to leave them enabled, they will "
                "not behave like in dry/live modes. "
            )

        if biased_pairlists:
            logger.warning(
                f"Pairlist Handlers {', '.join(biased_pairlists)} will introduce a lookahead bias "
                "to your backtest results, as they use today's data - which inheritly suffers from "
                "'winner bias'."
            )
        if pairlist_errors:
            static_whitelist = list(
                self._config.get("exchange", {}).get("pair_whitelist") or []
            )
            if not static_whitelist:
                # No pairs configured — try to discover from available data files so that
                # backtesting can proceed even when the config is designed for live trading.
                static_whitelist = self._discover_pairs_from_datadir()
                if static_whitelist:
                    logger.warning(
                        f"Pairlist Handlers {', '.join(pairlist_errors)} do not support "
                        f"backtesting and exchange.pair_whitelist is empty. "
                        f"Auto-discovered {len(static_whitelist)} pairs from the data directory."
                    )
                else:
                    logger.warning(
                        f"Pairlist Handlers {', '.join(pairlist_errors)} do not support "
                        "backtesting and exchange.pair_whitelist is empty. "
                        "No data files found either — backtest will have no pairs."
                    )
            else:
                logger.warning(
                    f"Pairlist Handlers {', '.join(pairlist_errors)} do not support backtesting. "
                    f"Automatically falling back to StaticPairList with "
                    f"{len(static_whitelist)} pairs from exchange.pair_whitelist."
                )
            # Inject the whitelist so StaticPairList.gen_pairlist finds it.
            self._config.setdefault("exchange", {})["pair_whitelist"] = static_whitelist
            static_handler = PairListResolver.load_pairlist(
                "StaticPairList",
                exchange=self._exchange,
                pairlistmanager=self,
                config=self._config,
                pairlistconfig={"method": "StaticPairList"},
                pairlist_pos=0,
            )
            self._pairlist_handlers = [static_handler]
            self._tickers_needed = False

    @property
    def whitelist(self) -> list[str]:
        """The current whitelist"""
        return self._whitelist

    @property
    def blacklist(self) -> list[str]:
        """
        The current blacklist
        -> no need to overwrite in subclasses
        """
        return self._blacklist

    @property
    def expanded_blacklist(self) -> list[str]:
        """The expanded blacklist (including wildcard expansion)"""
        eblacklist = self._not_expiring_cache.get("eblacklist")

        if not eblacklist:
            eblacklist = expand_pairlist(self._blacklist, self._exchange.get_markets().keys())

            if self._config["runmode"] in (RunMode.BACKTEST, RunMode.HYPEROPT):
                self._not_expiring_cache["eblacklist"] = eblacklist.copy()

        return eblacklist

    @property
    def name_list(self) -> list[str]:
        """Get list of loaded Pairlist Handler names"""
        return [p.name for p in self._pairlist_handlers]

    def short_desc(self) -> list[dict]:
        """List of short_desc for each Pairlist Handler"""
        return [{p.name: p.short_desc()} for p in self._pairlist_handlers]

    @cached(FtTTLCache(maxsize=1, ttl=1800))
    def _get_cached_tickers(self) -> Tickers:
        return self._exchange.get_tickers()

    def refresh_pairlist(self, only_first: bool = False, pairs: list[str] | None = None) -> None:
        """
        Run pairlist through all configured Pairlist Handlers.

        :param only_first: If True, only run the first PairList handler (the generator)
            and skip all subsequent filters. Used during backtesting startup to ensure
            historic data is loaded for the complete universe of pairs that the
            generator can produce (even if later filters would reduce the list size).
            Prevents missing data when a filter returns a variable number of pairs
            across refresh cycles.
        :param pairs: Optional list of pairs to intersect with the generated pairlist.
            Only pairs present both in the generated list and this parameter are kept.
            Used in backtesting to filter out pairs with no available data.
        """
        # Tickers should be cached to avoid calling the exchange on each call.
        tickers: dict = {}
        if self._tickers_needed:
            tickers = self._get_cached_tickers()

        # Generate the pairlist with first Pairlist Handler in the chain
        pairlist = self._pairlist_handlers[0].gen_pairlist(tickers)

        # Optional intersection with an explicit list of pairs (used in backtesting)
        if pairs is not None:
            pairlist = [p for p in pairlist if p in pairs]

        if not only_first:
            # Process all Pairlist Handlers in the chain
            # except for the first one, which is the generator.
            for pairlist_handler in self._pairlist_handlers[1:]:
                pairlist = pairlist_handler.filter_pairlist(pairlist, tickers)

        # Validation against blacklist happens after the chain of Pairlist Handlers
        # to ensure blacklist is respected.
        pairlist = self.verify_blacklist(pairlist, logger.warning)

        # Safety net for backtest/hyperopt: if the chain returned an empty list but the
        # config whitelist is non-empty, fall back to the raw config whitelist so that
        # the backtesting engine can proceed and load available OHLCV data.
        if not pairlist and self._config["runmode"] in (RunMode.BACKTEST, RunMode.HYPEROPT):
            fallback = list(self._config.get("exchange", {}).get("pair_whitelist") or [])
            if fallback:
                logger.warning(
                    f"Pairlist is empty after the full handler chain. "
                    f"Using exchange.pair_whitelist directly ({len(fallback)} pairs)."
                )
                pairlist = fallback

        self.log_once(f"Whitelist with {len(pairlist)} pairs: {pairlist}", logger.info)

        self._whitelist = pairlist

    def verify_blacklist(self, pairlist: list[str], logmethod) -> list[str]:
        """
        Verify and remove items from pairlist - returning a filtered pairlist.
        Logs a warning or info depending on `aswarning`.
        Pairlist Handlers explicitly using this method shall use
        `logmethod=logger.info` to avoid spamming with warning messages
        :param pairlist: Pairlist to validate
        :param logmethod: Function that'll be called, `logger.info` or `logger.warning`.
        :return: pairlist - blacklisted pairs
        """
        if self._blacklist:
            try:
                blacklist = self.expanded_blacklist
            except ValueError as err:
                logger.error(f"Pair blacklist contains an invalid Wildcard: {err}")
                return []
            log_once = partial(self.log_once, logmethod=logmethod)
            for pair in pairlist.copy():
                if pair in blacklist:
                    log_once(f"Pair {pair} in your blacklist. Removing it from whitelist...")
                    pairlist.remove(pair)
        return pairlist

    def verify_whitelist(
        self, pairlist: list[str], logmethod, keep_invalid: bool = False
    ) -> list[str]:
        """
        Verify and remove items from pairlist - returning a filtered pairlist.
        Logs a warning or info depending on `aswarning`.
        Pairlist Handlers explicitly using this method shall use
        `logmethod=logger.info` to avoid spamming with warning messages
        :param pairlist: Pairlist to validate
        :param logmethod: Function that'll be called, `logger.info` or `logger.warning`
        :param keep_invalid: If sets to True, drops invalid pairs silently while expanding regexes.
        :return: pairlist - whitelisted pairs
        """
        try:
            whitelist = expand_pairlist(pairlist, self._exchange.get_markets().keys(), keep_invalid)
        except ValueError as err:
            logger.error(f"Pair whitelist contains an invalid Wildcard: {err}")
            return []
        return whitelist

    def create_pair_list(
        self, pairs: list[str], timeframe: str | None = None
    ) -> ListPairsWithTimeframes:
        """
        Create list of pair tuples with (pair, timeframe)
        """
        return [
            (
                pair,
                timeframe or self._config["timeframe"],
                self._config.get("candle_type_def", CandleType.SPOT),
            )
            for pair in pairs
        ]
