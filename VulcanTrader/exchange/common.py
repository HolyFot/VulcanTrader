import asyncio
import logging
import time
from collections.abc import Callable
from functools import wraps
from typing import Any, TypeVar, cast, overload

from VulcanTrader.util.exceptions import DDosProtection, RetryableOrderError, TemporaryError
from VulcanTrader.util.logger import LoggingMixin


logger = logging.getLogger(__name__)
__logging_mixin = None


def _reset_logging_mixin():
    """
    Reset global logging mixin - used in tests only.
    """
    global __logging_mixin
    __logging_mixin = LoggingMixin(logger)


def _get_logging_mixin():
    # Logging-mixin to cache kucoin responses
    # Only to be used in retrier
    global __logging_mixin
    if not __logging_mixin:
        __logging_mixin = LoggingMixin(logger)
    return __logging_mixin


__rate_limit_logging_mixin = None
# Separate from _get_logging_mixin()'s kucoin-specific mixin (kept at its own
# 1-hour default) - this one is tuned much shorter, for the retrier's own
# rate-limit noise below: the backoff-delay line, the per-retry "returned
# exception... retrying" warning, and the terminal "...Giving up." warning.
# Under real congestion (a fleet of bots sharing one exchange IP) all three
# fire once per retry PER CONCURRENT call, so a single 429 storm produces
# dozens of near-identical lines within the same second - throttling by exact
# message text collapses repeats from different concurrent callers while
# still showing every distinct step of any one retry chain (the message text
# includes the changing count/delay, so 1->2->5->10 or "4 times"->"3 times"
# etc. never collide with each other).
_RATE_LIMIT_LOG_PERIOD_S = 30


def _get_rate_limit_logging_mixin():
    global __rate_limit_logging_mixin
    if not __rate_limit_logging_mixin:
        __rate_limit_logging_mixin = LoggingMixin(logger, refresh_period=_RATE_LIMIT_LOG_PERIOD_S)
    return __rate_limit_logging_mixin


# Maximum default retry count.
# Functions are always called RETRY_COUNT + 1 times (for the original call)
API_RETRY_COUNT = 4
API_FETCH_ORDER_RETRY_COUNT = 5

BAD_EXCHANGES = {
    "bitmex": "Various reasons.",
    "probit": "Requires additional, regular calls to `signIn()`.",
    "poloniex": "Does not provide fetch_order endpoint to fetch both open and closed orders.",
    "kucoinfutures": "Unsupported futures exchange.",
    "poloniexfutures": "Unsupported futures exchange.",
    "binancecoinm": "Unsupported futures exchange.",
}

MAP_EXCHANGE_CHILDCLASS = {
    "okex": "okx",
    "gateio": "gate",
    "huboi": "htx",
}

# Exchanges that are not in CCXT but have custom implementations
NON_CCXT_EXCHANGES = [
    "bitunix",
]

SUPPORTED_EXCHANGES = [
    "binance",
    "binanceus",
    "binanceusdm",
    "bingx",
    "bitmart",
    "bitget",
    "bitunix",
    "bybit",
    "coinbase",
    "gate",
    "htx",
    "hyperliquid",
    "kraken",
    "okx",
    "myokx",
]

# either the main, or replacement methods (array) is required
EXCHANGE_HAS_REQUIRED: dict[str, list[str]] = {
    # Required / private
    "fetchOrder": ["fetchOpenOrder", "fetchClosedOrder"],
    "fetchL2OrderBook": ["fetchTicker"],
    "cancelOrder": [],
    "createOrder": [],
    "fetchBalance": [],
    # Public endpoints
    "fetchOHLCV": [],
}

EXCHANGE_HAS_OPTIONAL = [
    # Private
    "fetchMyTrades",  # Trades for order - fee detection
    "createLimitOrder",
    "createMarketOrder",  # Either OR for orders
    # 'setLeverage',  # Margin/Futures trading
    # 'setMarginMode',  # Margin/Futures trading
    # 'fetchFundingHistory', # Futures trading
    # Public
    "fetchOrderBook",
    "fetchL2OrderBook",
    "fetchTicker",  # OR for pricing
    "fetchTickers",  # For volumepairlist?
    "fetchTrades",  # Downloading trades data
    # 'fetchFundingRateHistory',  # Futures trading
    # 'fetchPositions',  # Futures trading
    # 'fetchLeverageTiers',  # Futures initialization
    # 'fetchMarketLeverageTiers',  # Futures initialization
    # 'fetchOpenOrders', 'fetchClosedOrders',  # 'fetchOrders',  # Refinding balance...
    # "fetchPremiumIndexOHLCV",  # Futures additional data
    # "fetchMarkOHLCV",  # Futures additional data
    # "fetchIndexOHLCV",  # Futures additional data
    # ccxt.pro
    "watchOHLCV",
]


def calculate_backoff(retrycount, max_retries):
    """
    Calculate backoff
    """
    return (max_retries - retrycount) ** 2 + 1


def retrier_async(f):
    async def wrapper(*args, **kwargs):
        count = kwargs.pop("count", API_RETRY_COUNT)
        exchange_name = getattr(args[0], "name", "")
        kucoin = exchange_name == "KuCoin"  # Check if the exchange is KuCoin.
        try:
            return await f(*args, **kwargs)
        except TemporaryError as ex:
            msg = f'{f.__name__}() returned exception: "{ex}". '
            if count > 0:
                msg += f"Retrying still for {count} times."
                count -= 1
                kwargs["count"] = count
                if isinstance(ex, DDosProtection):
                    if kucoin and "429000" in str(ex):
                        # Temporary fix for 429000 error on kucoin
                        # see https://github.com/VulcanTrader/VulcanTrader/issues/5700 for details.
                        _get_logging_mixin().log_once(
                            f"Kucoin 429 error, avoid triggering DDosProtection backoff delay. "
                            f"{count} tries left before giving up",
                            logmethod=logger.warning,
                            key=f"{exchange_name}:{f.__name__}:kucoin429",
                        )
                        # Reset msg to avoid logging too many times.
                        msg = ""
                    else:
                        backoff_delay = calculate_backoff(count + 1, API_RETRY_COUNT)
                        # See _get_rate_limit_logging_mixin's docstring for
                        # why this (and the two warnings below) use log_once
                        # instead of a plain logger call. The `key=` here (and
                        # below) drops the changing count/delay from what's
                        # deduped on - otherwise every distinct retry step (and
                        # every concurrent caller currently sitting at a
                        # different step) escapes the throttle and each logs
                        # its own line, which is where the console spam comes
                        # from during a sustained rate-limit storm.
                        _get_rate_limit_logging_mixin().log_once(
                            f"Applying DDosProtection backoff delay: {backoff_delay}",
                            logmethod=logger.info,
                            key=f"{exchange_name}:{f.__name__}:backoff",
                        )
                        await asyncio.sleep(backoff_delay)
                if msg:
                    _get_rate_limit_logging_mixin().log_once(
                        msg,
                        logmethod=logger.warning,
                        key=f"{exchange_name}:{f.__name__}:retry:{type(ex).__name__}",
                    )
                return await wrapper(*args, **kwargs)
            else:
                _get_rate_limit_logging_mixin().log_once(
                    msg + "Giving up.",
                    logmethod=logger.warning,
                    key=f"{exchange_name}:{f.__name__}:giveup:{type(ex).__name__}",
                )
                raise ex

    return wrapper


F = TypeVar("F", bound=Callable[..., Any])


# Type shenanigans
@overload
def retrier(_func: F) -> F: ...


@overload
def retrier(_func: F, *, retries=API_RETRY_COUNT) -> F: ...


@overload
def retrier(*, retries=API_RETRY_COUNT) -> Callable[[F], F]: ...


def retrier(_func: F | None = None, *, retries=API_RETRY_COUNT):
    def decorator(f: F) -> F:
        @wraps(f)
        def wrapper(*args, **kwargs):
            count = kwargs.pop("count", retries)
            exchange_name = getattr(args[0], "name", "") if args else ""
            try:
                return f(*args, **kwargs)
            except (TemporaryError, RetryableOrderError) as ex:
                msg = f'{f.__name__}() returned exception: "{ex}". '
                if count > 0:
                    # See _get_rate_limit_logging_mixin's docstring (top of
                    # this module) for why these use log_once. `key=` drops the
                    # changing count/delay from the dedup key - otherwise every
                    # distinct retry step (and every concurrent caller sitting
                    # at a different step) escapes the throttle and logs its
                    # own line, which is where console spam during a sustained
                    # rate-limit storm comes from.
                    _get_rate_limit_logging_mixin().log_once(
                        msg + f"Retrying still for {count} times.",
                        logmethod=logger.warning,
                        key=f"{exchange_name}:{f.__name__}:retry:{type(ex).__name__}",
                    )
                    count -= 1
                    kwargs.update({"count": count})
                    if isinstance(ex, DDosProtection | RetryableOrderError):
                        # increasing backoff
                        backoff_delay = calculate_backoff(count + 1, retries)
                        _get_rate_limit_logging_mixin().log_once(
                            f"Applying DDosProtection backoff delay: {backoff_delay}",
                            logmethod=logger.info,
                            key=f"{exchange_name}:{f.__name__}:backoff",
                        )
                        time.sleep(backoff_delay)
                    return wrapper(*args, **kwargs)
                else:
                    _get_rate_limit_logging_mixin().log_once(
                        msg + "Giving up.",
                        logmethod=logger.warning,
                        key=f"{exchange_name}:{f.__name__}:giveup:{type(ex).__name__}",
                    )
                    raise ex

        return cast(F, wrapper)

    # Support both @retrier and @retrier(retries=2) syntax
    if _func is None:
        return decorator
    else:
        return decorator(_func)
