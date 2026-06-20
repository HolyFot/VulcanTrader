import logging

from VulcanTrader.exchange import Exchange
from VulcanTrader.exchange.exchange_types import TraderHas


logger = logging.getLogger(__name__)


class Hitbtc(Exchange):
    """
    Hitbtc exchange class. Contains adjustments needed for VulcanTrader to work
    with this exchange.

    Please note that this exchange is not included in the list of exchanges
    officially supported by the VulcanTrader development team. So some features
    may still not work as expected.
    """

    trader_has: TraderHas = {
        "ohlcv_candle_limit": 1000,
    }
