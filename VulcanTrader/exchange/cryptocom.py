"""Crypto.com exchange subclass"""

import logging

from VulcanTrader.exchange import Exchange
from VulcanTrader.exchange.exchange_types import TraderHas


logger = logging.getLogger(__name__)


class Cryptocom(Exchange):
    """Crypto.com exchange class.
    Contains adjustments needed for VulcanTrader to work with this exchange.
    """

    trader_has: TraderHas = {
        "ohlcv_candle_limit": 300,
    }
