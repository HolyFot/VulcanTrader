"""Bitmart exchange subclass"""

import logging

from VulcanTrader.exchange import Exchange
from VulcanTrader.exchange.exchange_types import TraderHas


logger = logging.getLogger(__name__)


class Bitmart(Exchange):
    """
    Bitmart exchange class. Contains adjustments needed for VulcanTrader to work
    with this exchange.
    """

    trader_has: TraderHas = {
        "stoploss_on_exchange": False,  # Bitmart API does not support stoploss orders
        "ohlcv_candle_limit": 200,
        "trades_has_history": False,  # Endpoint doesn't seem to support pagination
    }
