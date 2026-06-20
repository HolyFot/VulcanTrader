import logging

from VulcanTrader.exchange import Exchange
from VulcanTrader.exchange.exchange_types import TraderHas


logger = logging.getLogger(__name__)


class Coinex(Exchange):
    """
    CoinEx exchange class. Contains adjustments needed for VulcanTrader to work
    with this exchange.

    Please note that this exchange is not included in the list of exchanges
    officially supported by the VulcanTrader development team. So some features
    may still not work as expected.
    """

    trader_has: TraderHas = {
        "l2_limit_range": [5, 10, 20, 50],
        "tickers_have_bid_ask": False,
        "tickers_have_quoteVolume": False,
    }
