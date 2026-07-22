"""Coinbase (Advanced Trade) exchange subclass"""

import logging

from VulcanTrader.constants import BuySell
from VulcanTrader.enums import MarginMode, TradingMode
from VulcanTrader.exchange import Exchange
from VulcanTrader.exchange.exchange_types import TraderHas


logger = logging.getLogger(__name__)


class Coinbase(Exchange):
    """Coinbase exchange class (ccxt id ``coinbase`` — the Advanced Trade API).

    Contains adjustments needed for VulcanTrader to work with this exchange:

    * Candles: max 300 per request; granularities are limited to
      1m/5m/15m/30m/1h/2h/6h/1d (notably NO 4h) — strategies must use one of
      those timeframes.
    * Market buys are priced in quote currency; ccxt sets
      ``createMarketBuyOrderRequiresPrice`` so the base class already passes a
      price and ccxt converts amount → cost.
    * Stoploss on exchange: Advanced Trade only offers stop-LIMIT orders
      (``stop_limit_stop_limit_gtc``), no stop-market. The stop price is sent
      via ``stopPrice`` (ccxt's trigger branch) so our limit rate is honoured;
      ``stop_direction`` must be supplied explicitly because ccxt's default in
      that branch assumes take-profit semantics (see ``_get_stop_params``).
    * Public market-trades endpoint returns recent trades only — no paginated
      trade history download.
    """

    trader_has: TraderHas = {
        "stoploss_on_exchange": True,
        "stoploss_order_types": {"limit": "limit"},  # stop-limit only, no stop-market
        "stop_price_param": "stopPrice",
        "stop_price_prop": "triggerPrice",
        "order_time_in_force": ["GTC", "IOC", "FOK", "PO"],
        "ohlcv_candle_limit": 300,
        # fetch_tickers (products endpoint) has volume + percentage but no bid/ask
        # -> SpreadFilter is unavailable; VolumePairList/PercentChangePairList work.
        "tickers_have_bid_ask": False,
    }

    _supported_trading_mode_margin_pairs: list[tuple[TradingMode, MarginMode]] = [
        (TradingMode.SPOT, MarginMode.NONE),
    ]

    def _get_stop_params(self, side: BuySell, ordertype: str, stop_price: float) -> dict:
        """Add the mandatory ``stop_direction``.

        ccxt's trigger branch defaults sell → STOP_UP / buy → STOP_DOWN, which is
        take-profit direction. A stoploss triggers the other way: exiting a long
        (sell) must fire when price falls (STOP_DOWN), and vice versa.
        """
        params = super()._get_stop_params(side=side, ordertype=ordertype, stop_price=stop_price)
        params["stop_direction"] = (
            "STOP_DIRECTION_STOP_DOWN" if side == "sell" else "STOP_DIRECTION_STOP_UP"
        )
        return params
