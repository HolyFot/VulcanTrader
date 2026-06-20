"""Hyperliquid exchange subclass"""

import asyncio
import logging
from copy import deepcopy
from datetime import datetime
from typing import Any

import ccxt

from VulcanTrader.constants import BuySell
from VulcanTrader.enums import CandleType, MarginMode, TradingMode
from VulcanTrader.util.exceptions import ExchangeError, OperationalException, TemporaryError
from VulcanTrader.exchange import Exchange
from VulcanTrader.exchange.exchange_types import CcxtOrder, OHLCVResponse, TraderHas
from VulcanTrader.util.datetime_helpers import dt_from_ts, dt_ts
from VulcanTrader.exchange.common import retrier


logger = logging.getLogger(__name__)


class Hyperliquid(Exchange):
    """Hyperliquid exchange class.
    Contains adjustments needed for VulcanTrader to work with this exchange.
    """

    trader_has: TraderHas = {
        "ohlcv_has_history": True,
        "l2_limit_range": [20],
        "trades_has_history": False,
        "tickers_have_bid_ask": False,
        "stoploss_on_exchange": False,
        "exchange_has_overrides": {"fetchTrades": False},
        "marketOrderRequiresPrice": True,
        "download_data_parallel_quick": False,
        "ws_enabled": True,
    }
    trader_has_futures: TraderHas = {
        "stoploss_on_exchange": True,
        "stoploss_order_types": {"limit": "limit"},
        "stoploss_blocks_assets": False,
        "stop_price_prop": "stopPrice",
        "funding_fee_candle_limit": 500,
        "uses_leverage_tiers": False,
        "mark_ohlcv_price": "futures",
        "mark_ohlcv_timeframe": "8h",
    }

    _supported_trading_mode_margin_pairs: list[tuple[TradingMode, MarginMode]] = [
        (TradingMode.SPOT, MarginMode.NONE),
        (TradingMode.FUTURES, MarginMode.ISOLATED),
        (TradingMode.FUTURES, MarginMode.CROSS),
    ]

    @property
    def _ccxt_config(self) -> dict:
        # ccxt Hyperliquid defaults to swap
        config = {}
        if self.trading_mode == TradingMode.SPOT:
            config.update({"options": {"defaultType": "spot"}})
        config.update(super()._ccxt_config)
        return config

    def reload_markets(self, force: bool = False, *, load_leverage_tiers: bool = True) -> None:
        """
        Override to reclassify hip3 spot markets as swap markets after loading.
        Hyperliquid hip3 markets (stocks, commodities, forex) are perpetual contracts
        but ccxt classifies them as spot. We reclassify them so they work in futures mode.
        """
        super().reload_markets(force, load_leverage_tiers=load_leverage_tiers)
        if self.trading_mode == TradingMode.FUTURES:
            self._reclassify_hip3_markets()
            self._fix_inactive_markets()

    def _reclassify_hip3_markets(self) -> None:
        """
        Hyperliquid hip3 markets are perpetual contracts for stocks, commodities,
        and forex, but ccxt classifies them as spot markets. This method creates
        swap-type market entries so they work with futures trading mode.

        Hip3 markets are identified by their id starting with '@'.
        """
        new_markets = {}
        for symbol, market in list(self._markets.items()):
            is_hip3 = (
                market.get("type") == "spot"
                and str(market.get("id", "")).startswith("@")
            )
            if is_hip3:
                swap_symbol = f"{market['base']}/{market['quote']}:{market['quote']}"
                if swap_symbol not in self._markets:
                    swap_market = deepcopy(market)
                    swap_market.update({
                        "symbol": swap_symbol,
                        "type": "swap",
                        "spot": False,
                        "swap": True,
                        "contract": True,
                        "linear": True,
                        "inverse": False,
                        "subType": "linear",
                        "contractSize": 1.0,
                        "settle": market["quote"],
                        "settleId": market.get("quoteId", market["quote"]),
                    })
                    # Set default leverage limits for hip3 (conservative)
                    if swap_market["limits"]["leverage"]["max"] is None:
                        swap_market["limits"]["leverage"]["max"] = 5
                    if swap_market["limits"]["leverage"]["min"] is None:
                        swap_market["limits"]["leverage"]["min"] = 1
                    new_markets[swap_symbol] = swap_market
                    logger.debug(f"Reclassified hip3 market {symbol} -> {swap_symbol}")

        if new_markets:
            self._markets.update(new_markets)
            logger.info(f"Added {len(new_markets)} hip3 markets as futures pairs.")

    def _fix_inactive_markets(self) -> None:
        """
        Hyperliquid protocol-wrapped markets (ABCD-, FLX-, KM-, CASH-, XYZ-, etc.)
        are sometimes reported as active=False by ccxt even though they are tradable.
        Force them active if they are swap markets with valid data.
        """
        fixed = 0
        for symbol, market in self._markets.items():
            if (
                market.get("type") == "swap"
                and market.get("active") is False
                and any(symbol.startswith(p) for p in ("ABCD-", "FLX-", "KM-", "CASH-", "XYZ-", "VNTL-"))
            ):
                market["active"] = True
                fixed += 1
        if fixed:
            logger.info(f"Activated {fixed} protocol-wrapped markets marked inactive by ccxt.")

    def market_is_tradable(self, market: dict[str, Any]) -> bool:
        parent_check = super().market_is_tradable(market)

        # Exclude markets with ":" in base (old-format hip3 markers from ccxt)
        return parent_check and ":" not in market["base"]

    def get_max_leverage(self, pair: str, stake_amount: float | None) -> float:
        # There are no leverage tiers
        if self.trading_mode == TradingMode.FUTURES:
            max_lev = self.markets[pair]["limits"]["leverage"]["max"]
            return max_lev if max_lev is not None else 5.0
        else:
            return 1.0

    def _lev_prep(self, pair: str, leverage: float, side: BuySell, accept_fail: bool = False):
        if self.trading_mode != TradingMode.SPOT:
            # Hyperliquid expects leverage to be an int
            leverage = int(leverage)
            # Hyperliquid needs the parameter leverage.
            # Don't use _set_leverage(), as this sets margin back to cross
            self.set_margin_mode(pair, self.margin_mode, params={"leverage": leverage})

    def dry_run_liquidation_price(
        self,
        pair: str,
        open_rate: float,  # Entry price of position
        is_short: bool,
        amount: float,
        stake_amount: float,
        leverage: float,
        wallet_balance: float,  # Or margin balance
        open_trades: list,
    ) -> float | None:
        """
        Optimized
        Docs: https://hyperliquid.gitbook.io/hyperliquid-docs/trading/liquidations
        Below can be done in fewer lines of code, but like this it matches the documentation.

        Tested with 196 unique ccxt fetch_positions() position outputs
        - Only first output per position where pnl=0.0
        - Compare against returned liquidation price
        Positions: 197 Average deviation: 0.00028980% Max deviation: 0.01309453%
        Positions info:
        {'leverage': {1.0: 23, 2.0: 155, 3.0: 8, 4.0: 7, 5.0: 4},
        'side': {'long': 133, 'short': 64},
        'symbol': {'BTC/USDC:USDC': 81,
                   'DOGE/USDC:USDC': 20,
                   'ETH/USDC:USDC': 53,
                   'SOL/USDC:USDC': 43}}
        """
        # Defining/renaming variables to match the documentation
        position_size = amount
        price = open_rate
        position_value = price * position_size
        max_leverage = self.markets[pair]["limits"]["leverage"]["max"]

        # Docs: The maintenance margin is half of the initial margin at max leverage,
        #       which varies from 3-50x. In other words, the maintenance margin is between 1%
        #       (for 50x max leverage assets) and 16.7% (for 3x max leverage assets)
        #       depending on the asset
        # The key thing here is 'Half of the initial margin at max leverage'.
        # A bit ambiguous, but this interpretation leads to accurate results:
        #       1. Start from the position value
        #       2. Assume max leverage, calculate the initial margin by dividing the position value
        #          by the max leverage
        #       3. Divide this by 2
        maintenance_margin_required = position_value / max_leverage / 2

        if self.margin_mode == MarginMode.ISOLATED:
            # Docs: margin_available (isolated) = isolated_margin - maintenance_margin_required
            margin_available = stake_amount - maintenance_margin_required
        elif self.margin_mode == MarginMode.CROSS:
            # Docs: margin_available (cross) = account_value - maintenance_margin_required
            margin_available = wallet_balance - maintenance_margin_required
        else:
            raise OperationalException("Unsupported margin mode for liquidation price calculation")

        # Docs: The maintenance margin is half of the initial margin at max leverage
        # The docs don't explicitly specify maintenance leverage, but this works.
        # Double because of the statement 'half of the initial margin at max leverage'
        maintenance_leverage = max_leverage * 2

        # Docs: l = 1 / MAINTENANCE_LEVERAGE (Using 'll' to comply with PEP8: E741)
        ll = 1 / maintenance_leverage

        # Docs: side = 1 for long and -1 for short
        side = -1 if is_short else 1

        # Docs: liq_price = price - side * margin_available / position_size / (1 - l * side)
        liq_price = price - side * margin_available / position_size / (1 - ll * side)

        if self.trading_mode == TradingMode.FUTURES:
            return liq_price
        else:
            raise OperationalException(
                "VulcanTrader only supports isolated futures for leverage trading"
            )

    def get_funding_fees(
        self, pair: str, amount: float, is_short: bool, open_date: datetime
    ) -> float:
        """
        Fetch funding fees, either from the exchange (live) or calculates them
        based on funding rate/mark price history
        :param pair: The quote/base pair of the trade
        :param is_short: trade direction
        :param amount: Trade amount
        :param open_date: Open date of the trade
        :return: funding fee since open_date
        :raises: ExchangeError if something goes wrong.
        """
        # Hyperliquid does not have fetchFundingHistory
        if self.trading_mode == TradingMode.FUTURES:
            try:
                return self._fetch_and_calculate_funding_fees(pair, amount, is_short, open_date)
            except ExchangeError:
                logger.warning(f"Could not update funding fees for {pair}.")
        return 0.0

    def _adjust_hyperliquid_order(
        self,
        order: dict,
    ) -> dict:
        """
        Adjusts order response for Hyperliquid
        :param order: Order response from Hyperliquid
        :return: Adjusted order response
        """
        if (
            order["average"] is None
            and order["status"] in ("canceled", "closed")
            and order["filled"] > 0
        ):
            # Hyperliquid does not fill the average price in the order response
            # Fetch trades to calculate the average price to have the actual price
            # the order was executed at
            trades = self.get_trades_for_order(
                order["id"], order["symbol"], since=dt_from_ts(order["timestamp"])
            )

            if trades:
                total_amount = sum(t["amount"] for t in trades)
                order["average"] = (
                    sum(t["price"] * t["amount"] for t in trades) / total_amount
                    if total_amount
                    else None
                )
        return order

    def fetch_order(self, order_id: str, pair: str, params: dict | None = None) -> CcxtOrder:
        order = super().fetch_order(order_id, pair, params)

        order = self._adjust_hyperliquid_order(order)
        self._log_exchange_response("fetch_order2", order)

        return order

    def fetch_orders(
        self, pair: str, since: datetime, params: dict | None = None
    ) -> list[CcxtOrder]:
        orders = super().fetch_orders(pair, since, params)
        for idx, order in enumerate(deepcopy(orders)):
            order2 = self._adjust_hyperliquid_order(order)
            orders[idx] = order2

        self._log_exchange_response("fetch_orders2", orders)
        return orders

    # Hyperliquid POST /info has aggressive per-IP rate limits. Firing the full
    # OHLCV refresh batch (up to 100 concurrent requests) at startup reliably
    # triggers HTTP 429s. Cap concurrent OHLCV fetches with a semaphore so we
    # avoid the warning storm without slowing steady-state operation much.
    _OHLCV_CONCURRENCY = 4
    _ohlcv_semaphore: asyncio.Semaphore | None = None

    def _get_ohlcv_semaphore(self) -> asyncio.Semaphore:
        # Lazy-create on the running loop (semaphores bind to a loop on first await).
        sem = self._ohlcv_semaphore
        if sem is None:
            sem = asyncio.Semaphore(self._OHLCV_CONCURRENCY)
            self._ohlcv_semaphore = sem
        return sem

    async def _async_get_candle_history(
        self,
        pair: str,
        timeframe: str,
        candle_type: CandleType,
        since_ms: int | None = None,
    ) -> OHLCVResponse:
        async with self._get_ohlcv_semaphore():
            return await super()._async_get_candle_history(
                pair, timeframe, candle_type, since_ms
            )
