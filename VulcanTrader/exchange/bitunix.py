# ruff: noqa
"""
Bitunix exchange implementation with ccxt-like wrapper.
Bitunix is a cryptocurrency derivatives exchange that is not supported by CCXT.
This module provides a fake CCXT wrapper similar to drift.py.
"""

import asyncio
import hashlib
import json
import logging
import time
import uuid
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Any, Callable, Literal

import aiohttp
import requests
from pandas import DataFrame

from VulcanTrader.constants import BuySell
from VulcanTrader.data.converter import ohlcv_to_dataframe
from VulcanTrader.enums import CandleType, MarginMode, TradingMode
from VulcanTrader.util.exceptions import (
    DDosProtection,
    ExchangeError,
    InsufficientFundsError,
    InvalidOrderException,
    OperationalException,
    TemporaryError,
)
from VulcanTrader.exchange import Exchange
from VulcanTrader.exchange.exchange_types import (
    CcxtBalances,
    CcxtOrder,
    TraderHas,
    OrderBook,
    Ticker,
    Tickers,
)
from VulcanTrader.util import FtTTLCache


logger = logging.getLogger(__name__)


class BitunixErrorCode:
    """Bitunix API Error Codes"""
    SUCCESS = 0
    NETWORK_ERROR = 10001
    PARAMETER_ERROR = 10002
    API_KEY_EMPTY = 10003
    IP_NOT_IN_WHITELIST = 10004
    TOO_MANY_REQUESTS = 10005
    REQUEST_TOO_FREQUENTLY = 10006
    SIGN_SIGNATURE_ERROR = 10007
    VALUE_NOT_COMPLY = 10008
    MARKET_NOT_EXISTS = 20001
    POSITION_EXCEED_LIMIT = 20002
    INSUFFICIENT_BALANCE = 20003
    ORDER_NOT_FOUND = 20007
    INSUFFICIENT_AMOUNT = 20008
    ORDER_FAILED_LIQUIDATION = 30001
    CLIENT_ID_DUPLICATE = 30042


class BitunixAuth:
    """Authentication helper for Bitunix API"""
    
    @staticmethod
    def get_nonce() -> str:
        """Generate a random string as nonce"""
        return str(uuid.uuid4()).replace('-', '')
    
    @staticmethod
    def get_timestamp() -> str:
        """Get current timestamp in milliseconds"""
        return str(int(time.time() * 1000))
    
    @staticmethod
    def sort_params(params: dict) -> str:
        """Sort parameters and concatenate them"""
        if not params:
            return ""
        return ''.join(f"{k}{v}" for k, v in sorted(params.items()))
    
    @staticmethod
    def generate_signature(
        api_key: str,
        secret_key: str,
        nonce: str,
        timestamp: str,
        query_params: str = "",
        body: str = ""
    ) -> str:
        """Generate signature according to Bitunix OpenAPI doc"""
        digest_input = nonce + timestamp + api_key + query_params + body
        digest = hashlib.sha256(digest_input.encode('utf-8')).hexdigest()
        sign_input = digest + secret_key
        sign = hashlib.sha256(sign_input.encode('utf-8')).hexdigest()
        return sign
    
    @staticmethod
    def get_auth_headers(
        api_key: str,
        secret_key: str,
        query_params: str = "",
        body: str = ""
    ) -> dict[str, str]:
        """Get authentication headers"""
        nonce = BitunixAuth.get_nonce()
        timestamp = BitunixAuth.get_timestamp()
        
        sign = BitunixAuth.generate_signature(
            api_key=api_key,
            secret_key=secret_key,
            nonce=nonce,
            timestamp=timestamp,
            query_params=query_params,
            body=body
        )
        
        return {
            "api-key": api_key,
            "sign": sign,
            "nonce": nonce,
            "timestamp": timestamp,
            "language": "en-US",
            "Content-Type": "application/json"
        }


class BitunixWebSocket:
    """
    WebSocket handler for Bitunix exchange.
    Handles both public and private WebSocket connections.
    
    Public channels (wss://fapi.bitunix.com/public/):
        - price: Market price with funding rate (mp, ip, fr, ft, nft)
        - ticker: 24h mini-ticker for single symbol (o, h, l, la, b, q, r)
        - tickers: 24h ticker statistics for multiple symbols
        - trade: Public trade data (price, volume, side, timestamp)
        - depth_book1: Order book depth - top 1 level
        - depth_book5: Order book depth - top 5 levels
        - depth_book15: Order book depth - top 15 levels
        - depth_books: Full order book depth (100 levels)
        - market_kline_<interval>: Candlestick/OHLCV data
    
    Private channels (wss://fapi.bitunix.com/private/):
        - balance: Account balance updates
        - order: Order updates (CREATE, UPDATE, CLOSE)
        - position: Position updates (OPEN, UPDATE, CLOSE)
        - tpsl: TP/SL order updates (CREATE, UPDATE, CLOSE)
    
    Subscription format:
        {"op": "subscribe", "args": [{"symbol": "BTCUSDT", "ch": "<channel>"}]}
    
    Unsubscribe format:
        {"op": "unsubscribe", "args": [{"symbol": "BTCUSDT", "channel": "<channel>"}]}
    
    Ping format:
        {"op": "ping", "ping": <unix_timestamp_seconds>}
    
    Login format (private):
        {"op": "login", "args": [{"apiKey": "...", "timestamp": ..., "nonce": "...", "sign": "..."}]}
    """
    
    WS_PUBLIC_URL = "wss://fapi.bitunix.com/public/"
    WS_PRIVATE_URL = "wss://fapi.bitunix.com/private/"
    
    # Kline interval mapping
    KLINE_INTERVALS = {
        "1m": "market_kline_1min",
        "3m": "market_kline_3min",
        "5m": "market_kline_5min",
        "15m": "market_kline_15min",
        "30m": "market_kline_30min",
        "1h": "market_kline_1h",
        "2h": "market_kline_2h",
        "4h": "market_kline_4h",
        "6h": "market_kline_6h",
        "8h": "market_kline_8h",
        "12h": "market_kline_12h",
        "1d": "market_kline_1day",
        "3d": "market_kline_3day",
        "1w": "market_kline_1week",
        "1M": "market_kline_1month",
    }
    
    def __init__(
        self,
        api_key: str = "",
        api_secret: str = "",
        on_message: Callable[[dict], None] | None = None,
    ) -> None:
        self._api_key = api_key
        self._api_secret = api_secret
        self._on_message = on_message
        
        # WebSocket connections
        self._public_ws: aiohttp.ClientWebSocketResponse | None = None
        self._private_ws: aiohttp.ClientWebSocketResponse | None = None
        self._session: aiohttp.ClientSession | None = None
        
        # Subscriptions tracking
        self._public_subscriptions: set[tuple[str, str]] = set()  # (symbol, channel)
        self._private_subscriptions: set[str] = set()  # channel names
        
        # Data caches
        self._prices: dict[str, dict] = {}  # symbol -> price data
        self._tickers: dict[str, dict] = {}  # symbol -> ticker data
        self._trades: dict[str, list] = defaultdict(list)  # symbol -> trades list
        self._orderbooks: dict[str, dict] = {}  # symbol -> orderbook
        self._balances: dict[str, dict] = {}  # coin -> balance data
        self._orders: dict[str, dict] = {}  # orderId -> order data
        self._positions: dict[str, dict] = {}  # positionId -> position data
        self._tpsl_orders: dict[str, dict] = {}  # orderId -> tpsl order data
        self._ohlcv: dict[str, dict[str, list]] = defaultdict(dict)  # symbol -> {interval -> candles}
        
        # Connection state
        self._running = False
        self._reconnect_delay = 1.0
        self._max_reconnect_delay = 60.0
        self._ping_interval = 25  # seconds
        self._last_ping = 0.0
    
    async def _ensure_session(self) -> None:
        """Ensure aiohttp session exists"""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=30)
            self._session = aiohttp.ClientSession(timeout=timeout)
    
    async def ping(self, ws: aiohttp.ClientWebSocketResponse | None = None) -> bool:
        """Send ping to keep connection alive
        
        Request: {"op": "ping", "ping": <unix_timestamp_seconds>}
        Response: {"op": "ping", "pong": <request_timestamp>, "ping": <server_timestamp>}
        """
        target_ws = ws or self._public_ws
        if not target_ws:
            return False
        
        try:
            timestamp = int(time.time())
            ping_msg = {
                "op": "ping",
                "ping": timestamp
            }
            await target_ws.send_json(ping_msg)
            self._last_ping = time.time()
            return True
        except Exception as e:
            logger.error(f"Failed to send ping: {e}")
            return False
    
    async def connect_public(self) -> bool:
        """Connect to public WebSocket"""
        try:
            await self._ensure_session()
            self._public_ws = await self._session.ws_connect(
                self.WS_PUBLIC_URL,
                heartbeat=None,  # We handle ping manually
            )
            logger.info("Connected to Bitunix public WebSocket")
            self._running = True
            # Start ping task
            asyncio.create_task(self._ping_loop(self._public_ws))
            return True
        except Exception as e:
            logger.error(f"Failed to connect to public WebSocket: {e}")
            return False
    
    async def connect_private(self) -> bool:
        """Connect to private WebSocket with authentication"""
        if not self._api_key or not self._api_secret:
            logger.warning("Cannot connect to private WebSocket without API credentials")
            return False
        
        try:
            await self._ensure_session()
            self._private_ws = await self._session.ws_connect(
                self.WS_PRIVATE_URL,
                heartbeat=None,  # We handle ping manually
            )
            
            # Authenticate
            auth_success = await self._authenticate()
            if auth_success:
                logger.info("Connected and authenticated to Bitunix private WebSocket")
                # Start ping task
                asyncio.create_task(self._ping_loop(self._private_ws))
                return True
            else:
                logger.error("Failed to authenticate to private WebSocket")
                await self._private_ws.close()
                self._private_ws = None
                return False
        except Exception as e:
            logger.error(f"Failed to connect to private WebSocket: {e}")
            return False
    
    async def _ping_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        """Periodic ping to keep connection alive"""
        while self._running and ws and not ws.closed:
            try:
                await asyncio.sleep(self._ping_interval)
                if ws and not ws.closed:
                    await self.ping(ws)
            except Exception as e:
                logger.debug(f"Ping loop error: {e}")
                break
    
    async def _authenticate(self) -> bool:
        """Authenticate to private WebSocket
        
        Login format from API5:
        {
            "op": "login",
            "args": [{
                "apiKey": "...",
                "timestamp": <int>,  # Unix timestamp in milliseconds
                "nonce": "...",
                "sign": "..."
            }]
        }
        
        Signature: SHA256(nonce + timestamp + apiKey) -> SHA256(result + secretKey)
        """
        if not self._private_ws:
            return False
        
        try:
            nonce = BitunixAuth.get_nonce()
            timestamp = int(time.time() * 1000)  # Milliseconds as integer
            
            # Generate signature per API5: SHA256(nonce + timestamp + apiKey) then SHA256(result + secretKey)
            sign_str = f"{nonce}{timestamp}{self._api_key}"
            sign = hashlib.sha256(sign_str.encode()).hexdigest()
            sign = hashlib.sha256(f"{sign}{self._api_secret}".encode()).hexdigest()
            
            auth_msg = {
                "op": "login",
                "args": [{
                    "apiKey": self._api_key,
                    "timestamp": timestamp,
                    "nonce": nonce,
                    "sign": sign,
                }]
            }
            
            await self._private_ws.send_json(auth_msg)
            logger.debug(f"Sent login message to private WebSocket")
            
            # Wait for auth response with timeout
            try:
                async with asyncio.timeout(10):
                    async for msg in self._private_ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            logger.debug(f"Auth response: {data}")
                            if data.get("op") == "login" or data.get("event") == "login":
                                return data.get("code") == 0 or data.get("success", False)
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
            except asyncio.TimeoutError:
                logger.error("Authentication timeout")
                return False
            return False
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            return False
    
    async def subscribe_price(self, symbol: str) -> bool:
        """Subscribe to market price channel (includes funding rate)
        
        Receives: mp (market price), ip (index price), fr (funding rate),
                  ft (funding time), nft (next funding time)
        """
        return await self._subscribe_public(symbol, "price")
    
    async def subscribe_ticker(self, symbol: str) -> bool:
        """Subscribe to 24h rolling window mini-ticker for a single symbol
        
        Channel: ticker (singular, for individual symbol)
        Receives: o (open), h (high), l (low), la (last), b (base volume),
                  q (quote volume), r (24h change ratio)
        
        Example subscription:
            {"op": "subscribe", "args": [{"symbol": "BTCUSDT", "ch": "ticker"}]}
        """
        return await self._subscribe_public(symbol, "ticker")
    
    async def subscribe_tickers(self, symbols: list[str]) -> bool:
        """Subscribe to 24h ticker channel for multiple symbols
        
        Channel: tickers (plural, for multiple symbols)
        Receives: open, high, low, last, volume, quote volume, 24h change,
                  best bid/ask price and volume
        """
        args = [{"symbol": s.replace("/", "").replace(":USDT", ""), "ch": "tickers"} for s in symbols]
        return await self._subscribe_public_multi(args)
    
    async def subscribe_trades(self, symbol: str) -> bool:
        """Subscribe to public trades channel
        
        Receives: price, volume, side, timestamp for each trade
        """
        return await self._subscribe_public(symbol, "trade")
    
    async def subscribe_depth(self, symbol: str, level: str = "depth_book5") -> bool:
        """Subscribe to order book depth channel
        
        :param level: depth_book1, depth_book5, depth_book15, or depth_books
        """
        valid_levels = ["depth_book1", "depth_book5", "depth_book15", "depth_books"]
        if level not in valid_levels:
            level = "depth_book5"
        return await self._subscribe_public(symbol, level)
    
    async def subscribe_kline(self, symbol: str, interval: str = "1m") -> bool:
        """Subscribe to candlestick/OHLCV channel
        
        :param symbol: Trading pair (e.g., 'BTC/USDT:USDT' or 'BTCUSDT')
        :param interval: Kline interval (1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M)
        """
        channel = self.KLINE_INTERVALS.get(interval, "market_kline_1min")
        return await self._subscribe_public(symbol, channel)
    
    async def subscribe_klines(self, symbols: list[str], interval: str = "1m") -> bool:
        """Subscribe to candlestick/OHLCV channel for multiple symbols
        
        :param symbols: List of trading pairs
        :param interval: Kline interval
        """
        channel = self.KLINE_INTERVALS.get(interval, "market_kline_1min")
        args = [{"symbol": s.replace("/", "").replace(":USDT", ""), "ch": channel} for s in symbols]
        return await self._subscribe_public_multi(args)
    
    async def subscribe_balance(self) -> bool:
        """Subscribe to account balance updates (private)"""
        return await self._subscribe_private("balance")
    
    async def subscribe_orders(self) -> bool:
        """Subscribe to order updates (private)
        
        Events: CREATE, UPDATE, CLOSE
        """
        return await self._subscribe_private("order")
    
    async def subscribe_positions(self) -> bool:
        """Subscribe to position updates (private)
        
        Events: OPEN, UPDATE, CLOSE
        """
        return await self._subscribe_private("position")
    
    async def subscribe_tpsl(self) -> bool:
        """Subscribe to TP/SL order updates (private)
        
        Events: CREATE, UPDATE, CLOSE
        """
        return await self._subscribe_private("tpsl")
    
    async def _subscribe_public(self, symbol: str, channel: str) -> bool:
        """Subscribe to a public channel"""
        if not self._public_ws:
            if not await self.connect_public():
                return False
        
        bitunix_symbol = symbol.replace("/", "").replace(":USDT", "")
        
        msg = {
            "op": "subscribe",
            "args": [{"symbol": bitunix_symbol, "ch": channel}]
        }
        
        try:
            await self._public_ws.send_json(msg)
            self._public_subscriptions.add((bitunix_symbol, channel))
            logger.debug(f"Subscribed to {channel} for {bitunix_symbol}")
            return True
        except Exception as e:
            logger.error(f"Failed to subscribe to {channel}: {e}")
            return False
    
    async def _subscribe_public_multi(self, args: list[dict]) -> bool:
        """Subscribe to multiple public channels"""
        if not self._public_ws:
            if not await self.connect_public():
                return False
        
        msg = {"op": "subscribe", "args": args}
        
        try:
            await self._public_ws.send_json(msg)
            for arg in args:
                self._public_subscriptions.add((arg["symbol"], arg["ch"]))
            logger.debug(f"Subscribed to {len(args)} channels")
            return True
        except Exception as e:
            logger.error(f"Failed to subscribe: {e}")
            return False
    
    async def _subscribe_private(self, channel: str) -> bool:
        """Subscribe to a private channel"""
        if not self._private_ws:
            if not await self.connect_private():
                return False
        
        msg = {
            "op": "subscribe",
            "args": [{"ch": channel}]
        }
        
        try:
            await self._private_ws.send_json(msg)
            self._private_subscriptions.add(channel)
            logger.debug(f"Subscribed to private channel: {channel}")
            return True
        except Exception as e:
            logger.error(f"Failed to subscribe to private {channel}: {e}")
            return False
    
    async def unsubscribe_public(self, symbol: str, channel: str) -> bool:
        """Unsubscribe from a public channel
        
        Unsubscribe format from API5 (note: uses 'channel' not 'ch'):
        {"op": "unsubscribe", "args": [{"symbol": "BTCUSDT", "channel": "market_kline_1min"}]}
        """
        if not self._public_ws:
            return True
        
        bitunix_symbol = symbol.replace("/", "").replace(":USDT", "")
        
        msg = {
            "op": "unsubscribe",
            "args": [{"symbol": bitunix_symbol, "channel": channel}]  # Use 'channel' not 'ch'
        }
        
        try:
            await self._public_ws.send_json(msg)
            self._public_subscriptions.discard((bitunix_symbol, channel))
            logger.debug(f"Unsubscribed from {channel} for {bitunix_symbol}")
            return True
        except Exception as e:
            logger.error(f"Failed to unsubscribe: {e}")
            return False
    
    async def unsubscribe_private(self, channel: str) -> bool:
        """Unsubscribe from a private channel
        
        Unsubscribe format (uses 'channel' not 'ch'):
        {"op": "unsubscribe", "args": [{"channel": "<channel>"}]}
        """
        if not self._private_ws:
            return True
        
        msg = {
            "op": "unsubscribe",
            "args": [{"channel": channel}]  # Use 'channel' not 'ch'
        }
        
        try:
            await self._private_ws.send_json(msg)
            self._private_subscriptions.discard(channel)
            logger.debug(f"Unsubscribed from private channel: {channel}")
            return True
        except Exception as e:
            logger.error(f"Failed to unsubscribe: {e}")
            return False
    
    async def unsubscribe_kline(self, symbol: str, interval: str = "1m") -> bool:
        """Unsubscribe from candlestick/OHLCV channel"""
        channel = self.KLINE_INTERVALS.get(interval, "market_kline_1min")
        return await self.unsubscribe_public(symbol, channel)
    
    def _handle_price_message(self, msg: dict) -> None:
        """Handle price channel message"""
        symbol = msg.get("symbol", "")
        data = msg.get("data", {})
        
        self._prices[symbol] = {
            "symbol": symbol,
            "markPrice": float(data.get("mp", 0)),
            "indexPrice": float(data.get("ip", 0)),
            "fundingRate": float(data.get("fr", 0)),
            "fundingTime": data.get("ft"),
            "nextFundingTime": data.get("nft"),
            "timestamp": msg.get("ts", 0),
        }
    
    def _handle_ticker_message(self, msg: dict) -> None:
        """Handle individual ticker channel message (singular 'ticker' channel)
        
        Format from API6:
        {
            "ch": "ticker",
            "symbol": "BNBUSDT",
            "ts": 1732178884994,
            "data": {
                "la": "0.0025",  # last price
                "o": "0.0010",   # open
                "h": "0.0025",   # high
                "l": "0.0010",   # low
                "b": "10000",    # base volume
                "q": "1",        # quote volume
                "r": "0.98"      # 24h change ratio
            }
        }
        """
        symbol = msg.get("symbol", "")
        data = msg.get("data", {})
        ts = msg.get("ts", 0)
        
        self._tickers[symbol] = {
            "symbol": symbol,
            "open": float(data.get("o", 0)),
            "high": float(data.get("h", 0)),
            "low": float(data.get("l", 0)),
            "last": float(data.get("la", 0)),
            "baseVolume": float(data.get("b", 0)),
            "quoteVolume": float(data.get("q", 0)),
            "percentage": float(data.get("r", 0)) * 100,
            "bid": None,  # Not provided in ticker channel
            "ask": None,  # Not provided in ticker channel
            "bidVolume": None,
            "askVolume": None,
            "timestamp": ts,
        }
    
    def _handle_tickers_message(self, msg: dict) -> None:
        """Handle tickers channel message (plural 'tickers' channel for multiple symbols)"""
        data_list = msg.get("data", [])
        ts = msg.get("ts", 0)
        
        for ticker in data_list:
            symbol = ticker.get("s", "")
            self._tickers[symbol] = {
                "symbol": symbol,
                "open": float(ticker.get("o", 0)),
                "high": float(ticker.get("h", 0)),
                "low": float(ticker.get("l", 0)),
                "last": float(ticker.get("la", 0)),
                "baseVolume": float(ticker.get("b", 0)),
                "quoteVolume": float(ticker.get("q", 0)),
                "percentage": float(ticker.get("r", 0)) * 100,
                "bid": float(ticker.get("bd", 0)),
                "ask": float(ticker.get("ak", 0)),
                "bidVolume": float(ticker.get("bv", 0)),
                "askVolume": float(ticker.get("av", 0)),
                "timestamp": ts,
            }
    
    def _handle_trade_message(self, msg: dict) -> None:
        """Handle trade channel message"""
        symbol = msg.get("symbol", "")
        data_list = msg.get("data", [])
        
        for trade in data_list:
            parsed_trade = {
                "symbol": symbol,
                "price": float(trade.get("p", 0)),
                "amount": float(trade.get("v", 0)),
                "side": trade.get("s", "").lower(),
                "timestamp": trade.get("t"),
            }
            # Keep last 100 trades per symbol
            self._trades[symbol].append(parsed_trade)
            if len(self._trades[symbol]) > 100:
                self._trades[symbol] = self._trades[symbol][-100:]
    
    def _handle_depth_message(self, msg: dict) -> None:
        """Handle order book depth message"""
        symbol = msg.get("symbol", "")
        data = msg.get("data", {})
        
        bids = [[float(b[0]), float(b[1])] for b in data.get("b", [])]
        asks = [[float(a[0]), float(a[1])] for a in data.get("a", [])]
        
        self._orderbooks[symbol] = {
            "symbol": symbol,
            "bids": bids,
            "asks": asks,
            "timestamp": msg.get("ts", 0),
        }
    
    def _handle_balance_message(self, msg: dict) -> None:
        """Handle balance channel message"""
        data = msg.get("data", {})
        coin = data.get("coin", "USDT")
        
        self._balances[coin] = {
            "coin": coin,
            "available": float(data.get("available", 0)),
            "frozen": float(data.get("frozen", 0)),
            "isolationFrozen": float(data.get("isolationFrozen", 0)),
            "crossFrozen": float(data.get("crossFrozen", 0)),
            "margin": float(data.get("margin", 0)),
            "isolationMargin": float(data.get("isolationMargin", 0)),
            "crossMargin": float(data.get("crossMargin", 0)),
            "expMoney": float(data.get("expMoney", 0)),
            "timestamp": msg.get("ts", 0),
        }
    
    def _handle_order_message(self, msg: dict) -> None:
        """Handle order channel message"""
        data = msg.get("data", {})
        order_id = data.get("orderId", "")
        
        self._orders[order_id] = {
            "event": data.get("event"),  # CREATE, UPDATE, CLOSE
            "orderId": order_id,
            "symbol": data.get("symbol", ""),
            "side": data.get("side", "").lower(),
            "type": data.get("type", "").lower(),
            "price": float(data.get("price", 0)) if data.get("price") else None,
            "amount": float(data.get("qty", 0)),
            "filled": float(data.get("dealAmount", 0)),
            "average": float(data.get("averagePrice", 0)) if data.get("averagePrice") else None,
            "status": data.get("orderStatus", ""),
            "leverage": int(data.get("leverage", 1)),
            "marginMode": data.get("positionType", "").lower(),
            "positionMode": data.get("positionMode", ""),
            "fee": float(data.get("fee", 0)),
            "clientId": data.get("clientId"),
            "tpPrice": data.get("tpPrice"),
            "slPrice": data.get("slPrice"),
            "timestamp": int(data.get("ctime", 0)),
            "updateTime": int(data.get("mtime", 0)),
        }
        
        # Remove closed orders from cache after delay
        if data.get("event") == "CLOSE":
            asyncio.get_event_loop().call_later(
                5.0, lambda: self._orders.pop(order_id, None)
            )
    
    def _handle_position_message(self, msg: dict) -> None:
        """Handle position channel message"""
        data = msg.get("data", {})
        position_id = data.get("positionId", "")
        
        self._positions[position_id] = {
            "event": data.get("event"),  # OPEN, UPDATE, CLOSE
            "positionId": position_id,
            "symbol": data.get("symbol", ""),
            "side": data.get("side", "").lower(),
            "leverage": int(data.get("leverage", 1)),
            "margin": float(data.get("margin", 0)),
            "contracts": float(data.get("qty", 0)),
            "marginMode": data.get("marginMode", "").lower(),
            "positionMode": data.get("positionMode", ""),
            "realizedPnl": float(data.get("realizedPNL", 0)),
            "unrealizedPnl": float(data.get("unrealizedPNL", 0)),
            "funding": float(data.get("funding", 0)),
            "fee": float(data.get("fee", 0)),
            "timestamp": int(data.get("ctime", 0)),
        }
        
        # Remove closed positions from cache after delay
        if data.get("event") == "CLOSE":
            asyncio.get_event_loop().call_later(
                5.0, lambda: self._positions.pop(position_id, None)
            )
    
    def _handle_tpsl_message(self, msg: dict) -> None:
        """Handle TP/SL channel message"""
        data = msg.get("data", {})
        order_id = data.get("orderId", "")
        
        self._tpsl_orders[order_id] = {
            "event": data.get("event"),  # CREATE, UPDATE, CLOSE
            "orderId": order_id,
            "positionId": data.get("positionId", ""),
            "symbol": data.get("symbol", ""),
            "side": data.get("side", "").lower(),
            "leverage": int(data.get("leverage", 1)),
            "positionMode": data.get("positionMode", ""),
            "status": data.get("status", ""),
            "type": data.get("type", "").lower(),
            "tpQty": float(data.get("tpQty", 0)) if data.get("tpQty") else None,
            "slQty": float(data.get("slQty", 0)) if data.get("slQty") else None,
            "tpPrice": data.get("tpPrice"),
            "tpStopType": data.get("tpStopType"),
            "tpOrderType": data.get("tpOrderType"),
            "tpOrderPrice": data.get("tpOrderPrice"),
            "slPrice": data.get("slPrice"),
            "slStopType": data.get("slStopType"),
            "slOrderType": data.get("slOrderType"),
            "slOrderPrice": data.get("slOrderPrice"),
            "timestamp": int(data.get("ctime", 0)),
        }
        
        if data.get("event") == "CLOSE":
            asyncio.get_event_loop().call_later(
                5.0, lambda: self._tpsl_orders.pop(order_id, None)
            )
    
    def _handle_kline_message(self, msg: dict, channel: str) -> None:
        """Handle kline/candlestick channel message
        
        Channel format: market_kline_<interval> (e.g., market_kline_1min, market_kline_1h)
        """
        symbol = msg.get("symbol", "")
        data = msg.get("data", {})
        
        # Extract interval from channel name
        interval = channel.replace("market_kline_", "")
        
        candle = {
            "timestamp": int(data.get("t", 0)),  # Open time
            "open": float(data.get("o", 0)),
            "high": float(data.get("h", 0)),
            "low": float(data.get("l", 0)),
            "close": float(data.get("c", 0)),
            "volume": float(data.get("v", 0)),
            "quoteVolume": float(data.get("qv", 0)) if data.get("qv") else None,
        }
        
        # Store candle - keep last 200 candles per symbol/interval
        if symbol not in self._ohlcv:
            self._ohlcv[symbol] = {}
        if interval not in self._ohlcv[symbol]:
            self._ohlcv[symbol][interval] = []
        
        # Update or append candle
        candles = self._ohlcv[symbol][interval]
        if candles and candles[-1]["timestamp"] == candle["timestamp"]:
            # Update existing candle
            candles[-1] = candle
        else:
            # Append new candle
            candles.append(candle)
            # Keep only last 200 candles
            if len(candles) > 200:
                self._ohlcv[symbol][interval] = candles[-200:]
    
    async def _process_message(self, msg: dict) -> None:
        """Process incoming WebSocket message"""
        # Handle ping/pong responses
        op = msg.get("op", "")
        if op == "ping":
            # Pong response received
            logger.debug(f"Received pong: {msg}")
            return
        
        channel = msg.get("ch", "")
        
        try:
            if channel == "price":
                self._handle_price_message(msg)
            elif channel == "ticker":
                self._handle_ticker_message(msg)  # Individual symbol ticker
            elif channel == "tickers":
                self._handle_tickers_message(msg)  # Multiple symbols tickers
            elif channel == "trade":
                self._handle_trade_message(msg)
            elif channel.startswith("depth_"):
                self._handle_depth_message(msg)
            elif channel.startswith("market_kline_"):
                self._handle_kline_message(msg, channel)
            elif channel == "balance":
                self._handle_balance_message(msg)
            elif channel == "order":
                self._handle_order_message(msg)
            elif channel == "position":
                self._handle_position_message(msg)
            elif channel == "tpsl":
                self._handle_tpsl_message(msg)
            
            # Call user callback if provided
            if self._on_message:
                self._on_message(msg)
                
        except Exception as e:
            logger.error(f"Error processing WebSocket message: {e}")
    
    async def listen_public(self) -> None:
        """Listen for public WebSocket messages"""
        if not self._public_ws:
            return
        
        try:
            async for msg in self._public_ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                        await self._process_message(data)
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to decode WS message: {e}")
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logger.error(f"WebSocket error: {self._public_ws.exception()}")
                    break
                elif msg.type == aiohttp.WSMsgType.CLOSED:
                    logger.info("Public WebSocket closed")
                    break
        except Exception as e:
            logger.error(f"Error in public WebSocket listener: {e}")
        finally:
            self._public_ws = None
    
    async def listen_private(self) -> None:
        """Listen for private WebSocket messages"""
        if not self._private_ws:
            return
        
        try:
            async for msg in self._private_ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                        await self._process_message(data)
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to decode WS message: {e}")
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logger.error(f"WebSocket error: {self._private_ws.exception()}")
                    break
                elif msg.type == aiohttp.WSMsgType.CLOSED:
                    logger.info("Private WebSocket closed")
                    break
        except Exception as e:
            logger.error(f"Error in private WebSocket listener: {e}")
        finally:
            self._private_ws = None
    
    async def close(self) -> None:
        """Close all WebSocket connections"""
        self._running = False
        
        if self._public_ws:
            await self._public_ws.close()
            self._public_ws = None
        
        if self._private_ws:
            await self._private_ws.close()
            self._private_ws = None
        
        if self._session:
            await self._session.close()
            self._session = None
        
        logger.info("Bitunix WebSocket connections closed")
    
    # Getter methods for cached data
    
    def get_price(self, symbol: str) -> dict | None:
        """Get cached price data for a symbol"""
        bitunix_symbol = symbol.replace("/", "").replace(":USDT", "")
        return self._prices.get(bitunix_symbol)
    
    def get_ticker(self, symbol: str) -> dict | None:
        """Get cached ticker data for a symbol"""
        bitunix_symbol = symbol.replace("/", "").replace(":USDT", "")
        return self._tickers.get(bitunix_symbol)
    
    def get_all_tickers(self) -> dict[str, dict]:
        """Get all cached tickers"""
        return self._tickers.copy()
    
    def get_trades(self, symbol: str) -> list[dict]:
        """Get cached trades for a symbol"""
        bitunix_symbol = symbol.replace("/", "").replace(":USDT", "")
        return self._trades.get(bitunix_symbol, []).copy()
    
    def get_orderbook(self, symbol: str) -> dict | None:
        """Get cached orderbook for a symbol"""
        bitunix_symbol = symbol.replace("/", "").replace(":USDT", "")
        return self._orderbooks.get(bitunix_symbol)
    
    def get_balance(self, coin: str = "USDT") -> dict | None:
        """Get cached balance for a coin"""
        return self._balances.get(coin)
    
    def get_all_balances(self) -> dict[str, dict]:
        """Get all cached balances"""
        return self._balances.copy()
    
    def get_order(self, order_id: str) -> dict | None:
        """Get cached order by ID"""
        return self._orders.get(order_id)
    
    def get_all_orders(self) -> dict[str, dict]:
        """Get all cached orders"""
        return self._orders.copy()
    
    def get_position(self, position_id: str) -> dict | None:
        """Get cached position by ID"""
        return self._positions.get(position_id)
    
    def get_all_positions(self) -> dict[str, dict]:
        """Get all cached positions"""
        return self._positions.copy()
    
    def get_tpsl_order(self, order_id: str) -> dict | None:
        """Get cached TP/SL order by ID"""
        return self._tpsl_orders.get(order_id)
    
    def get_all_tpsl_orders(self) -> dict[str, dict]:
        """Get all cached TP/SL orders"""
        return self._tpsl_orders.copy()
    
    def get_ohlcv(self, symbol: str, interval: str = "1min") -> list[dict]:
        """Get cached OHLCV/kline data for a symbol and interval
        
        :param symbol: Trading pair (e.g., 'BTC/USDT:USDT' or 'BTCUSDT')
        :param interval: Kline interval (1min, 5min, 1h, etc.)
        :returns: List of candle dicts
        """
        bitunix_symbol = symbol.replace("/", "").replace(":USDT", "")
        return self._ohlcv.get(bitunix_symbol, {}).get(interval, []).copy()
    
    def get_all_ohlcv(self, symbol: str) -> dict[str, list]:
        """Get all cached OHLCV data for a symbol (all intervals)"""
        bitunix_symbol = symbol.replace("/", "").replace(":USDT", "")
        return {k: v.copy() for k, v in self._ohlcv.get(bitunix_symbol, {}).items()}


class BitunixCCXTAdapter:
    """
    A minimal ccxt-like adapter for Bitunix.
    This allows VulcanTrader to interact with Bitunix like it does with real ccxt exchanges.
    """
    
    def __init__(self, parent: "Bitunix", *, is_async: bool) -> None:
        self._parent = parent
        self._is_async = is_async
        
        self.id = "bitunix"
        self.name = "Bitunix"
        self.markets: dict = {}
        self.symbols: list[str] = []
        
        # Rate limiting - Bitunix has strict limits (10006 = request too frequently)
        # Working bitunix_adapter.py targets ~8 req/sec; 125ms spacing is safe
        self.rateLimit = 125  # 125ms between requests (~8 rps)
        self.enableRateLimit = True
        self._last_request_time = 0.0
        self._rate_limit_lock = asyncio.Lock()
        
        # ccxt precision mode
        self.precisionMode = 2  # DECIMAL_PLACES
        
        # Supported timeframes
        self.timeframes = {
            "1m": "1m",
            "3m": "3m",
            "5m": "5m",
            "15m": "15m",
            "30m": "30m",
            "1h": "1h",
            "2h": "2h",
            "4h": "4h",
            "6h": "6h",
            "8h": "8h",
            "12h": "12h",
            "1d": "1d",
            "3d": "3d",
            "1w": "1w",
            "1M": "1M",
        }
        
        self.options = {
            "timeframes": {
                "spot": self.timeframes,
                "swap": self.timeframes,
            }
        }
        
        # Capabilities
        self.has = {
            "fetchOHLCV": True,
            "fetchTrades": True,
            "fetchTicker": True,
            "fetchTickers": True,
            "fetchOrderBook": True,
            "fetchL2OrderBook": True,
            "createOrder": True,
            "cancelOrder": True,
            "fetchBalance": True,
            "fetchOrder": True,
            "fetchOrders": True,
            "fetchOpenOrders": True,
            "fetchClosedOrders": True,
            "fetchMyTrades": True,
            "fetchPositions": True,
            "setLeverage": True,
            "setMarginMode": True,
            # WebSocket capabilities
            "watchOHLCV": True,  # market_kline_<interval> channels
            "watchOHLCVForSymbols": True,
            "watchTicker": True,
            "watchTickers": True,
            "watchTrades": True,
            "watchOrderBook": True,
            "watchBalance": True,
            "watchOrders": True,
            "watchPositions": True,
        }
        
        # Features for ccxt feature helpers
        self.features = {
            "spot": {"fetchOHLCV": {"limit": 200}},
            "swap": {"linear": {"fetchOHLCV": {"limit": 200}}},
        }
        
        # aiohttp session for async adapter
        self.session: aiohttp.ClientSession | None = None
        
        # WebSocket handler
        self._ws: BitunixWebSocket | None = None
        
        # OHLCV cache for ccxt compatibility (used by ExchangeWS)
        self.ohlcvs: dict[str, dict[str, list]] = {}
    
    def set_markets_from_exchange(self, other: Any) -> None:
        """ccxt helper used by VulcanTrader on real exchanges."""
        self.markets = getattr(other, "markets", {}) or {}
        self.symbols = list(self.markets.keys())
    
    def load_markets(self, *args, **kwargs):
        """Load markets from parent exchange"""
        self.markets = self._parent.markets
        self.symbols = list(self.markets.keys())
        return self.markets
    
    async def _throttle(self):
        """Apply rate limiting between requests"""
        if self.enableRateLimit:
            async with self._rate_limit_lock:
                now = time.time() * 1000  # current time in ms
                elapsed = now - self._last_request_time
                wait_time = self.rateLimit - elapsed
                if wait_time > 0:
                    await asyncio.sleep(wait_time / 1000)
                self._last_request_time = time.time() * 1000
    
    async def _ensure_session(self):
        """Ensure aiohttp session is created"""
        if self.session is None:
            timeout = aiohttp.ClientTimeout(total=30, connect=5, sock_read=15)
            connector = aiohttp.TCPConnector(limit=10, keepalive_timeout=30)
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
            )
    
    async def _get_json(self, url: str, params: dict | None = None, auth: bool = False, max_retries: int = 5) -> Any:
        """Make GET request and return JSON with built-in retry logic for rate limits"""
        for retry in range(max_retries):
            await self._throttle()
            await self._ensure_session()
            params = params or {}
            
            headers = {"Content-Type": "application/json"}
            if auth and self._parent._api_key and self._parent._api_secret:
                query_string = BitunixAuth.sort_params(params)
                headers = BitunixAuth.get_auth_headers(
                    self._parent._api_key,
                    self._parent._api_secret,
                    query_string
                )
            
            async with self.session.get(url, params=params, headers=headers) as resp:
                txt = await resp.text()
                
                if resp.status == 429:
                    # Exponential backoff on 429
                    wait_time = (2 ** retry) * 2  # 2, 4, 8, 16, 32 seconds
                    logger.warning(f"Bitunix rate limited (429), waiting {wait_time}s before retry {retry + 1}/{max_retries}")
                    await asyncio.sleep(wait_time)
                    continue
                
                if resp.status != 200:
                    raise TemporaryError(f"Bitunix API error {resp.status}: {txt[:200]}")
                
                try:
                    data = await resp.json()
                    error_code = data.get("code", 0)
                    
                    # Handle rate limit error 10006 with retry
                    if error_code in (BitunixErrorCode.TOO_MANY_REQUESTS, BitunixErrorCode.REQUEST_TOO_FREQUENTLY):
                        wait_time = (2 ** retry) * 2  # 2, 4, 8, 16, 32 seconds
                        logger.warning(f"Bitunix rate limit (code {error_code}), waiting {wait_time}s before retry {retry + 1}/{max_retries}")
                        await asyncio.sleep(wait_time)
                        continue
                    
                    if error_code != 0:
                        error_msg = data.get("msg", "Unknown error")
                        self._handle_error(error_code, error_msg)
                    
                    return data.get("data", data)
                except json.JSONDecodeError:
                    raise TemporaryError(f"Bitunix API returned non-JSON: {txt[:200]}")
        
        # If we exhausted all retries
        raise DDosProtection(f"Bitunix rate limit exceeded after {max_retries} retries")
    
    async def _post_json(self, url: str, data: dict | None = None) -> Any:
        """Make POST request and return JSON"""
        await self._throttle()
        await self._ensure_session()
        data = data or {}
        
        body = json.dumps(data)
        headers = BitunixAuth.get_auth_headers(
            self._parent._api_key,
            self._parent._api_secret,
            body=body
        )
        
        async with self.session.post(url, json=data, headers=headers) as resp:
            txt = await resp.text()
            
            if resp.status == 429:
                raise DDosProtection(f"Bitunix rate limited (429): {txt[:200]}")
            
            if resp.status != 200:
                raise TemporaryError(f"Bitunix API error {resp.status}: {txt[:200]}")
            
            try:
                response = await resp.json()
                if response.get("code") != 0:
                    error_code = response.get("code")
                    error_msg = response.get("msg", "Unknown error")
                    self._handle_error(error_code, error_msg)
                return response.get("data", response)
            except Exception as e:
                raise TemporaryError(f"Bitunix API returned non-JSON: {txt[:200]}") from e
    
    def _handle_error(self, code: int, msg: str):
        """Handle Bitunix error codes"""
        if code == BitunixErrorCode.INSUFFICIENT_BALANCE:
            raise InsufficientFundsError(f"Bitunix: {msg}")
        elif code == BitunixErrorCode.ORDER_NOT_FOUND:
            raise InvalidOrderException(f"Bitunix: {msg}")
        elif code in (BitunixErrorCode.TOO_MANY_REQUESTS, BitunixErrorCode.REQUEST_TOO_FREQUENTLY):
            raise DDosProtection(f"Bitunix: {msg}")
        elif code == BitunixErrorCode.SIGN_SIGNATURE_ERROR:
            raise ExchangeError(f"Bitunix authentication error: {msg}")
        elif code != 0:
            raise ExchangeError(f"Bitunix error {code}: {msg}")
    
    def _symbol_to_bitunix(self, symbol: str) -> str:
        """Convert VulcanTrader symbol to Bitunix format"""
        # Bitunix uses format like BTCUSDT
        if "/" in symbol:
            base, quote = symbol.split("/")
            # Remove :USDT suffix if present (futures)
            if ":" in quote:
                quote = quote.split(":")[0]
            return f"{base}{quote}"
        return symbol.replace("-", "").replace(":", "")
    
    def _bitunix_to_symbol(self, bitunix_symbol: str) -> str:
        """Convert Bitunix symbol to VulcanTrader format"""
        # Assume USDT quote for futures
        if bitunix_symbol.endswith("USDT"):
            base = bitunix_symbol[:-4]
            if self._parent.trading_mode == TradingMode.FUTURES:
                return f"{base}/USDT:USDT"
            return f"{base}/USDT"
        return bitunix_symbol
    
    async def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1m",
        since: int | None = None,
        limit: int | None = None,
        params: dict | None = None,
    ) -> list[list]:
        """Fetch OHLCV data from Bitunix.
        
        Uses the same field mapping as the proven bitunix_adapter.py:
        API returns: time, open, high, low, close, quoteVol
        
        Supports endTime via params={'endTime': <ms>} for backward pagination.
        Filters response data to only include candles at/after 'since' timestamp.
        Returns data in ascending timestamp order.
        """
        bitunix_symbol = self._symbol_to_bitunix(symbol)
        limit = min(limit or 200, 200)
        
        url = f"{self._parent._BASE_URL}/api/v1/futures/market/kline"
        
        request_params = {
            "symbol": bitunix_symbol,
            "interval": timeframe,
            "limit": str(max(10, limit)),
        }
        
        # Bitunix API supports startTime and endTime parameters
        if since is not None:
            request_params["startTime"] = str(since)
        
        # Support endTime via params dict (used for backward pagination)
        params = params or {}
        if "endTime" in params:
            request_params["endTime"] = str(params["endTime"])
        
        data = await self._get_json(url, request_params)
        
        if not data or not isinstance(data, list):
            return []
        
        # Convert to ccxt OHLCV format: [timestamp, open, high, low, close, volume]
        # Bitunix kline API returns: time, open, high, low, close, quoteVol
        ohlcv = []
        for candle in data:
            if not isinstance(candle, dict):
                continue
            try:
                ts_ms = int(candle.get("time", 0))
                # Filter: only include candles at/after 'since' timestamp
                if since is not None and ts_ms < since:
                    continue
                op = float(candle.get("open", 0))
                hi = float(candle.get("high", 0))
                lo = float(candle.get("low", 0))
                cl = float(candle.get("close", 0))
                vol = float(candle.get("quoteVol", 0)) if candle.get("quoteVol") is not None else 0.0
                ohlcv.append([ts_ms, op, hi, lo, cl, vol])
            except (ValueError, TypeError):
                continue
        
        # Sort by timestamp ascending
        ohlcv.sort(key=lambda x: x[0])
        return ohlcv[:limit]
    
    async def fetch_trades(
        self,
        symbol: str,
        since: int | None = None,
        limit: int | None = None,
        params: dict | None = None,
    ) -> list[dict]:
        """Fetch recent trades - Bitunix may not have public trades endpoint"""
        # Bitunix doesn't seem to have a public trades endpoint in the provided API docs
        # Return empty for now - can be implemented if endpoint is available
        logger.debug(f"fetch_trades not fully implemented for Bitunix - symbol: {symbol}")
        return []
    
    async def fetch_ticker(self, symbol: str, params: dict | None = None) -> dict:
        """Fetch single ticker"""
        tickers = await self.fetch_tickers([symbol], params)
        return tickers.get(symbol, {})
    
    async def fetch_tickers(
        self,
        symbols: list[str] | None = None,
        params: dict | None = None
    ) -> dict[str, dict]:
        """Fetch tickers from Bitunix"""
        url = f"{self._parent._BASE_URL}/api/v1/futures/market/tickers"
        
        request_params = {}
        if symbols:
            bitunix_symbols = ",".join(self._symbol_to_bitunix(s) for s in symbols)
            request_params["symbols"] = bitunix_symbols
        
        data = await self._get_json(url, request_params)
        
        tickers = {}
        if data and isinstance(data, list):
            for ticker_data in data:
                try:
                    bitunix_symbol = ticker_data.get("symbol", "")
                    symbol = self._bitunix_to_symbol(bitunix_symbol)
                    
                    if symbols and symbol not in symbols:
                        continue
                    
                    tickers[symbol] = {
                        "symbol": symbol,
                        "last": float(ticker_data.get("lastPrice", 0)),
                        "high": float(ticker_data.get("high", 0)),
                        "low": float(ticker_data.get("low", 0)),
                        "bid": float(ticker_data.get("bestBid", 0)) if ticker_data.get("bestBid") else None,
                        "ask": float(ticker_data.get("bestAsk", 0)) if ticker_data.get("bestAsk") else None,
                        "baseVolume": float(ticker_data.get("baseVolume", 0)),
                        "quoteVolume": float(ticker_data.get("quoteVolume", 0)),
                        "percentage": float(ticker_data.get("priceChangePercent", 0)),
                        "timestamp": int(ticker_data.get("ts", 0)),
                        "info": ticker_data,
                    }
                except (ValueError, TypeError, KeyError):
                    continue
        
        return tickers
    
    async def fetch_order_book(
        self,
        symbol: str,
        limit: int = 100,
        params: dict | None = None
    ) -> dict:
        """Fetch order book / depth data"""
        bitunix_symbol = self._symbol_to_bitunix(symbol)
        
        url = f"{self._parent._BASE_URL}/api/v1/futures/market/depth"
        request_params = {
            "symbol": bitunix_symbol,
            "limit": str(min(limit, 100))
        }
        
        data = await self._get_json(url, request_params)
        
        bids = []
        asks = []
        
        if data:
            for bid in data.get("bids", []) or []:
                try:
                    bids.append([float(bid[0]), float(bid[1])])
                except (ValueError, TypeError, IndexError):
                    continue
            
            for ask in data.get("asks", []) or []:
                try:
                    asks.append([float(ask[0]), float(ask[1])])
                except (ValueError, TypeError, IndexError):
                    continue
        
        return {
            "symbol": symbol,
            "bids": bids,
            "asks": asks,
            "timestamp": int(datetime.now(tz=UTC).timestamp() * 1000),
            "datetime": datetime.now(tz=UTC).isoformat(),
            "nonce": None,
        }
    
    async def fetch_balance(self, params: dict | None = None) -> dict:
        """Fetch account balance"""
        url = f"{self._parent._BASE_URL}/api/v1/futures/account"
        request_params = {"marginCoin": "USDT"}
        
        data = await self._get_json(url, request_params, auth=True)
        
        balance = {
            "info": data,
            "timestamp": int(datetime.now(tz=UTC).timestamp() * 1000),
            "datetime": datetime.now(tz=UTC).isoformat(),
        }
        
        if data:
            # Parse balance data
            available = float(data.get("available", 0))
            frozen = float(data.get("frozen", 0))
            total = available + frozen
            
            balance["USDT"] = {
                "free": available,
                "used": frozen,
                "total": total,
            }
            balance["free"] = {"USDT": available}
            balance["used"] = {"USDT": frozen}
            balance["total"] = {"USDT": total}
        
        return balance
    
    async def create_order(
        self,
        symbol: str,
        type: str,
        side: str,
        amount: float,
        price: float | None = None,
        params: dict | None = None,
    ) -> dict:
        """Create an order on Bitunix"""
        params = params or {}
        bitunix_symbol = self._symbol_to_bitunix(symbol)
        
        url = f"{self._parent._BASE_URL}/api/v1/futures/trade/place_order"
        
        order_data = {
            "symbol": bitunix_symbol,
            "side": side.upper(),
            "orderType": type.upper(),
            "qty": str(amount),
            "tradeSide": params.get("tradeSide", "OPEN"),
            "effect": params.get("timeInForce", "GTC"),
            "reduceOnly": params.get("reduceOnly", False),
        }
        
        if price is not None and type.upper() == "LIMIT":
            order_data["price"] = str(price)
        
        if params.get("clientOrderId"):
            order_data["clientId"] = params["clientOrderId"]
        
        if params.get("positionId"):
            order_data["positionId"] = params["positionId"]
        
        # Take profit parameters
        if params.get("tpPrice"):
            order_data["tpPrice"] = str(params["tpPrice"])
            order_data["tpStopType"] = params.get("tpStopType", "MARK")
            order_data["tpOrderType"] = params.get("tpOrderType", "MARKET")
        
        result = await self._post_json(url, order_data)
        
        # Convert to ccxt order format
        return {
            "id": result.get("orderId", ""),
            "clientOrderId": result.get("clientId", ""),
            "timestamp": int(datetime.now(tz=UTC).timestamp() * 1000),
            "datetime": datetime.now(tz=UTC).isoformat(),
            "symbol": symbol,
            "type": type.lower(),
            "side": side.lower(),
            "price": price,
            "amount": amount,
            "filled": 0,
            "remaining": amount,
            "status": "open",
            "info": result,
        }
    
    async def cancel_order(
        self,
        id: str,
        symbol: str,
        params: dict | None = None
    ) -> dict:
        """Cancel an order"""
        params = params or {}
        bitunix_symbol = self._symbol_to_bitunix(symbol)
        
        url = f"{self._parent._BASE_URL}/api/v1/futures/trade/cancel_orders"
        
        order_list = [{"orderId": id}]
        if params.get("clientOrderId"):
            order_list = [{"clientId": params["clientOrderId"]}]
        
        cancel_data = {
            "symbol": bitunix_symbol,
            "orderList": order_list
        }
        
        result = await self._post_json(url, cancel_data)
        
        return {
            "id": id,
            "symbol": symbol,
            "status": "canceled",
            "info": result,
        }
    
    async def batch_order(
        self,
        symbol: str,
        orders: list[dict],
        params: dict | None = None
    ) -> dict:
        """Place multiple orders at once (max 5)
        
        API: POST /api/v1/futures/trade/batch_order
        
        :param symbol: Trading pair
        :param orders: List of order dicts with keys:
            - qty: Amount (required)
            - price: Price (required for LIMIT)
            - side: BUY or SELL (required)
            - tradeSide: OPEN or CLOSE (required)
            - orderType: LIMIT or MARKET (required)
            - positionId: Position ID (required for CLOSE)
            - effect: GTC, IOC, FOK, POST_ONLY
            - clientId: Custom order ID
            - reduceOnly: bool
            - tpPrice, tpStopType, tpOrderType, tpOrderPrice: Take profit params
            - slPrice, slStopType, slOrderType, slOrderPrice: Stop loss params
        """
        url = f"{self._parent._BASE_URL}/api/v1/futures/trade/batch_order"
        
        bitunix_symbol = self._symbol_to_bitunix(symbol)
        
        # Validate max 5 orders
        if len(orders) > 5:
            raise InvalidOrderException("Batch order supports maximum 5 orders")
        
        order_list = []
        for order in orders:
            order_data = {
                "qty": str(order.get("qty", order.get("amount", 0))),
                "side": order.get("side", "BUY").upper(),
                "tradeSide": order.get("tradeSide", "OPEN").upper(),
                "orderType": order.get("orderType", order.get("type", "LIMIT")).upper(),
            }
            
            if order.get("price"):
                order_data["price"] = str(order["price"])
            if order.get("positionId"):
                order_data["positionId"] = order["positionId"]
            if order.get("effect"):
                order_data["effect"] = order["effect"]
            if order.get("clientId"):
                order_data["clientId"] = order["clientId"]
            if order.get("reduceOnly") is not None:
                order_data["reduceOnly"] = order["reduceOnly"]
            
            # TP/SL params
            for key in ["tpPrice", "tpStopType", "tpOrderType", "tpOrderPrice",
                        "slPrice", "slStopType", "slOrderType", "slOrderPrice"]:
                if order.get(key):
                    order_data[key] = str(order[key])
            
            order_list.append(order_data)
        
        data = {
            "symbol": bitunix_symbol,
            "orderList": order_list,
        }
        
        result = await self._post_json(url, data)
        
        return {
            "symbol": symbol,
            "successList": result.get("successList", []),
            "failureList": result.get("failureList", []),
            "info": result,
        }
    
    async def cancel_all_orders(
        self,
        symbol: str | None = None,
        params: dict | None = None
    ) -> dict:
        """Cancel all orders
        
        API: POST /api/v1/futures/trade/cancel_all_orders
        
        :param symbol: Trading pair (optional, cancels all if not provided)
        """
        url = f"{self._parent._BASE_URL}/api/v1/futures/trade/cancel_all_orders"
        
        data = {}
        if symbol:
            data["symbol"] = self._symbol_to_bitunix(symbol)
        
        result = await self._post_json(url, data)
        
        return {
            "symbol": symbol,
            "successList": result.get("successList", []),
            "failureList": result.get("failureList", []),
            "info": result,
        }
    
    async def close_all_positions(
        self,
        symbol: str | None = None,
        params: dict | None = None
    ) -> dict:
        """Close all positions
        
        API: POST /api/v1/futures/trade/close_all_position
        
        :param symbol: Trading pair (optional, closes all if not provided)
        """
        url = f"{self._parent._BASE_URL}/api/v1/futures/trade/close_all_position"
        
        data = {}
        if symbol:
            data["symbol"] = self._symbol_to_bitunix(symbol)
        
        result = await self._post_json(url, data)
        
        return {
            "symbol": symbol,
            "status": "closed",
            "info": result,
        }
    
    async def flash_close_position(
        self,
        positionId: str,
        params: dict | None = None
    ) -> dict:
        """Close position by position ID (flash close)
        
        API: POST /api/v1/futures/trade/flash_close_position
        
        :param positionId: Position ID to close
        """
        url = f"{self._parent._BASE_URL}/api/v1/futures/trade/flash_close_position"
        
        data = {
            "positionId": positionId,
        }
        
        result = await self._post_json(url, data)
        
        return {
            "positionId": result.get("positionId", positionId),
            "status": "closed",
            "info": result,
        }
    
    async def fetch_order_detail(
        self,
        orderId: str | None = None,
        clientId: str | None = None,
        params: dict | None = None
    ) -> dict:
        """Get order detail by orderId or clientId
        
        API: GET /api/v1/futures/trade/get_order_detail
        
        :param orderId: Order ID
        :param clientId: Client order ID
        """
        url = f"{self._parent._BASE_URL}/api/v1/futures/trade/get_order_detail"
        
        request_params = {}
        if orderId:
            request_params["orderId"] = orderId
        if clientId:
            request_params["clientId"] = clientId
        
        if not request_params:
            raise InvalidOrderException("Either orderId or clientId is required")
        
        data = await self._get_json(url, request_params, auth=True)
        
        if data:
            return self._parse_order(data)
        raise InvalidOrderException(f"Order not found")
    
    async def fetch_order(
        self,
        id: str,
        symbol: str,
        params: dict | None = None
    ) -> dict:
        """Fetch a single order by ID
        
        API: GET /api/v1/futures/trade/get_order_detail
        """
        params = params or {}
        return await self.fetch_order_detail(
            orderId=id,
            clientId=params.get("clientOrderId"),
            params=params
        )
    
    async def modify_order(
        self,
        orderId: str | None = None,
        clientId: str | None = None,
        qty: str | None = None,
        price: str | None = None,
        tpPrice: str | None = None,
        tpStopType: str | None = None,
        tpOrderType: str | None = None,
        tpOrderPrice: str | None = None,
        slPrice: str | None = None,
        slStopType: str | None = None,
        slOrderType: str | None = None,
        slOrderPrice: str | None = None,
        params: dict | None = None
    ) -> dict:
        """Modify a pending order
        
        API: POST /api/v1/futures/trade/modify_order
        
        :param orderId: Order ID (required if no clientId)
        :param clientId: Client order ID (required if no orderId)
        :param qty: New quantity
        :param price: New price
        :param tpPrice: Take profit trigger price
        :param tpStopType: TP trigger type (MARK_PRICE/LAST_PRICE)
        :param tpOrderType: TP order type (LIMIT/MARKET)
        :param tpOrderPrice: TP order price (for LIMIT)
        :param slPrice: Stop loss trigger price
        :param slStopType: SL trigger type (MARK_PRICE/LAST_PRICE)
        :param slOrderType: SL order type (LIMIT/MARKET)
        :param slOrderPrice: SL order price (for LIMIT)
        """
        url = f"{self._parent._BASE_URL}/api/v1/futures/trade/modify_order"
        
        if not orderId and not clientId:
            raise InvalidOrderException("Either orderId or clientId is required")
        
        data = {}
        if orderId:
            data["orderId"] = orderId
        if clientId:
            data["clientId"] = clientId
        if qty:
            data["qty"] = str(qty)
        if price:
            data["price"] = str(price)
        
        # TP params
        if tpPrice:
            data["tpPrice"] = str(tpPrice)
        if tpStopType:
            data["tpStopType"] = tpStopType
        if tpOrderType:
            data["tpOrderType"] = tpOrderType
        if tpOrderPrice:
            data["tpOrderPrice"] = str(tpOrderPrice)
        
        # SL params
        if slPrice:
            data["slPrice"] = str(slPrice)
        if slStopType:
            data["slStopType"] = slStopType
        if slOrderType:
            data["slOrderType"] = slOrderType
        if slOrderPrice:
            data["slOrderPrice"] = str(slOrderPrice)
        
        result = await self._post_json(url, data)
        
        return {
            "orderId": result.get("orderId", orderId),
            "clientId": result.get("clientId", clientId),
            "info": result,
        }
    
    async def fetch_history_trades(
        self,
        symbol: str | None = None,
        orderId: str | None = None,
        positionId: str | None = None,
        since: int | None = None,
        until: int | None = None,
        skip: int = 0,
        limit: int = 10,
        params: dict | None = None
    ) -> list[dict]:
        """Get history trades
        
        API: GET /api/v1/futures/trade/get_history_trades
        
        :param symbol: Trading pair
        :param orderId: Order ID
        :param positionId: Position ID
        :param since: Start timestamp (ms)
        :param until: End timestamp (ms)
        :param skip: Number of records to skip
        :param limit: Max records (max 100)
        """
        url = f"{self._parent._BASE_URL}/api/v1/futures/trade/get_history_trades"
        
        request_params = {
            "skip": str(skip),
            "limit": str(min(limit, 100)),
        }
        
        if symbol:
            request_params["symbol"] = self._symbol_to_bitunix(symbol)
        if orderId:
            request_params["orderId"] = orderId
        if positionId:
            request_params["positionId"] = positionId
        if since:
            request_params["startTime"] = str(since)
        if until:
            request_params["endTime"] = str(until)
        
        data = await self._get_json(url, request_params, auth=True)
        
        trades = []
        if data and isinstance(data, dict):
            trade_list = data.get("tradeList", [])
            for trade_data in trade_list:
                try:
                    trades.append(self._parse_trade(trade_data))
                except Exception as e:
                    logger.warning(f"Failed to parse trade: {e}")
        
        return trades
    
    def _parse_trade(self, trade_data: dict) -> dict:
        """Parse Bitunix trade to ccxt format"""
        bitunix_symbol = trade_data.get("symbol", "")
        symbol = self._bitunix_to_symbol(bitunix_symbol)
        
        return {
            "id": trade_data.get("tradeId", ""),
            "orderId": trade_data.get("orderId", ""),
            "symbol": symbol,
            "timestamp": int(trade_data.get("ctime", 0)),
            "datetime": datetime.fromtimestamp(
                int(trade_data.get("ctime", 0)) / 1000, tz=UTC
            ).isoformat() if trade_data.get("ctime") else None,
            "type": trade_data.get("orderType", "").lower(),
            "side": trade_data.get("side", "").lower(),
            "price": float(trade_data.get("price", 0)),
            "amount": float(trade_data.get("qty", 0)),
            "cost": float(trade_data.get("price", 0)) * float(trade_data.get("qty", 0)),
            "fee": {
                "cost": float(trade_data.get("fee", 0)),
                "currency": "USDT",
            },
            "realizedPnl": float(trade_data.get("realizedPNL", 0)),
            "takerOrMaker": trade_data.get("roleType", "TAKER").lower(),
            "leverage": int(trade_data.get("leverage", 1)),
            "marginMode": trade_data.get("marginMode", "ISOLATION").lower(),
            "positionMode": trade_data.get("positionMode", "HEDGE"),
            "reduceOnly": trade_data.get("reduceOnly", False),
            "info": trade_data,
        }
    
    async def fetch_orders(
        self,
        symbol: str | None = None,
        since: int | None = None,
        limit: int | None = None,
        params: dict | None = None
    ) -> list[dict]:
        """Fetch orders from history"""
        url = f"{self._parent._BASE_URL}/api/v1/futures/trade/get_history_orders"
        
        request_params = {}
        if symbol:
            request_params["symbol"] = self._symbol_to_bitunix(symbol)
        
        data = await self._get_json(url, request_params, auth=True)
        
        orders = []
        if data and isinstance(data, list):
            for order_data in data:
                try:
                    orders.append(self._parse_order(order_data))
                except Exception as e:
                    logger.warning(f"Failed to parse order: {e}")
                    continue
        
        return orders
    
    async def fetch_open_orders(
        self,
        symbol: str | None = None,
        since: int | None = None,
        limit: int | None = None,
        params: dict | None = None
    ) -> list[dict]:
        """Fetch open orders"""
        url = f"{self._parent._BASE_URL}/api/v1/futures/trade/get_pending_orders"
        
        request_params = {}
        if symbol:
            request_params["symbol"] = self._symbol_to_bitunix(symbol)
        
        data = await self._get_json(url, request_params, auth=True)
        
        orders = []
        if data and isinstance(data, list):
            for order_data in data:
                try:
                    orders.append(self._parse_order(order_data))
                except Exception as e:
                    logger.warning(f"Failed to parse open order: {e}")
                    continue
        
        return orders
    
    async def fetch_positions(
        self,
        symbols: list[str] | None = None,
        params: dict | None = None
    ) -> list[dict]:
        """Fetch open positions"""
        url = f"{self._parent._BASE_URL}/api/v1/futures/position/get_pending_positions"
        
        request_params = {}
        if symbols and len(symbols) == 1:
            request_params["symbol"] = self._symbol_to_bitunix(symbols[0])
        
        data = await self._get_json(url, request_params, auth=True)
        
        positions = []
        if data and isinstance(data, list):
            for pos_data in data:
                try:
                    positions.append(self._parse_position(pos_data))
                except Exception as e:
                    logger.warning(f"Failed to parse position: {e}")
                    continue
        
        return positions
    
    def _parse_order(self, order_data: dict) -> dict:
        """Parse Bitunix order to ccxt format"""
        bitunix_symbol = order_data.get("symbol", "")
        symbol = self._bitunix_to_symbol(bitunix_symbol)
        
        status_map = {
            "NEW": "open",
            "PARTIALLY_FILLED": "open",
            "FILLED": "closed",
            "CANCELED": "canceled",
            "REJECTED": "rejected",
            "EXPIRED": "expired",
        }
        
        return {
            "id": order_data.get("orderId", ""),
            "clientOrderId": order_data.get("clientId", ""),
            "timestamp": int(order_data.get("cTime", 0)),
            "datetime": datetime.fromtimestamp(
                int(order_data.get("cTime", 0)) / 1000, tz=UTC
            ).isoformat() if order_data.get("cTime") else None,
            "symbol": symbol,
            "type": order_data.get("orderType", "").lower(),
            "side": order_data.get("side", "").lower(),
            "price": float(order_data.get("price", 0)) if order_data.get("price") else None,
            "average": float(order_data.get("avgPrice", 0)) if order_data.get("avgPrice") else None,
            "amount": float(order_data.get("qty", 0)),
            "filled": float(order_data.get("filledQty", 0)),
            "remaining": float(order_data.get("qty", 0)) - float(order_data.get("filledQty", 0)),
            "status": status_map.get(order_data.get("status", ""), "unknown"),
            "fee": {
                "cost": float(order_data.get("fee", 0)),
                "currency": "USDT",
            } if order_data.get("fee") else None,
            "info": order_data,
        }
    
    def _parse_position(self, pos_data: dict) -> dict:
        """Parse Bitunix position to ccxt format"""
        bitunix_symbol = pos_data.get("symbol", "")
        symbol = self._bitunix_to_symbol(bitunix_symbol)
        
        side = pos_data.get("side", "").lower()
        is_short = side == "short" or side == "sell"
        
        return {
            "id": pos_data.get("positionId", ""),
            "symbol": symbol,
            "timestamp": int(pos_data.get("cTime", 0)),
            "datetime": datetime.fromtimestamp(
                int(pos_data.get("cTime", 0)) / 1000, tz=UTC
            ).isoformat() if pos_data.get("cTime") else None,
            "contracts": float(pos_data.get("qty", 0)),
            "contractSize": 1.0,
            "side": "short" if is_short else "long",
            "notional": float(pos_data.get("margin", 0)),
            "leverage": float(pos_data.get("leverage", 1)),
            "unrealizedPnl": float(pos_data.get("unrealizedPnl", 0)),
            "realizedPnl": float(pos_data.get("realizedPnl", 0)),
            "percentage": float(pos_data.get("pnlRate", 0)) * 100 if pos_data.get("pnlRate") else None,
            "entryPrice": float(pos_data.get("avgPrice", 0)),
            "markPrice": float(pos_data.get("markPrice", 0)),
            "liquidationPrice": float(pos_data.get("liqPrice", 0)) if pos_data.get("liqPrice") else None,
            "marginMode": pos_data.get("marginMode", "cross").lower(),
            "marginType": pos_data.get("marginMode", "cross").lower(),
            "maintenanceMargin": float(pos_data.get("maintMargin", 0)),
            "initialMargin": float(pos_data.get("margin", 0)),
            "info": pos_data,
        }
    
    async def set_leverage(
        self,
        leverage: int,
        symbol: str,
        params: dict | None = None
    ) -> dict:
        """Set leverage for a symbol
        
        API: POST /api/v1/futures/account/change_leverage
        """
        url = f"{self._parent._BASE_URL}/api/v1/futures/account/change_leverage"
        
        bitunix_symbol = self._symbol_to_bitunix(symbol)
        
        data = {
            "symbol": bitunix_symbol,
            "leverage": int(leverage),
            "marginCoin": "USDT",
        }
        
        result = await self._post_json(url, data)
        return {"leverage": leverage, "symbol": symbol, "info": result}
    
    async def set_margin_mode(
        self,
        marginMode: str,
        symbol: str,
        params: dict | None = None
    ) -> dict:
        """Set margin mode for a symbol
        
        API: POST /api/v1/futures/account/change_margin_mode
        Note: Cannot be used when user has open positions or orders
        """
        url = f"{self._parent._BASE_URL}/api/v1/futures/account/change_margin_mode"
        
        bitunix_symbol = self._symbol_to_bitunix(symbol)
        
        # Map margin mode values
        margin_mode_map = {
            "cross": "CROSS",
            "isolated": "ISOLATION",
            "CROSS": "CROSS",
            "ISOLATED": "ISOLATION",
            "ISOLATION": "ISOLATION",
        }
        bitunix_margin_mode = margin_mode_map.get(marginMode.upper(), "ISOLATION")
        
        data = {
            "symbol": bitunix_symbol,
            "marginMode": bitunix_margin_mode,
            "marginCoin": "USDT",
        }
        
        result = await self._post_json(url, data)
        return {"marginMode": marginMode, "symbol": symbol, "info": result}
    
    async def change_position_mode(
        self,
        positionMode: str,
        params: dict | None = None
    ) -> dict:
        """Change position mode between one-way and hedge mode
        
        API: POST /api/v1/futures/account/change_position_mode
        Note: Cannot be changed when there are open positions or orders
        
        :param positionMode: 'ONE_WAY' or 'HEDGE'
        """
        url = f"{self._parent._BASE_URL}/api/v1/futures/account/change_position_mode"
        
        data = {
            "positionMode": positionMode.upper(),
        }
        
        result = await self._post_json(url, data)
        return {"positionMode": positionMode, "info": result}
    
    async def adjust_position_margin(
        self,
        symbol: str,
        amount: float,
        side: str | None = None,
        positionId: str | None = None,
        params: dict | None = None
    ) -> dict:
        """Add or reduce position margin (isolated margin mode only)
        
        API: POST /api/v1/futures/account/adjust_position_margin
        
        :param symbol: Trading pair
        :param amount: Margin amount (positive=increase, negative=decrease)
        :param side: Position side ('LONG' or 'SHORT'), required if no positionId
        :param positionId: Position ID, required if no side
        """
        url = f"{self._parent._BASE_URL}/api/v1/futures/account/adjust_position_margin"
        
        bitunix_symbol = self._symbol_to_bitunix(symbol)
        
        data = {
            "symbol": bitunix_symbol,
            "marginCoin": "USDT",
            "amount": str(amount),
        }
        
        if side:
            data["side"] = side.upper()
        if positionId:
            data["positionId"] = positionId
        
        result = await self._post_json(url, data)
        return {"symbol": symbol, "amount": amount, "info": result}
    
    async def get_leverage_margin_mode(
        self,
        symbol: str,
        params: dict | None = None
    ) -> dict:
        """Get leverage and margin mode for a symbol
        
        API: GET /api/v1/futures/account/get_leverage_margin_mode
        """
        url = f"{self._parent._BASE_URL}/api/v1/futures/account/get_leverage_margin_mode"
        
        bitunix_symbol = self._symbol_to_bitunix(symbol)
        
        request_params = {
            "symbol": bitunix_symbol,
            "marginCoin": "USDT",
        }
        
        data = await self._get_json(url, request_params, auth=True)
        
        return {
            "symbol": symbol,
            "leverage": int(data.get("leverage", 1)) if data else 1,
            "marginMode": data.get("marginMode", "ISOLATION") if data else "ISOLATION",
            "info": data,
        }
    
    async def fetch_funding_rate(
        self,
        symbol: str,
        params: dict | None = None
    ) -> dict:
        """Get current funding rate for a symbol
        
        API: GET /api/v1/futures/market/funding_rate
        """
        url = f"{self._parent._BASE_URL}/api/v1/futures/market/funding_rate"
        
        bitunix_symbol = self._symbol_to_bitunix(symbol)
        
        request_params = {
            "symbol": bitunix_symbol,
        }
        
        data = await self._get_json(url, request_params)
        
        if data and isinstance(data, list) and len(data) > 0:
            rate_data = data[0]
            return {
                "symbol": symbol,
                "markPrice": float(rate_data.get("markPrice", 0)),
                "lastPrice": float(rate_data.get("lastPrice", 0)),
                "fundingRate": float(rate_data.get("fundingRate", 0)),
                "timestamp": int(datetime.now(tz=UTC).timestamp() * 1000),
                "info": rate_data,
            }
        return {"symbol": symbol, "fundingRate": 0, "info": data}
    
    async def fetch_history_positions(
        self,
        symbol: str | None = None,
        positionId: str | None = None,
        since: int | None = None,
        limit: int | None = None,
        params: dict | None = None
    ) -> list[dict]:
        """Get historical positions
        
        API: GET /api/v1/futures/position/get_history_positions
        """
        url = f"{self._parent._BASE_URL}/api/v1/futures/position/get_history_positions"
        
        request_params = {}
        if symbol:
            request_params["symbol"] = self._symbol_to_bitunix(symbol)
        if positionId:
            request_params["positionId"] = positionId
        if since:
            request_params["startTime"] = str(since)
        if limit:
            request_params["limit"] = str(min(limit, 100))
        
        data = await self._get_json(url, request_params, auth=True)
        
        positions = []
        if data and isinstance(data, dict):
            position_list = data.get("positionList", [])
            for pos_data in position_list:
                try:
                    positions.append(self._parse_history_position(pos_data))
                except Exception as e:
                    logger.warning(f"Failed to parse history position: {e}")
        
        return positions
    
    def _parse_history_position(self, pos_data: dict) -> dict:
        """Parse Bitunix history position to ccxt format"""
        bitunix_symbol = pos_data.get("symbol", "")
        symbol = self._bitunix_to_symbol(bitunix_symbol)
        
        side = pos_data.get("side", "").lower()
        is_short = side == "short"
        
        return {
            "id": pos_data.get("positionId", ""),
            "symbol": symbol,
            "timestamp": int(pos_data.get("cTime", 0)),
            "datetime": datetime.fromtimestamp(
                int(pos_data.get("cTime", 0)) / 1000, tz=UTC
            ).isoformat() if pos_data.get("cTime") else None,
            "side": "short" if is_short else "long",
            "contracts": float(pos_data.get("maxQty", 0)),
            "entryPrice": float(pos_data.get("entryPrice", 0)),
            "closePrice": float(pos_data.get("closePrice", 0)),
            "liqQty": float(pos_data.get("liqQty", 0)),
            "leverage": int(pos_data.get("leverage", 1)),
            "marginMode": pos_data.get("marginMode", "ISOLATION").lower(),
            "positionMode": pos_data.get("positionMode", "HEDGE"),
            "fee": float(pos_data.get("fee", 0)),
            "funding": float(pos_data.get("funding", 0)),
            "realizedPnl": float(pos_data.get("realizedPNL", 0)),
            "liquidationPrice": float(pos_data.get("liqPrice", 0)) if pos_data.get("liqPrice") else None,
            "info": pos_data,
        }
    
    async def fetch_position_tiers(
        self,
        symbol: str,
        params: dict | None = None
    ) -> list[dict]:
        """Get position tiers (leverage tiers) for a symbol
        
        API: GET /api/v1/futures/position/get_position_tiers
        """
        url = f"{self._parent._BASE_URL}/api/v1/futures/position/get_position_tiers"
        
        bitunix_symbol = self._symbol_to_bitunix(symbol)
        
        request_params = {
            "symbol": bitunix_symbol,
        }
        
        data = await self._get_json(url, request_params)
        
        tiers = []
        if data and isinstance(data, list):
            for tier_data in data:
                try:
                    tiers.append({
                        "symbol": symbol,
                        "tier": int(tier_data.get("level", 0)),
                        "minNotional": float(tier_data.get("startValue", 0)),
                        "maxNotional": float(tier_data.get("endValue", 0)),
                        "maxLeverage": int(tier_data.get("leverage", 1)),
                        "maintenanceMarginRate": float(tier_data.get("maintenanceMarginRate", 0)),
                        "info": tier_data,
                    })
                except Exception as e:
                    logger.warning(f"Failed to parse position tier: {e}")
        
        return tiers
    
    # TP/SL Order Methods
    
    async def place_tpsl_order(
        self,
        symbol: str,
        positionId: str,
        tpPrice: str | None = None,
        slPrice: str | None = None,
        tpStopType: str = "MARK_PRICE",
        slStopType: str = "MARK_PRICE",
        tpOrderType: str = "MARKET",
        slOrderType: str = "MARKET",
        tpOrderPrice: str | None = None,
        slOrderPrice: str | None = None,
        tpQty: str | None = None,
        slQty: str | None = None,
        params: dict | None = None
    ) -> dict:
        """Place TP/SL order
        
        API: POST /api/v1/futures/tpsl/place_order
        
        :param symbol: Trading pair
        :param positionId: Position ID
        :param tpPrice: Take-profit trigger price
        :param slPrice: Stop-loss trigger price
        :param tpStopType: TP trigger type (LAST_PRICE or MARK_PRICE)
        :param slStopType: SL trigger type (LAST_PRICE or MARK_PRICE)
        :param tpOrderType: TP order type (LIMIT or MARKET)
        :param slOrderType: SL order type (LIMIT or MARKET)
        :param tpOrderPrice: TP order price (for LIMIT)
        :param slOrderPrice: SL order price (for LIMIT)
        :param tpQty: TP quantity
        :param slQty: SL quantity
        """
        url = f"{self._parent._BASE_URL}/api/v1/futures/tpsl/place_order"
        
        bitunix_symbol = self._symbol_to_bitunix(symbol)
        
        data = {
            "symbol": bitunix_symbol,
            "positionId": positionId,
        }
        
        if tpPrice:
            data["tpPrice"] = tpPrice
            data["tpStopType"] = tpStopType
            data["tpOrderType"] = tpOrderType
            if tpOrderPrice:
                data["tpOrderPrice"] = tpOrderPrice
            if tpQty:
                data["tpQty"] = tpQty
        
        if slPrice:
            data["slPrice"] = slPrice
            data["slStopType"] = slStopType
            data["slOrderType"] = slOrderType
            if slOrderPrice:
                data["slOrderPrice"] = slOrderPrice
            if slQty:
                data["slQty"] = slQty
        
        result = await self._post_json(url, data)
        return {
            "orderId": result.get("orderId", ""),
            "symbol": symbol,
            "info": result,
        }
    
    async def place_position_tpsl_order(
        self,
        symbol: str,
        positionId: str,
        tpPrice: str | None = None,
        slPrice: str | None = None,
        tpStopType: str = "MARK_PRICE",
        slStopType: str = "MARK_PRICE",
        params: dict | None = None
    ) -> dict:
        """Place position TP/SL order (closes at market price based on position qty)
        
        API: POST /api/v1/futures/tpsl/position/place_order
        Each position can only have one Position TP/SL Order
        
        :param symbol: Trading pair
        :param positionId: Position ID
        :param tpPrice: Take-profit trigger price
        :param slPrice: Stop-loss trigger price
        :param tpStopType: TP trigger type (LAST_PRICE or MARK_PRICE)
        :param slStopType: SL trigger type (LAST_PRICE or MARK_PRICE)
        """
        url = f"{self._parent._BASE_URL}/api/v1/futures/tpsl/position/place_order"
        
        bitunix_symbol = self._symbol_to_bitunix(symbol)
        
        data = {
            "symbol": bitunix_symbol,
            "positionId": positionId,
        }
        
        if tpPrice:
            data["tpPrice"] = tpPrice
            data["tpStopType"] = tpStopType
        
        if slPrice:
            data["slPrice"] = slPrice
            data["slStopType"] = slStopType
        
        result = await self._post_json(url, data)
        return {
            "orderId": result.get("orderId", ""),
            "symbol": symbol,
            "info": result,
        }
    
    async def modify_tpsl_order(
        self,
        orderId: str,
        tpPrice: str | None = None,
        slPrice: str | None = None,
        tpStopType: str | None = None,
        slStopType: str | None = None,
        tpOrderType: str | None = None,
        slOrderType: str | None = None,
        tpOrderPrice: str | None = None,
        slOrderPrice: str | None = None,
        tpQty: str | None = None,
        slQty: str | None = None,
        params: dict | None = None
    ) -> dict:
        """Modify TP/SL order
        
        API: POST /api/v1/futures/tpsl/modify_order
        """
        url = f"{self._parent._BASE_URL}/api/v1/futures/tpsl/modify_order"
        
        data = {
            "orderId": orderId,
        }
        
        if tpPrice:
            data["tpPrice"] = tpPrice
        if slPrice:
            data["slPrice"] = slPrice
        if tpStopType:
            data["tpStopType"] = tpStopType
        if slStopType:
            data["slStopType"] = slStopType
        if tpOrderType:
            data["tpOrderType"] = tpOrderType
        if slOrderType:
            data["slOrderType"] = slOrderType
        if tpOrderPrice:
            data["tpOrderPrice"] = tpOrderPrice
        if slOrderPrice:
            data["slOrderPrice"] = slOrderPrice
        if tpQty:
            data["tpQty"] = tpQty
        if slQty:
            data["slQty"] = slQty
        
        result = await self._post_json(url, data)
        return {
            "orderId": result.get("orderId", ""),
            "info": result,
        }
    
    async def modify_position_tpsl_order(
        self,
        symbol: str,
        positionId: str,
        tpPrice: str | None = None,
        slPrice: str | None = None,
        tpStopType: str | None = None,
        slStopType: str | None = None,
        params: dict | None = None
    ) -> dict:
        """Modify position TP/SL order
        
        API: POST /api/v1/futures/tpsl/position/modify_order
        """
        url = f"{self._parent._BASE_URL}/api/v1/futures/tpsl/position/modify_order"
        
        bitunix_symbol = self._symbol_to_bitunix(symbol)
        
        data = {
            "symbol": bitunix_symbol,
            "positionId": positionId,
        }
        
        if tpPrice:
            data["tpPrice"] = tpPrice
        if slPrice:
            data["slPrice"] = slPrice
        if tpStopType:
            data["tpStopType"] = tpStopType
        if slStopType:
            data["slStopType"] = slStopType
        
        result = await self._post_json(url, data)
        return {
            "orderId": result.get("orderId", ""),
            "symbol": symbol,
            "info": result,
        }
    
    async def cancel_tpsl_order(
        self,
        symbol: str,
        orderId: str,
        params: dict | None = None
    ) -> dict:
        """Cancel TP/SL order
        
        API: POST /api/v1/futures/tpsl/cancel_order
        """
        url = f"{self._parent._BASE_URL}/api/v1/futures/tpsl/cancel_order"
        
        bitunix_symbol = self._symbol_to_bitunix(symbol)
        
        data = {
            "symbol": bitunix_symbol,
            "orderId": orderId,
        }
        
        result = await self._post_json(url, data)
        return {
            "orderId": result.get("orderId", ""),
            "symbol": symbol,
            "status": "canceled",
            "info": result,
        }
    
    async def fetch_pending_tpsl_orders(
        self,
        symbol: str | None = None,
        positionId: str | None = None,
        side: int | None = None,
        positionMode: int | None = None,
        skip: int = 0,
        limit: int = 10,
        params: dict | None = None
    ) -> list[dict]:
        """Get pending TP/SL orders
        
        API: GET /api/v1/futures/tpsl/get_pending_orders
        """
        url = f"{self._parent._BASE_URL}/api/v1/futures/tpsl/get_pending_orders"
        
        request_params = {
            "skip": str(skip),
            "limit": str(min(limit, 100)),
        }
        
        if symbol:
            request_params["symbol"] = self._symbol_to_bitunix(symbol)
        if positionId:
            request_params["positionId"] = positionId
        if side is not None:
            request_params["side"] = str(side)
        if positionMode is not None:
            request_params["positionMode"] = str(positionMode)
        
        data = await self._get_json(url, request_params, auth=True)
        
        orders = []
        if data and isinstance(data, list):
            for order_data in data:
                try:
                    orders.append(self._parse_tpsl_order(order_data))
                except Exception as e:
                    logger.warning(f"Failed to parse TP/SL order: {e}")
        
        return orders
    
    async def fetch_history_tpsl_orders(
        self,
        symbol: str | None = None,
        side: int | None = None,
        positionMode: int | None = None,
        since: int | None = None,
        until: int | None = None,
        skip: int = 0,
        limit: int = 10,
        params: dict | None = None
    ) -> list[dict]:
        """Get history TP/SL orders
        
        API: GET /api/v1/futures/tpsl/get_history_orders
        """
        url = f"{self._parent._BASE_URL}/api/v1/futures/tpsl/get_history_orders"
        
        request_params = {
            "skip": str(skip),
            "limit": str(min(limit, 100)),
        }
        
        if symbol:
            request_params["symbol"] = self._symbol_to_bitunix(symbol)
        if side is not None:
            request_params["side"] = str(side)
        if positionMode is not None:
            request_params["positionMode"] = str(positionMode)
        if since:
            request_params["startTime"] = str(since)
        if until:
            request_params["endTime"] = str(until)
        
        data = await self._get_json(url, request_params, auth=True)
        
        orders = []
        if data and isinstance(data, dict):
            order_list = data.get("orderList", [])
            for order_data in order_list:
                try:
                    orders.append(self._parse_tpsl_order(order_data))
                except Exception as e:
                    logger.warning(f"Failed to parse history TP/SL order: {e}")
        
        return orders
    
    def _parse_tpsl_order(self, order_data: dict) -> dict:
        """Parse Bitunix TP/SL order to standard format"""
        bitunix_symbol = order_data.get("symbol", "")
        symbol = self._bitunix_to_symbol(bitunix_symbol)
        
        return {
            "id": order_data.get("id", ""),
            "positionId": order_data.get("positionId", ""),
            "symbol": symbol,
            "base": order_data.get("base", ""),
            "quote": order_data.get("quote", ""),
            "tpPrice": order_data.get("tpPrice"),
            "tpStopType": order_data.get("tpStopType"),
            "slPrice": order_data.get("slPrice"),
            "slStopType": order_data.get("slStopType"),
            "tpOrderType": order_data.get("tpOrderType"),
            "tpOrderPrice": order_data.get("tpOrderPrice"),
            "slOrderType": order_data.get("slOrderType"),
            "slOrderPrice": order_data.get("slOrderPrice"),
            "tpQty": order_data.get("tpQty"),
            "slQty": order_data.get("slQty"),
            "status": order_data.get("status"),
            "timestamp": int(order_data.get("ctime", 0)),
            "triggerTime": int(order_data.get("triggerTime", 0)) if order_data.get("triggerTime") else None,
            "info": order_data,
        }
    
    def fetch_status(self):
        """Return exchange status"""
        return {"status": "ok", "updated": None}
    
    def calculate_fee(
        self,
        symbol: str,
        type: str,
        side: str,
        amount: float,
        price: float,
        takerOrMaker: str = "taker",
        params: dict | None = None,
    ) -> dict:
        """Calculate trading fee"""
        # Bitunix typical fees - these should be fetched from API if available
        fee_rates = {
            "taker": 0.0006,  # 0.06%
            "maker": 0.0002,  # 0.02%
        }
        fee_rate = fee_rates.get(takerOrMaker, 0.0006)
        
        return {
            "type": takerOrMaker,
            "currency": "USDT",
            "rate": fee_rate,
            "cost": amount * price * fee_rate if price else 0,
        }
    
    # ==================== WebSocket Methods ====================
    
    async def _ensure_ws(self) -> BitunixWebSocket:
        """Ensure WebSocket handler is initialized"""
        if self._ws is None:
            self._ws = BitunixWebSocket(
                api_key=self._parent._api_key,
                api_secret=self._parent._api_secret,
            )
        return self._ws
    
    async def watch_ticker(self, symbol: str, params: dict | None = None) -> dict:
        """Watch ticker updates for a single symbol via WebSocket
        
        Uses the 'ticker' channel (singular) for individual symbol subscription.
        
        :param symbol: Trading pair (e.g., 'BTC/USDT:USDT')
        :returns: Ticker dict with last, high, low, open, volume, etc.
        """
        ws = await self._ensure_ws()
        
        if not ws._public_ws:
            await ws.connect_public()
            asyncio.create_task(ws.listen_public())
        
        # Use individual ticker subscription (ticker channel, not tickers)
        await ws.subscribe_ticker(symbol)
        
        # Wait for data with timeout
        bitunix_symbol = symbol.replace("/", "").replace(":USDT", "")
        for _ in range(50):  # 5 second timeout
            ticker = ws.get_ticker(bitunix_symbol)
            if ticker:
                return self._format_ws_ticker(symbol, ticker)
            await asyncio.sleep(0.1)
        
        # Fallback to REST if no WS data
        return await self.fetch_ticker(symbol)
    
    async def watch_tickers(
        self,
        symbols: list[str] | None = None,
        params: dict | None = None
    ) -> dict[str, dict]:
        """Watch ticker updates for multiple symbols via WebSocket
        
        Uses the 'tickers' channel (plural) for batch symbol subscription.
        
        :param symbols: List of trading pairs
        :returns: Dict of symbol -> ticker data
        """
        ws = await self._ensure_ws()
        
        if not ws._public_ws:
            await ws.connect_public()
            asyncio.create_task(ws.listen_public())
        
        symbols = symbols or list(self.markets.keys())[:50]  # Limit to 50 symbols
        await ws.subscribe_tickers(symbols)
        
        # Wait for data with timeout
        for _ in range(50):
            tickers = ws.get_all_tickers()
            if tickers:
                result = {}
                for bitunix_sym, ticker in tickers.items():
                    sym = self._bitunix_to_symbol(bitunix_sym)
                    result[sym] = self._format_ws_ticker(sym, ticker)
                return result
            await asyncio.sleep(0.1)
        
        # Fallback to REST
        return await self.fetch_tickers(symbols)
    
    def _format_ws_ticker(self, symbol: str, ws_ticker: dict) -> dict:
        """Format WebSocket ticker to ccxt format"""
        return {
            "symbol": symbol,
            "timestamp": ws_ticker.get("timestamp", 0),
            "datetime": datetime.fromtimestamp(
                ws_ticker.get("timestamp", 0) / 1000, tz=UTC
            ).isoformat() if ws_ticker.get("timestamp") else None,
            "high": ws_ticker.get("high", 0),
            "low": ws_ticker.get("low", 0),
            "bid": ws_ticker.get("bid", 0),
            "bidVolume": ws_ticker.get("bidVolume"),
            "ask": ws_ticker.get("ask", 0),
            "askVolume": ws_ticker.get("askVolume"),
            "open": ws_ticker.get("open", 0),
            "close": ws_ticker.get("last", 0),
            "last": ws_ticker.get("last", 0),
            "baseVolume": ws_ticker.get("baseVolume", 0),
            "quoteVolume": ws_ticker.get("quoteVolume", 0),
            "percentage": ws_ticker.get("percentage", 0),
            "info": ws_ticker,
        }
    
    async def watch_trades(
        self,
        symbol: str,
        since: int | None = None,
        limit: int | None = None,
        params: dict | None = None
    ) -> list[dict]:
        """Watch public trades via WebSocket
        
        :param symbol: Trading pair
        :returns: List of trade dicts
        """
        ws = await self._ensure_ws()
        
        if not ws._public_ws:
            await ws.connect_public()
            asyncio.create_task(ws.listen_public())
        
        await ws.subscribe_trades(symbol)
        
        # Wait for data with timeout
        bitunix_symbol = symbol.replace("/", "").replace(":USDT", "")
        for _ in range(50):
            trades = ws.get_trades(bitunix_symbol)
            if trades:
                return [self._format_ws_trade(symbol, t) for t in trades]
            await asyncio.sleep(0.1)
        
        return []
    
    def _format_ws_trade(self, symbol: str, ws_trade: dict) -> dict:
        """Format WebSocket trade to ccxt format"""
        return {
            "id": None,
            "symbol": symbol,
            "timestamp": ws_trade.get("timestamp"),
            "datetime": ws_trade.get("timestamp"),
            "side": ws_trade.get("side", ""),
            "price": ws_trade.get("price", 0),
            "amount": ws_trade.get("amount", 0),
            "cost": ws_trade.get("price", 0) * ws_trade.get("amount", 0),
            "info": ws_trade,
        }
    
    async def watch_order_book(
        self,
        symbol: str,
        limit: int | None = None,
        params: dict | None = None
    ) -> dict:
        """Watch order book via WebSocket
        
        :param symbol: Trading pair
        :param limit: Depth level (1, 5, or 15)
        :returns: Order book dict with bids and asks
        """
        ws = await self._ensure_ws()
        
        if not ws._public_ws:
            await ws.connect_public()
            asyncio.create_task(ws.listen_public())
        
        # Determine depth level
        if limit and limit <= 1:
            level = "depth_book1"
        elif limit and limit <= 5:
            level = "depth_book5"
        elif limit and limit <= 15:
            level = "depth_book15"
        else:
            level = "depth_books"
        
        await ws.subscribe_depth(symbol, level)
        
        # Wait for data with timeout
        bitunix_symbol = symbol.replace("/", "").replace(":USDT", "")
        for _ in range(50):
            orderbook = ws.get_orderbook(bitunix_symbol)
            if orderbook:
                return {
                    "symbol": symbol,
                    "bids": orderbook.get("bids", []),
                    "asks": orderbook.get("asks", []),
                    "timestamp": orderbook.get("timestamp", 0),
                    "datetime": datetime.fromtimestamp(
                        orderbook.get("timestamp", 0) / 1000, tz=UTC
                    ).isoformat() if orderbook.get("timestamp") else None,
                    "nonce": None,
                }
            await asyncio.sleep(0.1)
        
        # Fallback to REST
        return await self.fetch_order_book(symbol, limit or 100)
    
    async def watch_balance(self, params: dict | None = None) -> dict:
        """Watch account balance updates via WebSocket (private)
        
        :returns: Balance dict
        """
        ws = await self._ensure_ws()
        
        if not ws._private_ws:
            if not await ws.connect_private():
                # Fallback to REST if WS auth fails
                return await self.fetch_balance()
            asyncio.create_task(ws.listen_private())
        
        await ws.subscribe_balance()
        
        # Wait for data with timeout
        for _ in range(50):
            balances = ws.get_all_balances()
            if balances:
                return self._format_ws_balance(balances)
            await asyncio.sleep(0.1)
        
        # Fallback to REST
        return await self.fetch_balance()
    
    def _format_ws_balance(self, ws_balances: dict) -> dict:
        """Format WebSocket balance to ccxt format"""
        balance = {
            "info": ws_balances,
            "timestamp": int(datetime.now(tz=UTC).timestamp() * 1000),
            "datetime": datetime.now(tz=UTC).isoformat(),
        }
        
        for coin, data in ws_balances.items():
            available = float(data.get("available", 0))
            frozen = float(data.get("frozen", 0))
            total = available + frozen
            
            balance[coin] = {
                "free": available,
                "used": frozen,
                "total": total,
            }
        
        return balance
    
    async def watch_orders(
        self,
        symbol: str | None = None,
        since: int | None = None,
        limit: int | None = None,
        params: dict | None = None
    ) -> list[dict]:
        """Watch order updates via WebSocket (private)
        
        :param symbol: Trading pair (optional filter)
        :returns: List of order dicts
        """
        ws = await self._ensure_ws()
        
        if not ws._private_ws:
            if not await ws.connect_private():
                return await self.fetch_open_orders(symbol)
            asyncio.create_task(ws.listen_private())
        
        await ws.subscribe_orders()
        
        # Wait for data with timeout
        for _ in range(50):
            orders = ws.get_all_orders()
            if orders:
                result = []
                for order in orders.values():
                    if symbol is None or order.get("symbol", "").replace("USDT", "/USDT:USDT") == symbol:
                        result.append(self._format_ws_order(order))
                return result
            await asyncio.sleep(0.1)
        
        return []
    
    def _format_ws_order(self, ws_order: dict) -> dict:
        """Format WebSocket order to ccxt format"""
        bitunix_symbol = ws_order.get("symbol", "")
        symbol = self._bitunix_to_symbol(bitunix_symbol)
        
        status_map = {
            "INIT": "open",
            "NEW": "open",
            "PART_FILLED": "open",
            "FILLED": "closed",
            "CANCELED": "canceled",
            "PART_FILLED_CANCELED": "canceled",
        }
        
        return {
            "id": ws_order.get("orderId", ""),
            "clientOrderId": ws_order.get("clientId"),
            "symbol": symbol,
            "type": ws_order.get("type", ""),
            "side": ws_order.get("side", ""),
            "price": ws_order.get("price"),
            "amount": ws_order.get("amount", 0),
            "filled": ws_order.get("filled", 0),
            "remaining": ws_order.get("amount", 0) - ws_order.get("filled", 0),
            "average": ws_order.get("average"),
            "status": status_map.get(ws_order.get("status", ""), "unknown"),
            "fee": {"cost": ws_order.get("fee", 0), "currency": "USDT"},
            "timestamp": ws_order.get("timestamp", 0),
            "datetime": datetime.fromtimestamp(
                ws_order.get("timestamp", 0) / 1000, tz=UTC
            ).isoformat() if ws_order.get("timestamp") else None,
            "info": ws_order,
        }
    
    async def watch_positions(
        self,
        symbols: list[str] | None = None,
        params: dict | None = None
    ) -> list[dict]:
        """Watch position updates via WebSocket (private)
        
        :param symbols: List of trading pairs (optional filter)
        :returns: List of position dicts
        """
        ws = await self._ensure_ws()
        
        if not ws._private_ws:
            if not await ws.connect_private():
                return await self.fetch_positions(symbols)
            asyncio.create_task(ws.listen_private())
        
        await ws.subscribe_positions()
        
        # Wait for data with timeout
        for _ in range(50):
            positions = ws.get_all_positions()
            if positions:
                result = []
                for pos in positions.values():
                    bitunix_sym = pos.get("symbol", "")
                    sym = self._bitunix_to_symbol(bitunix_sym)
                    if symbols is None or sym in symbols:
                        result.append(self._format_ws_position(pos))
                return result
            await asyncio.sleep(0.1)
        
        return []
    
    def _format_ws_position(self, ws_position: dict) -> dict:
        """Format WebSocket position to ccxt format"""
        bitunix_symbol = ws_position.get("symbol", "")
        symbol = self._bitunix_to_symbol(bitunix_symbol)
        
        side = ws_position.get("side", "").lower()
        is_short = side == "short"
        
        return {
            "id": ws_position.get("positionId", ""),
            "symbol": symbol,
            "contracts": float(ws_position.get("contracts", 0)),
            "contractSize": 1.0,
            "side": "short" if is_short else "long",
            "notional": float(ws_position.get("margin", 0)),
            "leverage": int(ws_position.get("leverage", 1)),
            "unrealizedPnl": float(ws_position.get("unrealizedPnl", 0)),
            "realizedPnl": float(ws_position.get("realizedPnl", 0)),
            "marginMode": ws_position.get("marginMode", "cross"),
            "timestamp": ws_position.get("timestamp", 0),
            "datetime": datetime.fromtimestamp(
                ws_position.get("timestamp", 0) / 1000, tz=UTC
            ).isoformat() if ws_position.get("timestamp") else None,
            "info": ws_position,
        }
    
    async def watch_funding_rate(
        self,
        symbol: str,
        params: dict | None = None
    ) -> dict:
        """Watch funding rate via price channel (public)
        
        :param symbol: Trading pair
        :returns: Funding rate data with mark price, index price
        """
        ws = await self._ensure_ws()
        
        if not ws._public_ws:
            await ws.connect_public()
            asyncio.create_task(ws.listen_public())
        
        await ws.subscribe_price(symbol)
        
        # Wait for data with timeout
        bitunix_symbol = symbol.replace("/", "").replace(":USDT", "")
        for _ in range(50):
            price_data = ws.get_price(bitunix_symbol)
            if price_data:
                return {
                    "symbol": symbol,
                    "markPrice": price_data.get("markPrice", 0),
                    "indexPrice": price_data.get("indexPrice", 0),
                    "fundingRate": price_data.get("fundingRate", 0),
                    "fundingTimestamp": price_data.get("fundingTime"),
                    "nextFundingTimestamp": price_data.get("nextFundingTime"),
                    "timestamp": price_data.get("timestamp", 0),
                    "info": price_data,
                }
            await asyncio.sleep(0.1)
        
        # Fallback to REST
        return await self.fetch_funding_rate(symbol)
    
    async def watch_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1m",
        since: int | None = None,
        limit: int | None = None,
        params: dict | None = None
    ) -> list[list]:
        """Watch OHLCV/candlestick data via WebSocket
        
        :param symbol: Trading pair (e.g., 'BTC/USDT:USDT')
        :param timeframe: Candle interval (1m, 5m, 15m, 1h, etc.)
        :returns: List of candles [[timestamp, open, high, low, close, volume], ...]
        """
        ws = await self._ensure_ws()
        
        if not ws._public_ws:
            await ws.connect_public()
            asyncio.create_task(ws.listen_public())
        
        await ws.subscribe_kline(symbol, timeframe)
        
        # Map timeframe to Bitunix interval format
        interval_map = {
            "1m": "1min", "3m": "3min", "5m": "5min",
            "15m": "15min", "30m": "30min",
            "1h": "1h", "2h": "2h", "4h": "4h", "6h": "6h", "8h": "8h", "12h": "12h",
            "1d": "1day", "3d": "3day", "1w": "1week", "1M": "1month",
        }
        interval = interval_map.get(timeframe, "1min")
        
        # Wait for data with timeout
        bitunix_symbol = symbol.replace("/", "").replace(":USDT", "")
        for _ in range(50):  # 5 second timeout
            candles = ws.get_ohlcv(bitunix_symbol, interval)
            if candles:
                # Convert to ccxt format [[timestamp, open, high, low, close, volume], ...]
                result = []
                for c in candles:
                    result.append([
                        c["timestamp"],
                        c["open"],
                        c["high"],
                        c["low"],
                        c["close"],
                        c["volume"],
                    ])
                return result
            await asyncio.sleep(0.1)
        
        # Fallback to REST
        return await self.fetch_ohlcv(symbol, timeframe, since, limit)
    
    async def watch_ohlcv_for_symbols(
        self,
        pairs_tf: list[tuple[str, str]],
        since: int | None = None,
        limit: int | None = None,
        params: dict | None = None
    ) -> dict[tuple[str, str], list[list]]:
        """Watch OHLCV data for multiple symbol/timeframe pairs
        
        :param pairs_tf: List of (symbol, timeframe) tuples
        :returns: Dict of (symbol, timeframe) -> candles
        """
        ws = await self._ensure_ws()
        
        if not ws._public_ws:
            await ws.connect_public()
            asyncio.create_task(ws.listen_public())
        
        # Subscribe to all pairs
        for symbol, timeframe in pairs_tf:
            await ws.subscribe_kline(symbol, timeframe)
        
        # Wait briefly for data
        await asyncio.sleep(1.0)
        
        # Collect results
        interval_map = {
            "1m": "1min", "3m": "3min", "5m": "5min",
            "15m": "15min", "30m": "30min",
            "1h": "1h", "2h": "2h", "4h": "4h", "6h": "6h", "8h": "8h", "12h": "12h",
            "1d": "1day", "3d": "3day", "1w": "1week", "1M": "1month",
        }
        
        result = {}
        for symbol, timeframe in pairs_tf:
            bitunix_symbol = symbol.replace("/", "").replace(":USDT", "")
            interval = interval_map.get(timeframe, "1min")
            candles = ws.get_ohlcv(bitunix_symbol, interval)
            
            if candles:
                result[(symbol, timeframe)] = [
                    [c["timestamp"], c["open"], c["high"], c["low"], c["close"], c["volume"]]
                    for c in candles
                ]
            else:
                result[(symbol, timeframe)] = []
        
        return result
    
    async def un_watch_ohlcv_for_symbols(self, pairs_tf: list[list]) -> None:
        """Unsubscribe from OHLCV WebSocket channels
        
        :param pairs_tf: List of [symbol, timeframe] pairs to unsubscribe
        """
        if self._ws is None:
            return
        
        for pair_tf in pairs_tf:
            symbol, timeframe = pair_tf[0], pair_tf[1]
            await self._ws.unsubscribe_kline(symbol, timeframe)
    
    async def close(self):
        """Close all connections"""
        if self._ws is not None:
            await self._ws.close()
            self._ws = None
        
        if self.session is not None:
            await self.session.close()
            self.session = None


class Bitunix(Exchange):
    """
    Bitunix exchange implementation.
    Bitunix is a cryptocurrency derivatives exchange not supported by CCXT.
    This class provides a ccxt-like wrapper for integration with VulcanTrader.
    """
    
    trader_has: TraderHas = {
        "stoploss_on_exchange": False,  # Can be enabled once stop order API is tested
        "stop_price_param": "stopPrice",
        "stop_price_prop": "stopPrice",
        "stoploss_order_types": {"limit": "stop_limit", "market": "stop_market"},
        "stoploss_blocks_assets": False,
        "order_time_in_force": ["GTC", "IOC", "FOK", "POST_ONLY"],
        "trades_pagination": "time",
        "trades_pagination_arg": "since",
        "trades_has_history": False,
        "l2_limit_range": [1, 5, 15, 100],  # book1, book5, book15, books
        "l2_limit_range_required": True,
        "ws_enabled": True,  # WebSocket support enabled
        "ohlcv_has_history": True,
        "ohlcv_partial_candle": True,
        "ohlcv_require_since": False,
        "ohlcv_candle_limit": 200,
        "download_data_parallel_quick": False,  # Disabled due to strict rate limits
        "tickers_have_quoteVolume": True,
        "tickers_have_percentage": True,
        "tickers_have_bid_ask": True,
        "tickers_have_price": True,
        "funding_fee_timeframe": "8h",
        "mark_ohlcv_timeframe": "1h",
        "needs_trading_fees": False,
        "order_props_in_contracts": ["amount", "filled", "remaining"],
    }
    
    trader_has_futures: TraderHas = {
        "stoploss_order_types": {"limit": "stop", "market": "stop_market"},
        "stoploss_blocks_assets": False,
        "tickers_have_price": True,
        "order_props_in_contracts": ["amount", "cost", "filled", "remaining"],
        "uses_leverage_tiers": False,
    }
    
    _supported_trading_mode_margin_pairs: list[tuple[TradingMode, MarginMode]] = [
        (TradingMode.FUTURES, MarginMode.CROSS),
        (TradingMode.FUTURES, MarginMode.ISOLATED),
    ]
    
    # Bitunix API base URL
    _BASE_URL = "https://fapi.bitunix.com"
    _WS_URL = "wss://fapi.bitunix.com/private"
    _WS_PUBLIC_URL = "wss://fapi.bitunix.com/public"
    
    def __init__(self, config, *args, **kwargs) -> None:
        """Initialize Bitunix exchange connector"""
        # Store API credentials before calling parent init
        exchange_config = config.get("exchange", {})
        self._api_key = exchange_config.get("key", "")
        self._api_secret = exchange_config.get("secret", "")
        
        # Initialize caches
        self._ticker_cache: FtTTLCache = FtTTLCache(maxsize=100, ttl=60)
        self._orderbook_cache: FtTTLCache = FtTTLCache(maxsize=100, ttl=5)
        
        # 1m OHLCV cache: download 1m once per pair, resample to higher timeframes
        # pair -> sorted list of [timestamp_ms, open, high, low, close, volume]
        self._1m_ohlcv_cache: dict[str, list[list]] = {}
        self._1m_cache_range: dict[str, tuple[int, int | None]] = {}  # pair -> (since_ms, until_ms)
        
        # Initialize parent
        super().__init__(config, *args, **kwargs)
        
        # Initialize Bitunix-specific attributes
        self._exchange_ws = None
        self._ws_async = None
        
        logger.info("Bitunix exchange initialized")
    
    def _init_ccxt(
        self, exchange_config: dict, sync: bool, ccxt_kwargs: dict
    ) -> BitunixCCXTAdapter:
        """
        Override ccxt initialization for Bitunix exchange.
        Returns a ccxt-like adapter instead of a real ccxt object.
        """
        return BitunixCCXTAdapter(self, is_async=not sync)
    
    def additional_exchange_init(self) -> None:
        """Additional initialization for Bitunix exchange"""
        try:
            if not self._config.get("dry_run", True):
                # Verify API credentials
                if not self._api_key or not self._api_secret:
                    raise OperationalException(
                        "Bitunix requires API key and secret for live trading"
                    )
                
                # Validate margin mode for futures
                if self.trading_mode == TradingMode.FUTURES:
                    if self.margin_mode not in [MarginMode.CROSS, MarginMode.ISOLATED]:
                        raise OperationalException(
                            f"Bitunix only supports CROSS or ISOLATED margin modes, not {self.margin_mode}"
                        )
            
            logger.info("Bitunix exchange additional initialization complete")
        
        except Exception as e:
            raise OperationalException(
                f"Failed to initialize Bitunix exchange: {e}"
            ) from e
    
    @property
    def name(self) -> str:
        """Return exchange name"""
        return "bitunix"
    
    @property
    def id(self) -> str:
        """Return exchange id"""
        return "bitunix"
    
    @property
    def precisionMode(self) -> int:
        """Return precision mode"""
        return 2  # DECIMAL_PLACES
    
    def exchange_has(self, endpoint: str) -> bool:
        """Check if exchange supports endpoint"""
        capabilities = {
            "fetchOHLCV": True,
            "fetchTrades": False,  # Not implemented in API docs
            "fetchOrderBook": True,
            "fetchL2OrderBook": True,
            "fetchTicker": True,
            "fetchTickers": True,
            "fetchMyTrades": True,
            "fetchOrders": True,
            "fetchOpenOrders": True,
            "fetchClosedOrders": True,
            "createOrder": True,
            "cancelOrder": True,
            "fetchBalance": True,
            "fetchPositions": True,
            "setLeverage": True,
            "setMarginMode": True,
        }
        return capabilities.get(endpoint, False)
    
    def load_markets(self) -> dict[str, Any]:
        """Load markets from Bitunix"""
        try:
            # Fetch tickers to get available markets
            url = f"{self._BASE_URL}/api/v1/futures/market/tickers"
            response = requests.get(url, timeout=30)
            
            if response.status_code != 200:
                raise TemporaryError(f"Failed to fetch Bitunix markets: {response.status_code}")
            
            data = response.json()
            if data.get("code") != 0:
                raise ExchangeError(f"Bitunix API error: {data.get('msg', 'Unknown error')}")
            
            markets = {}
            tickers_data = data.get("data", [])
            
            for ticker in tickers_data:
                bitunix_symbol = ticker.get("symbol", "")
                if not bitunix_symbol or not bitunix_symbol.endswith("USDT"):
                    continue
                
                base = bitunix_symbol[:-4]  # Remove USDT suffix
                quote = "USDT"
                
                if self.trading_mode == TradingMode.FUTURES:
                    symbol = f"{base}/{quote}:{quote}"
                else:
                    symbol = f"{base}/{quote}"
                
                markets[symbol] = {
                    "id": bitunix_symbol,
                    "symbol": symbol,
                    "base": base,
                    "quote": quote,
                    "settle": quote if self.trading_mode == TradingMode.FUTURES else None,
                    "active": True,
                    "type": "swap" if self.trading_mode == TradingMode.FUTURES else "spot",
                    "spot": self.trading_mode == TradingMode.SPOT,
                    "margin": False,
                    "future": False,
                    "swap": self.trading_mode == TradingMode.FUTURES,
                    "option": False,
                    "contract": self.trading_mode == TradingMode.FUTURES,
                    "linear": True,
                    "inverse": False,
                    "contractSize": 1.0,
                    "precision": {
                        "amount": 8,
                        "price": 8,
                    },
                    "limits": {
                        "amount": {"min": 0.001, "max": 1000000},
                        "price": {"min": 0.000001, "max": 10000000},
                        "cost": {"min": 1, "max": 100000000},
                        "leverage": {"min": 1, "max": 125},
                    },
                    "taker": 0.0006,
                    "maker": 0.0002,
                    "info": ticker,
                }
            
            self._markets = markets
            logger.info(f"Loaded {len(markets)} Bitunix markets")
            return markets
        
        except requests.RequestException as e:
            raise TemporaryError(f"Network error fetching Bitunix markets: {e}") from e
        except Exception as e:
            raise ExchangeError(f"Error loading Bitunix markets: {e}") from e
    
    def get_tickers(
        self,
        symbols: list[str] | None = None,
        *,
        cached: bool = False,
        market_type: TradingMode | None = None,
    ) -> Tickers:
        """Fetch tickers from Bitunix"""
        cache_key = "tickers"
        if cached and cache_key in self._ticker_cache:
            return self._ticker_cache[cache_key]
        
        try:
            url = f"{self._BASE_URL}/api/v1/futures/market/tickers"
            params = {}
            if symbols:
                bitunix_symbols = ",".join(
                    self._convert_to_bitunix_symbol(s) for s in symbols
                )
                params["symbols"] = bitunix_symbols
            
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code != 200:
                raise TemporaryError(f"Bitunix tickers error: {response.status_code}")
            
            data = response.json()
            if data.get("code") != 0:
                raise ExchangeError(f"Bitunix API error: {data.get('msg')}")
            
            tickers: Tickers = {}
            for ticker_data in data.get("data", []):
                try:
                    bitunix_symbol = ticker_data.get("symbol", "")
                    symbol = self._convert_from_bitunix_symbol(bitunix_symbol)
                    
                    if symbols and symbol not in symbols:
                        continue
                    
                    tickers[symbol] = {
                        "symbol": symbol,
                        "last": float(ticker_data.get("lastPrice", 0)),
                        "high": float(ticker_data.get("high", 0)),
                        "low": float(ticker_data.get("low", 0)),
                        "bid": float(ticker_data.get("bestBid", 0)) if ticker_data.get("bestBid") else None,
                        "ask": float(ticker_data.get("bestAsk", 0)) if ticker_data.get("bestAsk") else None,
                        "baseVolume": float(ticker_data.get("baseVolume", 0)),
                        "quoteVolume": float(ticker_data.get("quoteVolume", 0)),
                        "percentage": float(ticker_data.get("priceChangePercent", 0)),
                        "info": ticker_data,
                    }
                except (ValueError, TypeError):
                    continue
            
            self._ticker_cache[cache_key] = tickers
            return tickers
        
        except requests.RequestException as e:
            raise TemporaryError(f"Network error fetching Bitunix tickers: {e}") from e
    
    def fetch_l2_order_book(self, pair: str, limit: int = 100) -> OrderBook:
        """Fetch L2 orderbook from Bitunix"""
        cache_key = f"ob:{pair}:{limit}"
        if cache_key in self._orderbook_cache:
            return self._orderbook_cache[cache_key]
        
        bitunix_symbol = self._convert_to_bitunix_symbol(pair)
        
        url = f"{self._BASE_URL}/api/v1/futures/market/depth"
        params = {
            "symbol": bitunix_symbol,
            "limit": str(min(limit, 100))
        }
        
        try:
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code != 200:
                raise TemporaryError(f"Bitunix orderbook error: {response.status_code}")
            
            data = response.json()
            if data.get("code") != 0:
                raise ExchangeError(f"Bitunix API error: {data.get('msg')}")
            
            orderbook_data = data.get("data", {})
            
            bids = []
            asks = []
            
            for bid in orderbook_data.get("bids", []) or []:
                try:
                    bids.append([float(bid[0]), float(bid[1])])
                except (ValueError, TypeError, IndexError):
                    continue
            
            for ask in orderbook_data.get("asks", []) or []:
                try:
                    asks.append([float(ask[0]), float(ask[1])])
                except (ValueError, TypeError, IndexError):
                    continue
            
            orderbook: OrderBook = {
                "symbol": pair,
                "bids": bids,
                "asks": asks,
                "timestamp": int(datetime.now(tz=UTC).timestamp() * 1000),
                "datetime": datetime.now(tz=UTC).isoformat(),
                "nonce": None,
            }
            
            self._orderbook_cache[cache_key] = orderbook
            return orderbook
        
        except requests.RequestException as e:
            raise TemporaryError(f"Network error fetching Bitunix orderbook: {e}") from e
    
    def _convert_to_bitunix_symbol(self, pair: str) -> str:
        """Convert VulcanTrader pair to Bitunix format"""
        # e.g., BTC/USDT:USDT -> BTCUSDT
        if "/" in pair:
            base, quote = pair.split("/")
            if ":" in quote:
                quote = quote.split(":")[0]
            return f"{base}{quote}"
        return pair.replace("-", "").replace(":", "")
    
    def _convert_from_bitunix_symbol(self, bitunix_symbol: str) -> str:
        """Convert Bitunix symbol to VulcanTrader format"""
        if bitunix_symbol.endswith("USDT"):
            base = bitunix_symbol[:-4]
            if self.trading_mode == TradingMode.FUTURES:
                return f"{base}/USDT:USDT"
            return f"{base}/USDT"
        return bitunix_symbol
    
    def validate_timeframes(self, timeframe: str | None) -> None:
        """Validate timeframe for Bitunix"""
        supported = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M']
        if timeframe is None:
            return
        if timeframe not in supported:
            raise OperationalException(
                f"Timeframe {timeframe} not supported by Bitunix. Supported: {supported}"
            )
    
    def ohlcv_candle_limit(
        self, timeframe: str, candle_type: CandleType, since_ms: int | None = None
    ) -> int:
        """Return maximum candles per request"""
        return 200
    
    # ------------------------------------------------------------------
    # Download 1m data once, resample to all higher timeframes locally
    # ------------------------------------------------------------------
    
    # Timeframe -> milliseconds lookup
    _TF_MS: dict[str, int] = {
        "1m": 60_000, "3m": 180_000, "5m": 300_000,
        "10m": 600_000, "15m": 900_000, "30m": 1_800_000,
        "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000,
        "6h": 21_600_000, "8h": 28_800_000, "12h": 43_200_000,
        "1d": 86_400_000,
    }
    
    @staticmethod
    def _resample_ohlcv(data_1m: list[list], target_tf: str) -> list[list]:
        """Resample raw 1m OHLCV list to a higher timeframe.
        
        Groups 1m candles into target-timeframe buckets by integer division,
        then aggregates: open=first, high=max, low=min, close=last, volume=sum.
        
        Args:
            data_1m: Sorted ascending list of [ts_ms, open, high, low, close, vol]
            target_tf: Target timeframe string (e.g. "5m", "1h", "1d")
        Returns:
            Aggregated OHLCV list in ascending order.
        """
        target_ms = Bitunix._TF_MS.get(target_tf)
        if not target_ms or not data_1m:
            return data_1m  # Cannot resample or empty
        
        # Group 1m candles into target-timeframe buckets
        buckets: dict[int, list[list]] = {}
        for candle in data_1m:
            bucket_ts = (candle[0] // target_ms) * target_ms
            if bucket_ts not in buckets:
                buckets[bucket_ts] = []
            buckets[bucket_ts].append(candle)
        
        # Aggregate each bucket
        result: list[list] = []
        for bucket_ts in sorted(buckets):
            candles = buckets[bucket_ts]
            # candles are already timestamp-sorted from data_1m
            result.append([
                bucket_ts,
                candles[0][1],              # open  = first candle's open
                max(c[2] for c in candles),  # high  = max high
                min(c[3] for c in candles),  # low   = min low
                candles[-1][4],              # close = last candle's close
                sum(c[5] for c in candles),  # vol   = sum
            ])
        return result
    
    def _ensure_1m_data(
        self,
        pair: str,
        since_ms: int,
        candle_type: CandleType,
        until_ms: int | None,
    ) -> list[list]:
        """Download and cache 1m data for *pair*, expanding the cache if needed."""
        need_download = True
        if pair in self._1m_ohlcv_cache:
            cached_since, cached_until = self._1m_cache_range[pair]
            covers_start = cached_since <= since_ms
            covers_end = (
                (cached_until is None and until_ms is None)
                or (cached_until is not None and until_ms is not None and cached_until >= until_ms)
                or (cached_until is None and until_ms is not None)
            )
            if covers_start and covers_end:
                need_download = False
                logger.debug(
                    f"Bitunix: Using cached 1m data for {pair} "
                    f"({len(self._1m_ohlcv_cache[pair])} candles already cached)"
                )
        
        if need_download:
            # Determine the widest range we need
            dl_since = since_ms
            dl_until = until_ms
            if pair in self._1m_cache_range:
                old_since, old_until = self._1m_cache_range[pair]
                dl_since = min(since_ms, old_since)
                if until_ms is not None and old_until is not None:
                    dl_until = max(until_ms, old_until)
                elif old_until is None or until_ms is None:
                    dl_until = None  # open-ended
            
            logger.info(
                f"Bitunix: Downloading 1m data for {pair} "
                f"({datetime.fromtimestamp(dl_since / 1000, tz=UTC).strftime('%Y-%m-%d')} "
                f"-> {datetime.fromtimestamp(dl_until / 1000, tz=UTC).strftime('%Y-%m-%d') if dl_until else 'now'})"
            )
            with self._loop_lock:
                _, _, _, raw_1m, _ = self.loop.run_until_complete(
                    self._async_get_historic_ohlcv(
                        pair=pair,
                        timeframe="1m",
                        since_ms=dl_since,
                        until_ms=dl_until,
                        candle_type=candle_type,
                        raise_=True,
                    )
                )
            
            # Merge with existing cache (dedupe by timestamp)
            if pair in self._1m_ohlcv_cache:
                existing = self._1m_ohlcv_cache[pair]
                seen: set[int] = set()
                merged: list[list] = []
                for row in existing + raw_1m:
                    if row[0] not in seen:
                        seen.add(row[0])
                        merged.append(row)
                merged.sort(key=lambda x: x[0])
                raw_1m = merged
            
            self._1m_ohlcv_cache[pair] = raw_1m
            self._1m_cache_range[pair] = (dl_since, dl_until)
            logger.info(f"Bitunix: Cached {len(raw_1m)} 1m candles for {pair}")
        
        return self._1m_ohlcv_cache.get(pair, [])
    
    def get_historic_ohlcv(
        self,
        pair: str,
        timeframe: str,
        since_ms: int,
        candle_type: CandleType,
        is_new_pair: bool = False,
        until_ms: int | None = None,
    ) -> DataFrame:
        """Download 1m OHLCV and resample to any higher timeframe locally.
        
        For 1m (or funding-rate) requests the normal ccxt path is used.
        For every other timeframe the 1m data is fetched once per pair,
        cached, and then bucket-aggregated to the target timeframe.
        This dramatically reduces API calls for multi-timeframe downloads.
        """
        # Funding rate always goes through the normal path (no caching)
        if candle_type == CandleType.FUNDING_RATE:
            return super().get_historic_ohlcv(
                pair, timeframe, since_ms, candle_type, is_new_pair, until_ms
            )
        
        # For 1m requests, check cache first before downloading
        if timeframe == "1m":
            # Check if we already have cached 1m data that covers this range
            if pair in self._1m_ohlcv_cache and pair in self._1m_cache_range:
                cached_since, cached_until = self._1m_cache_range[pair]
                covers_start = cached_since <= since_ms
                covers_end = (
                    (cached_until is None and until_ms is None)
                    or (cached_until is not None and until_ms is not None and cached_until >= until_ms)
                    or (cached_until is None and until_ms is not None)
                )
                if covers_start and covers_end:
                    # Use cached data - filter to requested range
                    raw_1m = self._1m_ohlcv_cache[pair]
                    end_ts = until_ms if until_ms else int(datetime.now(tz=UTC).timestamp() * 1000)
                    filtered = [r for r in raw_1m if r[0] >= since_ms and r[0] <= end_ts]
                    logger.info(f"Bitunix: Using cached 1m data for {pair} ({len(filtered)} candles)")
                    return ohlcv_to_dataframe(
                        filtered, timeframe, pair, fill_missing=False, drop_incomplete=True
                    )
            
            # Cache miss or doesn't cover range - download and cache
            result = super().get_historic_ohlcv(
                pair, timeframe, since_ms, candle_type, is_new_pair, until_ms
            )
            if not result.empty:
                raw: list[list] = []
                for _, row in result.iterrows():
                    raw.append([
                        int(row["date"].timestamp() * 1000),
                        float(row["open"]),
                        float(row["high"]),
                        float(row["low"]),
                        float(row["close"]),
                        float(row["volume"]),
                    ])
                # Merge with existing cache if present
                if pair in self._1m_ohlcv_cache:
                    existing = self._1m_ohlcv_cache[pair]
                    seen: set[int] = set()
                    merged: list[list] = []
                    for row in existing + raw:
                        if row[0] not in seen:
                            seen.add(row[0])
                            merged.append(row)
                    merged.sort(key=lambda x: x[0])
                    raw = merged
                    # Expand cache range
                    old_since, old_until = self._1m_cache_range[pair]
                    new_since = min(since_ms, old_since)
                    if until_ms is not None and old_until is not None:
                        new_until = max(until_ms, old_until)
                    else:
                        new_until = None
                    self._1m_cache_range[pair] = (new_since, new_until)
                else:
                    self._1m_cache_range[pair] = (since_ms, until_ms)
                self._1m_ohlcv_cache[pair] = raw
                logger.info(f"Bitunix: Cached {len(raw)} 1m candles for {pair}")
            return result
        
        # Unsupported resample target -> fall back to API
        if timeframe not in self._TF_MS:
            logger.warning(f"Bitunix: Cannot resample to {timeframe}, using API directly")
            return super().get_historic_ohlcv(
                pair, timeframe, since_ms, candle_type, is_new_pair, until_ms
            )
        
        # Fetch / reuse cached 1m data
        raw_1m = self._ensure_1m_data(pair, since_ms, candle_type, until_ms)
        if not raw_1m:
            logger.warning(f"Bitunix: No 1m data for {pair}, falling back to API for {timeframe}")
            return super().get_historic_ohlcv(
                pair, timeframe, since_ms, candle_type, is_new_pair, until_ms
            )
        
        # Filter to requested range before resampling
        end_ts = until_ms if until_ms else int(datetime.now(tz=UTC).timestamp() * 1000)
        range_1m = [r for r in raw_1m if r[0] >= since_ms and r[0] <= end_ts]
        
        # Resample
        resampled = self._resample_ohlcv(range_1m, timeframe)
        logger.info(
            f"Bitunix: Resampled {len(range_1m)} 1m -> {len(resampled)} {timeframe} candles for {pair}"
        )
        
        return ohlcv_to_dataframe(
            resampled, timeframe, pair, fill_missing=False, drop_incomplete=True
        )
    
    def _lev_prep(self, pair: str, leverage: float, side: BuySell, accept_fail: bool = False):
        """Prepare leverage for trading"""
        if self.trading_mode != TradingMode.SPOT:
            try:
                self.set_leverage(leverage, pair)
            except Exception as e:
                if not accept_fail:
                    raise
                logger.warning(f"Failed to set leverage for {pair}: {e}")
    
    def set_leverage(self, leverage: float, pair: str, params: dict | None = None) -> None:
        """Set leverage for a trading pair
        
        API: POST /api/v1/futures/account/change_leverage
        """
        if self._config.get("dry_run", True):
            return
        
        bitunix_symbol = self._convert_to_bitunix_symbol(pair)
        
        url = f"{self._BASE_URL}/api/v1/futures/account/change_leverage"
        data = {
            "symbol": bitunix_symbol,
            "leverage": int(leverage),
            "marginCoin": "USDT",
        }
        
        body = json.dumps(data)
        headers = BitunixAuth.get_auth_headers(self._api_key, self._api_secret, body=body)
        
        try:
            response = requests.post(url, json=data, headers=headers, timeout=10)
            result = response.json()
            
            if result.get("code") != 0:
                raise ExchangeError(f"Failed to set leverage: {result.get('msg')}")
            
            logger.info(f"Set leverage to {leverage}x for {pair}")
        except requests.RequestException as e:
            raise TemporaryError(f"Network error setting leverage: {e}") from e
    
    def set_margin_mode(self, pair: str, marginmode: MarginMode, params: dict | None = None) -> None:
        """Set margin mode for a trading pair
        
        API: POST /api/v1/futures/account/change_margin_mode
        Note: Cannot be used when user has open positions or orders
        """
        if self._config.get("dry_run", True):
            return
        
        bitunix_symbol = self._convert_to_bitunix_symbol(pair)
        
        # Map margin mode values - Bitunix uses ISOLATION instead of ISOLATED
        margin_mode_map = {
            "cross": "CROSS",
            "isolated": "ISOLATION",
        }
        bitunix_margin_mode = margin_mode_map.get(marginmode.value.lower(), "ISOLATION")
        
        url = f"{self._BASE_URL}/api/v1/futures/account/change_margin_mode"
        data = {
            "symbol": bitunix_symbol,
            "marginMode": bitunix_margin_mode,
            "marginCoin": "USDT",
        }
        
        body = json.dumps(data)
        headers = BitunixAuth.get_auth_headers(self._api_key, self._api_secret, body=body)
        
        try:
            response = requests.post(url, json=data, headers=headers, timeout=10)
            result = response.json()
            
            if result.get("code") != 0:
                raise ExchangeError(f"Failed to set margin mode: {result.get('msg')}")
            
            logger.info(f"Set margin mode to {marginmode.value} for {pair}")
        except requests.RequestException as e:
            raise TemporaryError(f"Network error setting margin mode: {e}") from e
    
    def change_position_mode(self, position_mode: str) -> None:
        """Change position mode between one-way and hedge mode
        
        API: POST /api/v1/futures/account/change_position_mode
        Note: Cannot be changed when there are open positions or orders
        
        :param position_mode: 'ONE_WAY' or 'HEDGE'
        """
        if self._config.get("dry_run", True):
            return
        
        url = f"{self._BASE_URL}/api/v1/futures/account/change_position_mode"
        data = {
            "positionMode": position_mode.upper(),
        }
        
        body = json.dumps(data)
        headers = BitunixAuth.get_auth_headers(self._api_key, self._api_secret, body=body)
        
        try:
            response = requests.post(url, json=data, headers=headers, timeout=10)
            result = response.json()
            
            if result.get("code") != 0:
                raise ExchangeError(f"Failed to change position mode: {result.get('msg')}")
            
            logger.info(f"Changed position mode to {position_mode}")
        except requests.RequestException as e:
            raise TemporaryError(f"Network error changing position mode: {e}") from e
    
    def adjust_position_margin(
        self,
        pair: str,
        amount: float,
        side: str | None = None,
        position_id: str | None = None
    ) -> None:
        """Add or reduce position margin (isolated margin mode only)
        
        API: POST /api/v1/futures/account/adjust_position_margin
        
        :param pair: Trading pair
        :param amount: Margin amount (positive=increase, negative=decrease)
        :param side: Position side ('LONG' or 'SHORT'), required if no position_id
        :param position_id: Position ID, required if no side
        """
        if self._config.get("dry_run", True):
            return
        
        bitunix_symbol = self._convert_to_bitunix_symbol(pair)
        
        url = f"{self._BASE_URL}/api/v1/futures/account/adjust_position_margin"
        data = {
            "symbol": bitunix_symbol,
            "marginCoin": "USDT",
            "amount": str(amount),
        }
        
        if side:
            data["side"] = side.upper()
        if position_id:
            data["positionId"] = position_id
        
        body = json.dumps(data)
        headers = BitunixAuth.get_auth_headers(self._api_key, self._api_secret, body=body)
        
        try:
            response = requests.post(url, json=data, headers=headers, timeout=10)
            result = response.json()
            
            if result.get("code") != 0:
                raise ExchangeError(f"Failed to adjust position margin: {result.get('msg')}")
            
            logger.info(f"Adjusted position margin by {amount} for {pair}")
        except requests.RequestException as e:
            raise TemporaryError(f"Network error adjusting position margin: {e}") from e
    
    def get_leverage_margin_mode(self, pair: str) -> dict:
        """Get leverage and margin mode for a symbol
        
        API: GET /api/v1/futures/account/get_leverage_margin_mode
        """
        bitunix_symbol = self._convert_to_bitunix_symbol(pair)
        
        url = f"{self._BASE_URL}/api/v1/futures/account/get_leverage_margin_mode"
        params = {
            "symbol": bitunix_symbol,
            "marginCoin": "USDT",
        }
        
        query_string = BitunixAuth.sort_params(params)
        headers = BitunixAuth.get_auth_headers(self._api_key, self._api_secret, query_string)
        
        try:
            response = requests.get(url, params=params, headers=headers, timeout=10)
            result = response.json()
            
            if result.get("code") != 0:
                raise ExchangeError(f"Failed to get leverage/margin mode: {result.get('msg')}")
            
            data = result.get("data", {})
            return {
                "symbol": pair,
                "leverage": int(data.get("leverage", 1)),
                "marginMode": data.get("marginMode", "ISOLATION"),
                "info": data,
            }
        except requests.RequestException as e:
            raise TemporaryError(f"Network error getting leverage/margin mode: {e}") from e
    
    def fetch_funding_rate(self, pair: str) -> dict:
        """Get current funding rate for a symbol
        
        API: GET /api/v1/futures/market/funding_rate
        """
        bitunix_symbol = self._convert_to_bitunix_symbol(pair)
        
        url = f"{self._BASE_URL}/api/v1/futures/market/funding_rate"
        params = {
            "symbol": bitunix_symbol,
        }
        
        try:
            response = requests.get(url, params=params, timeout=10)
            result = response.json()
            
            if result.get("code") != 0:
                raise ExchangeError(f"Failed to get funding rate: {result.get('msg')}")
            
            data = result.get("data", [])
            if data and isinstance(data, list) and len(data) > 0:
                rate_data = data[0]
                return {
                    "symbol": pair,
                    "markPrice": float(rate_data.get("markPrice", 0)),
                    "lastPrice": float(rate_data.get("lastPrice", 0)),
                    "fundingRate": float(rate_data.get("fundingRate", 0)),
                    "timestamp": int(datetime.now(tz=UTC).timestamp() * 1000),
                    "info": rate_data,
                }
            return {"symbol": pair, "fundingRate": 0, "info": data}
        except requests.RequestException as e:
            raise TemporaryError(f"Network error getting funding rate: {e}") from e
    
    def fetch_position_tiers(self, pair: str) -> list[dict]:
        """Get position tiers (leverage tiers) for a symbol
        
        API: GET /api/v1/futures/position/get_position_tiers
        """
        bitunix_symbol = self._convert_to_bitunix_symbol(pair)
        
        url = f"{self._BASE_URL}/api/v1/futures/position/get_position_tiers"
        params = {
            "symbol": bitunix_symbol,
        }
        
        try:
            response = requests.get(url, params=params, timeout=10)
            result = response.json()
            
            if result.get("code") != 0:
                raise ExchangeError(f"Failed to get position tiers: {result.get('msg')}")
            
            data = result.get("data", [])
            tiers = []
            if data and isinstance(data, list):
                for tier_data in data:
                    tiers.append({
                        "symbol": pair,
                        "tier": int(tier_data.get("level", 0)),
                        "minNotional": float(tier_data.get("startValue", 0)),
                        "maxNotional": float(tier_data.get("endValue", 0)),
                        "maxLeverage": int(tier_data.get("leverage", 1)),
                        "maintenanceMarginRate": float(tier_data.get("maintenanceMarginRate", 0)),
                        "info": tier_data,
                    })
            return tiers
        except requests.RequestException as e:
            raise TemporaryError(f"Network error getting position tiers: {e}") from e
    
    def fetch_history_positions(
        self,
        pair: str | None = None,
        position_id: str | None = None,
        since: int | None = None,
        limit: int | None = None,
    ) -> list[dict]:
        """Get historical positions
        
        API: GET /api/v1/futures/position/get_history_positions
        """
        url = f"{self._BASE_URL}/api/v1/futures/position/get_history_positions"
        
        params = {}
        if pair:
            params["symbol"] = self._convert_to_bitunix_symbol(pair)
        if position_id:
            params["positionId"] = position_id
        if since:
            params["startTime"] = str(since)
        if limit:
            params["limit"] = str(min(limit, 100))
        
        query_string = BitunixAuth.sort_params(params)
        headers = BitunixAuth.get_auth_headers(self._api_key, self._api_secret, query_string)
        
        try:
            response = requests.get(url, params=params, headers=headers, timeout=10)
            result = response.json()
            
            if result.get("code") != 0:
                raise ExchangeError(f"Failed to get history positions: {result.get('msg')}")
            
            data = result.get("data", {})
            positions = []
            position_list = data.get("positionList", []) if isinstance(data, dict) else []
            
            for pos_data in position_list:
                bitunix_symbol = pos_data.get("symbol", "")
                symbol = self._convert_from_bitunix_symbol(bitunix_symbol)
                side = pos_data.get("side", "").lower()
                
                positions.append({
                    "id": pos_data.get("positionId", ""),
                    "symbol": symbol,
                    "side": "short" if side == "short" else "long",
                    "contracts": float(pos_data.get("maxQty", 0)),
                    "entryPrice": float(pos_data.get("entryPrice", 0)),
                    "closePrice": float(pos_data.get("closePrice", 0)),
                    "leverage": int(pos_data.get("leverage", 1)),
                    "marginMode": pos_data.get("marginMode", "ISOLATION").lower(),
                    "positionMode": pos_data.get("positionMode", "HEDGE"),
                    "fee": float(pos_data.get("fee", 0)),
                    "funding": float(pos_data.get("funding", 0)),
                    "realizedPnl": float(pos_data.get("realizedPNL", 0)),
                    "liquidationPrice": float(pos_data.get("liqPrice", 0)) if pos_data.get("liqPrice") else None,
                    "timestamp": int(pos_data.get("cTime", 0)),
                    "info": pos_data,
                })
            
            return positions
        except requests.RequestException as e:
            raise TemporaryError(f"Network error getting history positions: {e}") from e
    
    def batch_order(
        self,
        pair: str,
        orders: list[dict],
    ) -> dict:
        """Place multiple orders at once (max 5)
        
        API: POST /api/v1/futures/trade/batch_order
        """
        if self._config.get("dry_run", True):
            return {"successList": [], "failureList": [], "info": {}}
        
        bitunix_symbol = self._convert_to_bitunix_symbol(pair)
        
        if len(orders) > 5:
            raise InvalidOrderException("Batch order supports maximum 5 orders")
        
        order_list = []
        for order in orders:
            order_data = {
                "qty": str(order.get("qty", order.get("amount", 0))),
                "side": order.get("side", "BUY").upper(),
                "tradeSide": order.get("tradeSide", "OPEN").upper(),
                "orderType": order.get("orderType", order.get("type", "LIMIT")).upper(),
            }
            if order.get("price"):
                order_data["price"] = str(order["price"])
            if order.get("positionId"):
                order_data["positionId"] = order["positionId"]
            if order.get("effect"):
                order_data["effect"] = order["effect"]
            if order.get("clientId"):
                order_data["clientId"] = order["clientId"]
            if order.get("reduceOnly") is not None:
                order_data["reduceOnly"] = order["reduceOnly"]
            for key in ["tpPrice", "tpStopType", "tpOrderType", "tpOrderPrice",
                        "slPrice", "slStopType", "slOrderType", "slOrderPrice"]:
                if order.get(key):
                    order_data[key] = str(order[key])
            order_list.append(order_data)
        
        url = f"{self._BASE_URL}/api/v1/futures/trade/batch_order"
        data = {
            "symbol": bitunix_symbol,
            "orderList": order_list,
        }
        
        body = json.dumps(data)
        headers = BitunixAuth.get_auth_headers(self._api_key, self._api_secret, body=body)
        
        try:
            response = requests.post(url, json=data, headers=headers, timeout=10)
            result = response.json()
            
            if result.get("code") != 0:
                raise ExchangeError(f"Failed to place batch order: {result.get('msg')}")
            
            return {
                "successList": result.get("data", {}).get("successList", []),
                "failureList": result.get("data", {}).get("failureList", []),
                "info": result.get("data", {}),
            }
        except requests.RequestException as e:
            raise TemporaryError(f"Network error placing batch order: {e}") from e
    
    def cancel_all_orders(self, pair: str | None = None) -> dict:
        """Cancel all orders
        
        API: POST /api/v1/futures/trade/cancel_all_orders
        """
        if self._config.get("dry_run", True):
            return {"successList": [], "failureList": [], "info": {}}
        
        url = f"{self._BASE_URL}/api/v1/futures/trade/cancel_all_orders"
        data = {}
        if pair:
            data["symbol"] = self._convert_to_bitunix_symbol(pair)
        
        body = json.dumps(data)
        headers = BitunixAuth.get_auth_headers(self._api_key, self._api_secret, body=body)
        
        try:
            response = requests.post(url, json=data, headers=headers, timeout=10)
            result = response.json()
            
            if result.get("code") != 0:
                raise ExchangeError(f"Failed to cancel all orders: {result.get('msg')}")
            
            logger.info(f"Cancelled all orders" + (f" for {pair}" if pair else ""))
            return {
                "successList": result.get("data", {}).get("successList", []),
                "failureList": result.get("data", {}).get("failureList", []),
                "info": result.get("data", {}),
            }
        except requests.RequestException as e:
            raise TemporaryError(f"Network error cancelling all orders: {e}") from e
    
    def close_all_positions(self, pair: str | None = None) -> dict:
        """Close all positions
        
        API: POST /api/v1/futures/trade/close_all_position
        """
        if self._config.get("dry_run", True):
            return {"status": "closed", "info": {}}
        
        url = f"{self._BASE_URL}/api/v1/futures/trade/close_all_position"
        data = {}
        if pair:
            data["symbol"] = self._convert_to_bitunix_symbol(pair)
        
        body = json.dumps(data)
        headers = BitunixAuth.get_auth_headers(self._api_key, self._api_secret, body=body)
        
        try:
            response = requests.post(url, json=data, headers=headers, timeout=10)
            result = response.json()
            
            if result.get("code") != 0:
                raise ExchangeError(f"Failed to close all positions: {result.get('msg')}")
            
            logger.info(f"Closed all positions" + (f" for {pair}" if pair else ""))
            return {"status": "closed", "info": result.get("data", {})}
        except requests.RequestException as e:
            raise TemporaryError(f"Network error closing all positions: {e}") from e
    
    def flash_close_position(self, position_id: str) -> dict:
        """Close position by position ID (flash close)
        
        API: POST /api/v1/futures/trade/flash_close_position
        """
        if self._config.get("dry_run", True):
            return {"positionId": position_id, "status": "closed", "info": {}}
        
        url = f"{self._BASE_URL}/api/v1/futures/trade/flash_close_position"
        data = {"positionId": position_id}
        
        body = json.dumps(data)
        headers = BitunixAuth.get_auth_headers(self._api_key, self._api_secret, body=body)
        
        try:
            response = requests.post(url, json=data, headers=headers, timeout=10)
            result = response.json()
            
            if result.get("code") != 0:
                raise ExchangeError(f"Failed to flash close position: {result.get('msg')}")
            
            logger.info(f"Flash closed position {position_id}")
            return {
                "positionId": result.get("data", {}).get("positionId", position_id),
                "status": "closed",
                "info": result.get("data", {}),
            }
        except requests.RequestException as e:
            raise TemporaryError(f"Network error flash closing position: {e}") from e
    
    def fetch_order_detail(
        self,
        order_id: str | None = None,
        client_id: str | None = None,
    ) -> dict:
        """Get order detail by orderId or clientId
        
        API: GET /api/v1/futures/trade/get_order_detail
        """
        if not order_id and not client_id:
            raise InvalidOrderException("Either order_id or client_id is required")
        
        url = f"{self._BASE_URL}/api/v1/futures/trade/get_order_detail"
        params = {}
        if order_id:
            params["orderId"] = order_id
        if client_id:
            params["clientId"] = client_id
        
        query_string = BitunixAuth.sort_params(params)
        headers = BitunixAuth.get_auth_headers(self._api_key, self._api_secret, query_string)
        
        try:
            response = requests.get(url, params=params, headers=headers, timeout=10)
            result = response.json()
            
            if result.get("code") != 0:
                raise InvalidOrderException(f"Failed to get order detail: {result.get('msg')}")
            
            data = result.get("data", {})
            if data:
                return self._parse_api_order(data)
            raise InvalidOrderException("Order not found")
        except requests.RequestException as e:
            raise TemporaryError(f"Network error getting order detail: {e}") from e
    
    def _parse_api_order(self, order_data: dict) -> dict:
        """Parse Bitunix API order response to standard format"""
        bitunix_symbol = order_data.get("symbol", "")
        symbol = self._convert_from_bitunix_symbol(bitunix_symbol)
        
        status_map = {
            "INIT": "open",
            "NEW": "open",
            "PART_FILLED": "open",
            "FILLED": "closed",
            "CANCELED": "canceled",
            "PART_FILLED_CANCELED": "canceled",
            "EXPIRED": "expired",
        }
        
        return {
            "id": order_data.get("orderId", ""),
            "clientOrderId": order_data.get("clientId", ""),
            "symbol": symbol,
            "type": order_data.get("orderType", "").lower(),
            "side": order_data.get("side", "").lower(),
            "price": float(order_data.get("price", 0)) if order_data.get("price") else None,
            "amount": float(order_data.get("qty", 0)),
            "filled": float(order_data.get("tradeQty", 0)),
            "remaining": float(order_data.get("qty", 0)) - float(order_data.get("tradeQty", 0)),
            "status": status_map.get(order_data.get("status", ""), "unknown"),
            "leverage": int(order_data.get("leverage", 1)),
            "marginMode": order_data.get("marginMode", "ISOLATION").lower(),
            "positionMode": order_data.get("positionMode", "HEDGE"),
            "reduceOnly": order_data.get("reduceOnly", False),
            "fee": float(order_data.get("fee", 0)),
            "realizedPnl": float(order_data.get("realizedPNL", 0)),
            "tpPrice": order_data.get("tpPrice"),
            "slPrice": order_data.get("slPrice"),
            "timestamp": int(order_data.get("ctime", 0)),
            "info": order_data,
        }
    
    def modify_order(
        self,
        order_id: str | None = None,
        client_id: str | None = None,
        qty: float | None = None,
        price: float | None = None,
        tp_price: float | None = None,
        sl_price: float | None = None,
    ) -> dict:
        """Modify a pending order
        
        API: POST /api/v1/futures/trade/modify_order
        """
        if self._config.get("dry_run", True):
            return {"orderId": order_id, "clientId": client_id, "info": {}}
        
        if not order_id and not client_id:
            raise InvalidOrderException("Either order_id or client_id is required")
        
        url = f"{self._BASE_URL}/api/v1/futures/trade/modify_order"
        data = {}
        if order_id:
            data["orderId"] = order_id
        if client_id:
            data["clientId"] = client_id
        if qty is not None:
            data["qty"] = str(qty)
        if price is not None:
            data["price"] = str(price)
        if tp_price is not None:
            data["tpPrice"] = str(tp_price)
        if sl_price is not None:
            data["slPrice"] = str(sl_price)
        
        body = json.dumps(data)
        headers = BitunixAuth.get_auth_headers(self._api_key, self._api_secret, body=body)
        
        try:
            response = requests.post(url, json=data, headers=headers, timeout=10)
            result = response.json()
            
            if result.get("code") != 0:
                raise ExchangeError(f"Failed to modify order: {result.get('msg')}")
            
            logger.info(f"Modified order {order_id or client_id}")
            return {
                "orderId": result.get("data", {}).get("orderId", order_id),
                "clientId": result.get("data", {}).get("clientId", client_id),
                "info": result.get("data", {}),
            }
        except requests.RequestException as e:
            raise TemporaryError(f"Network error modifying order: {e}") from e
    
    def fetch_history_trades(
        self,
        pair: str | None = None,
        order_id: str | None = None,
        position_id: str | None = None,
        since: int | None = None,
        until: int | None = None,
        limit: int = 10,
    ) -> list[dict]:
        """Get history trades
        
        API: GET /api/v1/futures/trade/get_history_trades
        """
        url = f"{self._BASE_URL}/api/v1/futures/trade/get_history_trades"
        
        params = {
            "limit": str(min(limit, 100)),
        }
        if pair:
            params["symbol"] = self._convert_to_bitunix_symbol(pair)
        if order_id:
            params["orderId"] = order_id
        if position_id:
            params["positionId"] = position_id
        if since:
            params["startTime"] = str(since)
        if until:
            params["endTime"] = str(until)
        
        query_string = BitunixAuth.sort_params(params)
        headers = BitunixAuth.get_auth_headers(self._api_key, self._api_secret, query_string)
        
        try:
            response = requests.get(url, params=params, headers=headers, timeout=10)
            result = response.json()
            
            if result.get("code") != 0:
                raise ExchangeError(f"Failed to get history trades: {result.get('msg')}")
            
            data = result.get("data", {})
            trades = []
            trade_list = data.get("tradeList", []) if isinstance(data, dict) else []
            
            for trade_data in trade_list:
                bitunix_symbol = trade_data.get("symbol", "")
                symbol = self._convert_from_bitunix_symbol(bitunix_symbol)
                
                trades.append({
                    "id": trade_data.get("tradeId", ""),
                    "orderId": trade_data.get("orderId", ""),
                    "symbol": symbol,
                    "type": trade_data.get("orderType", "").lower(),
                    "side": trade_data.get("side", "").lower(),
                    "price": float(trade_data.get("price", 0)),
                    "amount": float(trade_data.get("qty", 0)),
                    "fee": float(trade_data.get("fee", 0)),
                    "realizedPnl": float(trade_data.get("realizedPNL", 0)),
                    "takerOrMaker": trade_data.get("roleType", "TAKER").lower(),
                    "leverage": int(trade_data.get("leverage", 1)),
                    "timestamp": int(trade_data.get("ctime", 0)),
                    "info": trade_data,
                })
            
            return trades
        except requests.RequestException as e:
            raise TemporaryError(f"Network error getting history trades: {e}") from e
    
    def get_max_leverage(self, pair: str, stake_amount: float | None) -> float:
        """Get maximum leverage for a pair"""
        market = self.markets.get(pair, {})
        return float(market.get("limits", {}).get("leverage", {}).get("max", 125))
    
    def dry_run_liquidation_price(
        self,
        pair: str,
        open_rate: float,
        is_short: bool,
        amount: float,
        stake_amount: float,
        leverage: float,
        wallet_balance: float,
        open_trades: list,
    ) -> float | None:
        """Calculate liquidation price for dry run"""
        # Simplified liquidation calculation
        # Real implementation would need more details from Bitunix documentation
        
        if leverage <= 0:
            return None
        
        maintenance_margin_rate = 0.004  # 0.4% typical maintenance margin
        
        if is_short:
            # For short: liq_price = entry_price * (1 + (1/leverage) - maintenance_margin_rate)
            liq_price = open_rate * (1 + (1 / leverage) - maintenance_margin_rate)
        else:
            # For long: liq_price = entry_price * (1 - (1/leverage) + maintenance_margin_rate)
            liq_price = open_rate * (1 - (1 / leverage) + maintenance_margin_rate)
        
        return max(0, liq_price)
    
    def get_funding_fees(
        self, pair: str, amount: float, is_short: bool, open_date: datetime
    ) -> float:
        """Get funding fees for a position"""
        if self.trading_mode == TradingMode.FUTURES:
            try:
                return self._fetch_and_calculate_funding_fees(pair, amount, is_short, open_date)
            except Exception as e:
                logger.warning(f"Could not fetch funding fees for {pair}: {e}")
        return 0.0
    
    def _load_async_markets(self, reload: bool = False) -> None:
        """Load markets asynchronously"""
        if reload:
            self.load_markets()
    
    def reload_markets(self, force: bool = False, *, load_leverage_tiers: bool = True) -> None:
        """Reload markets"""
        from VulcanTrader.util.datetime_helpers import dt_ts
        
        if (
            not force
            and self._last_markets_refresh > 0
            and (self._last_markets_refresh + self.markets_refresh_interval > dt_ts())
        ):
            return
        
        self.load_markets()
        self._last_markets_refresh = dt_ts()
