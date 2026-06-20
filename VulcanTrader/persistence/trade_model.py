"""
Pure-Python trade & order persistence (no SQLAlchemy).

The on-disk format is a JSON file driven by
:func:`VulcanTrader.persistence.models.init_db` -- this module owns the
in-memory state and provides Python equivalents of the SQL queries the rest
of the codebase used to issue.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from math import isclose
from typing import Any, Optional, Self

from VulcanTrader.constants import (
    CANCELED_EXCHANGE_STATES,
    CUSTOM_TAG_MAX_LENGTH,
    DATETIME_PRINT_FORMAT,
    MATH_CLOSE_PREC,
    NON_OPEN_EXCHANGE_STATES,
    BuySell,
    LongShort,
)
from VulcanTrader.enums import ExitType, TradingMode
from VulcanTrader.util.exceptions import DependencyException, OperationalException
from VulcanTrader.exchange import (
    ROUND_DOWN,
    ROUND_UP,
    amount_to_contract_precision,
    price_to_precision,
)
from VulcanTrader.exchange.exchange_types import CcxtOrder
from VulcanTrader.util import interest
from VulcanTrader.util.misc import safe_value_fallback
from VulcanTrader.persistence.base import ModelBase
from VulcanTrader.persistence.custom_data import CustomDataWrapper, _CustomData
from VulcanTrader.util import FtPrecise, dt_from_ts, dt_now, dt_ts, dt_ts_none, round_value


logger = logging.getLogger(__name__)


# Type alias: callers may pass a callable predicate (recommended) or a list of
# callables. Legacy SQLAlchemy expression objects are no longer accepted.
TradeFilter = Callable[["Trade"], bool] | list[Callable[["Trade"], bool]] | None


def _truncate(value: str | None, max_len: int) -> str | None:
    if value and len(value) > max_len:
        return value[:max_len]
    return value


@dataclass
class ProfitStruct:
    profit_abs: float
    profit_ratio: float
    total_profit: float
    total_profit_ratio: float


# ---------------------------------------------------------------------------
#  Order
# ---------------------------------------------------------------------------


class Order(ModelBase):
    """Mirrors a CCXT order. One-to-many relationship with :class:`Trade`."""

    __tablename__ = "orders"

    def __init__(
        self,
        *,
        order_id: str,
        ft_order_side: str,
        ft_pair: str,
        ft_amount: float,
        ft_price: float | None,
        ft_trade_id: int | None = None,
        ft_is_open: bool = True,
        ft_cancel_reason: str | None = None,
        ft_fee_base: float | None = None,
        ft_order_tag: str | None = None,
        status: str | None = None,
        symbol: str | None = None,
        order_type: str | None = None,
        side: str | None = None,
        price: float | None = None,
        average: float | None = None,
        amount: float | None = None,
        filled: float | None = None,
        remaining: float | None = None,
        cost: float | None = None,
        stop_price: float | None = None,
        order_date: datetime | None = None,
        order_filled_date: datetime | None = None,
        order_update_date: datetime | None = None,
        funding_fee: float | None = None,
        id: int | None = None,
    ) -> None:
        self.id = id or 0
        self.ft_trade_id = ft_trade_id
        self.ft_order_side = ft_order_side
        self.ft_pair = ft_pair
        self.ft_is_open = ft_is_open
        self.ft_amount = ft_amount
        self.ft_price = ft_price
        self.ft_cancel_reason = ft_cancel_reason
        self.ft_fee_base = ft_fee_base
        self.ft_order_tag = _truncate(ft_order_tag, CUSTOM_TAG_MAX_LENGTH)

        self.order_id = order_id
        self.status = status
        self.symbol = symbol
        self.order_type = order_type
        self.side = side
        self.price = price
        self.average = average
        self.amount = amount
        self.filled = filled
        self.remaining = remaining
        self.cost = cost
        self.stop_price = stop_price
        self.order_date = order_date or dt_now()
        self.order_filled_date = order_filled_date
        self.order_update_date = order_update_date
        self.funding_fee = funding_fee

        # Back-pointers populated by Trade.__init__ / append.
        self._trade_live: "Trade | None" = None
        self._trade_bt: "LocalTrade | None" = None

    # ----- serialisation ---------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "ft_trade_id": self.ft_trade_id,
            "ft_order_side": self.ft_order_side,
            "ft_pair": self.ft_pair,
            "ft_is_open": self.ft_is_open,
            "ft_amount": self.ft_amount,
            "ft_price": self.ft_price,
            "ft_cancel_reason": self.ft_cancel_reason,
            "ft_fee_base": self.ft_fee_base,
            "ft_order_tag": self.ft_order_tag,
            "order_id": self.order_id,
            "status": self.status,
            "symbol": self.symbol,
            "order_type": self.order_type,
            "side": self.side,
            "price": self.price,
            "average": self.average,
            "amount": self.amount,
            "filled": self.filled,
            "remaining": self.remaining,
            "cost": self.cost,
            "stop_price": self.stop_price,
            "order_date": self.order_date.isoformat() if self.order_date else None,
            "order_filled_date": self.order_filled_date.isoformat()
            if self.order_filled_date
            else None,
            "order_update_date": self.order_update_date.isoformat()
            if self.order_update_date
            else None,
            "funding_fee": self.funding_fee,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Order":
        def _parse(v: Any) -> datetime | None:
            if v is None:
                return None
            return datetime.fromisoformat(v) if isinstance(v, str) else v

        return cls(
            id=data.get("id"),
            ft_trade_id=data.get("ft_trade_id"),
            ft_order_side=data["ft_order_side"],
            ft_pair=data["ft_pair"],
            ft_is_open=data.get("ft_is_open", True),
            ft_amount=data.get("ft_amount", 0.0),
            ft_price=data.get("ft_price"),
            ft_cancel_reason=data.get("ft_cancel_reason"),
            ft_fee_base=data.get("ft_fee_base"),
            ft_order_tag=data.get("ft_order_tag"),
            order_id=data["order_id"],
            status=data.get("status"),
            symbol=data.get("symbol"),
            order_type=data.get("order_type"),
            side=data.get("side"),
            price=data.get("price"),
            average=data.get("average"),
            amount=data.get("amount"),
            filled=data.get("filled"),
            remaining=data.get("remaining"),
            cost=data.get("cost"),
            stop_price=data.get("stop_price"),
            order_date=_parse(data.get("order_date")),
            order_filled_date=_parse(data.get("order_filled_date")),
            order_update_date=_parse(data.get("order_update_date")),
            funding_fee=data.get("funding_fee"),
        )

    # ----- properties -------------------------------------------------------

    @property
    def order_date_utc(self) -> datetime:
        return self.order_date.replace(tzinfo=UTC)

    @property
    def order_filled_utc(self) -> datetime | None:
        return self.order_filled_date.replace(tzinfo=UTC) if self.order_filled_date else None

    @property
    def safe_amount(self) -> float:
        return self.amount or self.ft_amount

    @property
    def safe_placement_price(self) -> float:
        return self.price or self.stop_price or self.ft_price

    @property
    def safe_price(self) -> float:
        return self.average or self.price or self.stop_price or self.ft_price

    @property
    def safe_filled(self) -> float:
        return self.filled if self.filled is not None else 0.0

    @property
    def safe_cost(self) -> float:
        return self.cost or 0.0

    @property
    def safe_remaining(self) -> float:
        return (
            self.remaining
            if self.remaining is not None
            else self.safe_amount - (self.filled or 0.0)
        )

    @property
    def safe_fee_base(self) -> float:
        return self.ft_fee_base or 0.0

    @property
    def safe_amount_after_fee(self) -> float:
        return self.safe_filled - self.safe_fee_base

    @property
    def trade(self) -> "LocalTrade":
        return self._trade_bt or self._trade_live

    @property
    def stake_amount(self) -> float:
        return float(
            FtPrecise(self.safe_amount)
            * FtPrecise(self.safe_price)
            / FtPrecise(self.trade.leverage)
        )

    @property
    def stake_amount_filled(self) -> float:
        return float(
            FtPrecise(self.safe_filled)
            * FtPrecise(self.safe_price)
            / FtPrecise(self.trade.leverage)
        )

    def __repr__(self) -> str:
        return (
            f"Order(id={self.id}, trade={self.ft_trade_id}, order_id={self.order_id}, "
            f"side={self.side}, filled={self.safe_filled}, price={self.safe_price}, "
            f"amount={self.amount}, status={self.status}, "
            f"date={self.order_date_utc:{DATETIME_PRINT_FORMAT}})"
        )

    # ----- mutation ---------------------------------------------------------

    def update_from_ccxt_object(self, order: dict[str, Any]) -> None:
        if self.order_id != str(order["id"]):
            raise DependencyException("Order-id's don't match")

        self.status = safe_value_fallback(order, "status", default_value=self.status)
        self.symbol = safe_value_fallback(order, "symbol", default_value=self.symbol)
        self.order_type = safe_value_fallback(order, "type", default_value=self.order_type)
        self.side = safe_value_fallback(order, "side", default_value=self.side)
        self.price = safe_value_fallback(order, "price", default_value=self.price)
        self.amount = safe_value_fallback(order, "amount", default_value=self.amount)
        self.filled = safe_value_fallback(order, "filled", default_value=self.filled)
        self.average = safe_value_fallback(order, "average", default_value=self.average)
        self.remaining = safe_value_fallback(order, "remaining", default_value=self.remaining)
        self.cost = safe_value_fallback(order, "cost", default_value=self.cost)
        self.stop_price = safe_value_fallback(order, "stopPrice", default_value=self.stop_price)
        order_date = safe_value_fallback(order, "timestamp")
        if order_date:
            self.order_date = dt_from_ts(order_date)
        elif not self.order_date:
            self.order_date = dt_now()

        self.ft_is_open = True
        if self.status in NON_OPEN_EXCHANGE_STATES:
            self.ft_is_open = False
            if (order.get("filled", 0.0) or 0.0) > 0 and not self.order_filled_date:
                self.order_filled_date = dt_from_ts(
                    safe_value_fallback(order, "lastTradeTimestamp", default_value=dt_ts())
                )
        self.order_update_date = datetime.now(UTC)

    def to_ccxt_object(self, stopPriceName: str = "stopPrice") -> dict[str, Any]:
        order: dict[str, Any] = {
            "id": self.order_id,
            "symbol": self.ft_pair,
            "price": self.price,
            "average": self.average,
            "amount": self.amount,
            "cost": self.cost,
            "type": self.order_type,
            "side": self.ft_order_side,
            "filled": self.filled,
            "remaining": self.remaining,
            "datetime": self.order_date_utc.strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "timestamp": int(self.order_date_utc.timestamp() * 1000),
            "status": self.status,
            "fee": None,
            "info": {},
        }
        if self.ft_order_side == "stoploss":
            order.update({stopPriceName: self.stop_price, "ft_order_type": "stoploss"})
        return order

    def to_json(self, entry_side: str, minified: bool = False) -> dict[str, Any]:
        resp: dict[str, Any] = {
            "amount": self.safe_amount,
            "safe_price": self.safe_price,
            "ft_order_side": self.ft_order_side,
            "order_filled_timestamp": dt_ts_none(self.order_filled_utc),
            "ft_is_entry": self.ft_order_side == entry_side,
            "ft_order_tag": self.ft_order_tag,
            "cost": self.cost if self.cost else 0,
        }
        if not minified:
            resp.update(
                {
                    "pair": self.ft_pair,
                    "order_id": self.order_id,
                    "status": self.status,
                    "average": round(self.average, 8) if self.average else 0,
                    "filled": self.filled,
                    "is_open": self.ft_is_open,
                    "order_date": (
                        self.order_date.strftime(DATETIME_PRINT_FORMAT)
                        if self.order_date
                        else None
                    ),
                    "order_timestamp": (
                        int(self.order_date.replace(tzinfo=UTC).timestamp() * 1000)
                        if self.order_date
                        else None
                    ),
                    "order_filled_date": (
                        self.order_filled_date.strftime(DATETIME_PRINT_FORMAT)
                        if self.order_filled_date
                        else None
                    ),
                    "order_type": self.order_type,
                    "price": self.price,
                    "remaining": self.remaining,
                    "ft_fee_base": self.ft_fee_base,
                    "funding_fee": self.funding_fee,
                }
            )
        return resp

    def close_bt_order(self, close_date: datetime, trade: "LocalTrade") -> None:
        self.order_filled_date = close_date
        self.filled = self.amount
        self.remaining = 0
        self.status = "closed"
        self.ft_is_open = False
        self.funding_fee = trade.funding_fee_running
        trade.funding_fee_running = 0.0

        if self.ft_order_side == trade.entry_side and self.price:
            trade.open_rate = self.price
            trade.recalc_trade_from_orders()
            if trade.nr_of_successful_entries == 1:
                trade.initial_stop_loss_pct = None
                trade.is_stop_loss_trailing = False
            trade.adjust_stop_loss(trade.open_rate, trade.stop_loss_pct)

    @staticmethod
    def update_orders(orders: list["Order"], order: CcxtOrder) -> None:
        if not isinstance(order, dict):
            logger.warning(f"{order} is not a valid response object.")
            return

        filtered_orders = [o for o in orders if o.order_id == order.get("id")]
        if filtered_orders:
            oobj = filtered_orders[0]
            oobj.update_from_ccxt_object(order)
            Trade.commit()
        else:
            logger.warning(f"Did not find order for {order}.")

    @classmethod
    def parse_from_ccxt_object(
        cls,
        order: CcxtOrder,
        pair: str,
        side: str,
        amount: float | None = None,
        price: float | None = None,
    ) -> Self:
        o = cls(
            order_id=str(order["id"]),
            ft_order_side=side,
            ft_pair=pair,
            ft_amount=amount or order.get("amount", None) or 0.0,
            ft_price=price or order.get("price", None),
        )
        o.update_from_ccxt_object(order)
        return o

    @staticmethod
    def get_open_orders() -> Sequence["Order"]:
        return [o for o in Order._instances if o.ft_is_open]  # type: ignore[arg-type]

    @staticmethod
    def order_by_id(order_id: str) -> Optional["Order"]:
        for o in Order._instances:
            if o.order_id == order_id:
                return o  # type: ignore[return-value]
        return None


# ---------------------------------------------------------------------------
#  LocalTrade -- backtesting / in-memory trade
# ---------------------------------------------------------------------------


class LocalTrade:
    """In-memory trade. ``Trade`` extends this with persistence semantics."""

    use_db: bool = False
    bt_trades: list["LocalTrade"] = []
    bt_trades_open: list["LocalTrade"] = []
    bt_trades_open_pp: dict[str, list["LocalTrade"]] = defaultdict(list)
    bt_open_open_trade_count: int = 0
    bt_total_profit: float = 0
    realized_profit: float = 0

    id: int = 0

    orders: list[Order] = []

    exchange: str = ""
    pair: str = ""
    base_currency: str | None = ""
    stake_currency: str | None = ""
    is_open: bool = True
    fee_open: float = 0.0
    fee_open_cost: float | None = None
    fee_open_currency: str | None = ""
    fee_close: float | None = 0.0
    fee_close_cost: float | None = None
    fee_close_currency: str | None = ""
    open_rate: float = 0.0
    open_rate_requested: float | None = None
    open_trade_value: float = 0.0
    close_rate: float | None = None
    close_rate_requested: float | None = None
    close_profit: float | None = None
    close_profit_abs: float | None = None
    stake_amount: float = 0.0
    max_stake_amount: float | None = 0.0
    amount: float = 0.0
    amount_requested: float | None = None
    open_date: datetime
    close_date: datetime | None = None
    stop_loss: float = 0.0
    stop_loss_pct: float | None = 0.0
    initial_stop_loss: float | None = 0.0
    initial_stop_loss_pct: float | None = None
    is_stop_loss_trailing: bool = False
    max_rate: float | None = None
    min_rate: float | None = None
    exit_reason: str | None = ""
    exit_order_status: str | None = ""
    strategy: str | None = ""
    enter_tag: str | None = None
    timeframe: int | None = None

    trading_mode: TradingMode = TradingMode.SPOT
    amount_precision: float | None = None
    price_precision: float | None = None
    precision_mode: int | None = None
    precision_mode_price: int | None = None
    contract_size: float | None = None

    liquidation_price: float | None = None
    is_short: bool = False
    leverage: float = 1.0

    interest_rate: float = 0.0

    funding_fees: float | None = None
    funding_fee_running: float | None = None
    record_version: int = 2

    @property
    def stoploss_or_liquidation(self) -> float:
        if self.liquidation_price:
            if self.is_short:
                return min(self.stop_loss, self.liquidation_price)
            return max(self.stop_loss, self.liquidation_price)
        return self.stop_loss

    @property
    def buy_tag(self) -> str | None:
        return self.enter_tag

    @property
    def has_no_leverage(self) -> bool:
        return (self.leverage == 1.0 or self.leverage is None) and not self.is_short

    @property
    def borrowed(self) -> float:
        if self.has_no_leverage:
            return 0.0
        if not self.is_short:
            return (self.amount * self.open_rate) * ((self.leverage - 1) / self.leverage)
        return self.amount

    @property
    def _date_last_filled_utc(self) -> datetime | None:
        orders = self.select_filled_orders()
        if orders:
            return max(o.order_filled_utc for o in orders if o.order_filled_utc)
        return None

    @property
    def date_last_filled_utc(self) -> datetime:
        dt_last_filled = self._date_last_filled_utc
        if not dt_last_filled:
            return self.open_date_utc
        return max([self.open_date_utc, dt_last_filled])

    @property
    def date_entry_fill_utc(self) -> datetime | None:
        orders = self.select_filled_orders(self.entry_side)
        if orders and len(
            filled_date := [o.order_filled_utc for o in orders if o.order_filled_utc]
        ):
            return min(filled_date)
        return None

    @property
    def open_date_utc(self) -> datetime:
        return self.open_date.replace(tzinfo=UTC)

    @property
    def stoploss_last_update_utc(self) -> datetime | None:
        if self.has_open_sl_orders:
            return max(o.order_date_utc for o in self.open_sl_orders)
        return None

    @property
    def close_date_utc(self) -> datetime | None:
        return self.close_date.replace(tzinfo=UTC) if self.close_date else None

    @property
    def entry_side(self) -> str:
        return "sell" if self.is_short else "buy"

    @property
    def exit_side(self) -> BuySell:
        return "buy" if self.is_short else "sell"

    @property
    def trade_direction(self) -> LongShort:
        return "short" if self.is_short else "long"

    @property
    def safe_base_currency(self) -> str:
        try:
            return self.base_currency or self.pair.split("/")[0]
        except IndexError:
            return ""

    @property
    def safe_quote_currency(self) -> str:
        try:
            return self.stake_currency or self.pair.split("/")[1].split(":")[0]
        except IndexError:
            return ""

    @property
    def open_orders(self) -> list[Order]:
        return [o for o in self.orders if o.ft_is_open and o.ft_order_side != "stoploss"]

    @property
    def has_open_orders(self) -> bool:
        return any(o for o in self.orders if o.ft_order_side != "stoploss" and o.ft_is_open)

    @property
    def has_open_position(self) -> bool:
        return self.amount > 0

    @property
    def open_sl_orders(self) -> list[Order]:
        return [o for o in self.orders if o.ft_order_side == "stoploss" and o.ft_is_open]

    @property
    def has_open_sl_orders(self) -> bool:
        return any(o for o in self.orders if o.ft_order_side == "stoploss" and o.ft_is_open)

    @property
    def sl_orders(self) -> list[Order]:
        return [o for o in self.orders if o.ft_order_side == "stoploss"]

    @property
    def open_orders_ids(self) -> list[str]:
        return [oo.order_id for oo in self.open_orders if oo.ft_order_side != "stoploss"]

    def __init__(self, **kwargs: Any) -> None:
        self.orders = []
        for key, value in kwargs.items():
            setattr(self, key, value)
        self.recalc_open_trade_value()
        if self.trading_mode == TradingMode.MARGIN and self.interest_rate is None:
            raise OperationalException(
                f"{self.trading_mode} trading requires param interest_rate on trades"
            )

    def __repr__(self) -> str:
        open_since = (
            self.open_date_utc.strftime(DATETIME_PRINT_FORMAT) if self.is_open else "closed"
        )
        return (
            f"Trade(id={self.id}, pair={self.pair}, amount={round_value(self.amount, 8)}, "
            f"is_short={self.is_short or False}, "
            f"leverage={round_value(self.leverage or 1.0, 1)}, "
            f"open_rate={round_value(self.open_rate, 8)}, open_since={open_since})"
        )

    def to_json(self, minified: bool = False) -> dict[str, Any]:
        filled_or_open_orders = self.select_filled_or_open_orders()
        orders_json = [order.to_json(self.entry_side, minified) for order in filled_or_open_orders]

        return {
            "trade_id": self.id,
            "pair": self.pair,
            "base_currency": self.safe_base_currency,
            "quote_currency": self.safe_quote_currency,
            "is_open": self.is_open,
            "exchange": self.exchange,
            "amount": round(self.amount, 8),
            "amount_requested": round(self.amount_requested, 8) if self.amount_requested else None,
            "stake_amount": round(self.stake_amount, 8),
            "max_stake_amount": round(self.max_stake_amount, 8) if self.max_stake_amount else None,
            "strategy": self.strategy,
            "enter_tag": self.enter_tag,
            "timeframe": self.timeframe,
            "fee_open": self.fee_open,
            "fee_open_cost": self.fee_open_cost,
            "fee_open_currency": self.fee_open_currency,
            "fee_close": self.fee_close,
            "fee_close_cost": self.fee_close_cost,
            "fee_close_currency": self.fee_close_currency,
            "open_date": self.open_date.strftime(DATETIME_PRINT_FORMAT),
            "open_timestamp": dt_ts_none(self.open_date_utc),
            "open_fill_date": (
                self.date_entry_fill_utc.strftime(DATETIME_PRINT_FORMAT)
                if self.date_entry_fill_utc
                else None
            ),
            "open_fill_timestamp": dt_ts_none(self.date_entry_fill_utc),
            "open_rate": self.open_rate,
            "open_rate_requested": self.open_rate_requested,
            "open_trade_value": round(self.open_trade_value, 8),
            "close_date": (
                self.close_date.strftime(DATETIME_PRINT_FORMAT) if self.close_date else None
            ),
            "close_timestamp": dt_ts_none(self.close_date_utc),
            "realized_profit": self.realized_profit or 0.0,
            "realized_profit_ratio": self.close_profit or None,
            "close_rate": self.close_rate,
            "close_rate_requested": self.close_rate_requested,
            "close_profit": self.close_profit,
            "close_profit_pct": round(self.close_profit * 100, 2) if self.close_profit else None,
            "close_profit_abs": self.close_profit_abs,
            "trade_duration_s": (
                int((self.close_date_utc - self.open_date_utc).total_seconds())
                if self.close_date
                else None
            ),
            "trade_duration": (
                int((self.close_date_utc - self.open_date_utc).total_seconds() // 60)
                if self.close_date
                else None
            ),
            "profit_ratio": self.close_profit,
            "profit_pct": round(self.close_profit * 100, 2) if self.close_profit else None,
            "profit_abs": self.close_profit_abs,
            "exit_reason": self.exit_reason,
            "exit_order_status": self.exit_order_status,
            "stop_loss_abs": self.stop_loss,
            "stop_loss_ratio": self.stop_loss_pct if self.stop_loss_pct else None,
            "stop_loss_pct": (self.stop_loss_pct * 100) if self.stop_loss_pct else None,
            "stoploss_last_update": (
                self.stoploss_last_update_utc.strftime(DATETIME_PRINT_FORMAT)
                if self.stoploss_last_update_utc
                else None
            ),
            "stoploss_last_update_timestamp": dt_ts_none(self.stoploss_last_update_utc),
            "initial_stop_loss_abs": self.initial_stop_loss,
            "initial_stop_loss_ratio": (
                self.initial_stop_loss_pct if self.initial_stop_loss_pct else None
            ),
            "initial_stop_loss_pct": (
                self.initial_stop_loss_pct * 100 if self.initial_stop_loss_pct else None
            ),
            "min_rate": self.min_rate,
            "max_rate": self.max_rate,
            "leverage": self.leverage,
            "interest_rate": self.interest_rate,
            "liquidation_price": self.liquidation_price,
            "is_short": self.is_short,
            "trading_mode": self.trading_mode,
            "funding_fees": self.funding_fees,
            "amount_precision": self.amount_precision,
            "price_precision": self.price_precision,
            "precision_mode": self.precision_mode,
            "precision_mode_price": self.precision_mode_price,
            "contract_size": self.contract_size,
            "nr_of_successful_entries": self.nr_of_successful_entries,
            "nr_of_successful_exits": self.nr_of_successful_exits,
            "has_open_orders": self.has_open_orders,
            "orders": orders_json,
        }

    @staticmethod
    def reset_trades() -> None:
        LocalTrade.bt_trades = []
        LocalTrade.bt_trades_open = []
        LocalTrade.bt_trades_open_pp = defaultdict(list)
        LocalTrade.bt_open_open_trade_count = 0
        LocalTrade.bt_total_profit = 0

    def adjust_min_max_rates(self, current_price: float, current_price_low: float) -> None:
        self.max_rate = max(current_price, self.max_rate or self.open_rate)
        self.min_rate = min(current_price_low, self.min_rate or self.open_rate)

    def set_liquidation_price(self, liquidation_price: float | None) -> None:
        if liquidation_price is None:
            return
        self.liquidation_price = price_to_precision(
            liquidation_price, self.price_precision, self.precision_mode_price
        )

    def set_funding_fees(self, funding_fee: float) -> None:
        if funding_fee is None:
            return
        self.funding_fee_running = funding_fee
        prior_funding_fees = sum(o.funding_fee for o in self.orders if o.funding_fee)
        self.funding_fees = prior_funding_fees + funding_fee

    def __set_stop_loss(self, stop_loss: float, percent: float) -> None:
        if not self.stop_loss:
            self.initial_stop_loss = stop_loss
        self.stop_loss = stop_loss
        self.stop_loss_pct = -1 * abs(percent)

    def adjust_stop_loss(
        self,
        current_price: float,
        stoploss: float | None,
        initial: bool = False,
        allow_refresh: bool = False,
    ) -> None:
        if stoploss is None or (initial and not (self.stop_loss is None or self.stop_loss == 0)):
            return

        leverage = self.leverage or 1.0
        if self.is_short:
            new_loss = float(current_price * (1 + abs(stoploss / leverage)))
        else:
            new_loss = float(current_price * (1 - abs(stoploss / leverage)))

        stop_loss_norm = price_to_precision(
            new_loss,
            self.price_precision,
            self.precision_mode_price,
            rounding_mode=ROUND_DOWN if self.is_short else ROUND_UP,
        )
        if self.initial_stop_loss_pct is None:
            self.__set_stop_loss(stop_loss_norm, stoploss)
            self.initial_stop_loss = price_to_precision(
                stop_loss_norm,
                self.price_precision,
                self.precision_mode_price,
                rounding_mode=ROUND_DOWN if self.is_short else ROUND_UP,
            )
            self.initial_stop_loss_pct = -1 * abs(stoploss)
        else:
            higher_stop = stop_loss_norm > self.stop_loss
            lower_stop = stop_loss_norm < self.stop_loss
            if (
                allow_refresh
                or (higher_stop and not self.is_short)
                or (lower_stop and self.is_short)
            ):
                logger.debug(f"{self.pair} - Adjusting stoploss...")
                if not allow_refresh:
                    self.is_stop_loss_trailing = True
                self.__set_stop_loss(stop_loss_norm, stoploss)
            else:
                logger.debug(f"{self.pair} - Keeping current stoploss...")

        logger.debug(
            f"{self.pair} - Stoploss adjusted. current_price={current_price:.8f}, "
            f"open_rate={self.open_rate:.8f}, max_rate={self.max_rate or self.open_rate:.8f}, "
            f"initial_stop_loss={self.initial_stop_loss:.8f}, "
            f"stop_loss={self.stop_loss:.8f}. "
            f"Trailing stoploss saved us: "
            f"{float(self.stop_loss) - float(self.initial_stop_loss or 0.0):.8f}."
        )

    def update_trade(self, order: Order, recalculating: bool = False) -> None:
        if order.status == "open" or order.safe_price is None:
            return

        logger.info(f"Updating trade (id={self.id}) ...")
        if order.ft_order_side != "stoploss":
            order.funding_fee = self.funding_fee_running
            self.funding_fee_running = 0.0
        order_type = order.order_type.upper() if order.order_type else None

        if order.ft_order_side == self.entry_side:
            self.open_rate = order.safe_price
            self.amount = order.safe_amount_after_fee
            if self.is_open:
                payment = "SELL" if self.is_short else "BUY"
                logger.info(f"{order_type}_{payment} has been fulfilled for {self}.")
            self.recalc_trade_from_orders()
        elif order.ft_order_side == self.exit_side:
            if self.is_open:
                payment = "BUY" if self.is_short else "SELL"
                logger.info(f"{order_type}_{payment} has been fulfilled for {self}.")
        elif order.ft_order_side == "stoploss" and order.status not in ("open",):
            self.close_rate_requested = self.stop_loss
            self.exit_reason = ExitType.STOPLOSS_ON_EXCHANGE.value
            if self.is_open and order.safe_filled > 0:
                logger.info(f"{order_type} is hit for {self}.")
        else:
            raise ValueError(f"Unknown order type: {order.order_type}")

        if order.ft_order_side != self.entry_side:
            amount_tr = amount_to_contract_precision(
                self.amount, self.amount_precision, self.precision_mode, self.contract_size
            )
            if isclose(order.safe_amount_after_fee, amount_tr, abs_tol=MATH_CLOSE_PREC) or (
                not recalculating and order.safe_amount_after_fee > amount_tr
            ):
                self.close(order.safe_price)
            else:
                self.recalc_trade_from_orders()

        Trade.commit()

    def close(self, rate: float, *, show_msg: bool = True) -> None:
        self.close_rate = rate
        self.close_date = self.close_date or self._date_last_filled_utc or dt_now()
        self.is_open = False
        self.exit_order_status = "closed"
        self.recalc_trade_from_orders(is_closing=True)
        if show_msg:
            logger.info(
                f"Marking {self} as closed as the trade is fulfilled "
                "and found no open orders for it."
            )

    def update_fee(
        self, fee_cost: float, fee_currency: str | None, fee_rate: float | None, side: str
    ) -> None:
        if self.entry_side == side and self.fee_open_currency is None:
            self.fee_open_cost = fee_cost
            self.fee_open_currency = fee_currency
            if fee_rate is not None:
                self.fee_open = fee_rate
                self.fee_close = fee_rate
        elif self.exit_side == side and self.fee_close_currency is None:
            self.fee_close_cost = fee_cost
            self.fee_close_currency = fee_currency
            if fee_rate is not None:
                self.fee_close = fee_rate

    def fee_updated(self, side: str) -> bool:
        if self.entry_side == side:
            return self.fee_open_currency is not None
        if self.exit_side == side:
            return self.fee_close_currency is not None
        return False

    def update_order(self, order: CcxtOrder) -> None:
        Order.update_orders(self.orders, order)

    @property
    def fully_canceled_entry_order_count(self) -> int:
        return len(
            [
                o
                for o in self.orders
                if o.ft_order_side == self.entry_side
                and o.status in CANCELED_EXCHANGE_STATES
                and o.filled == 0
            ]
        )

    @property
    def canceled_exit_order_count(self) -> int:
        return len(
            [
                o
                for o in self.orders
                if o.ft_order_side == self.exit_side and o.status in CANCELED_EXCHANGE_STATES
            ]
        )

    def get_canceled_exit_order_count(self) -> int:
        return self.canceled_exit_order_count

    def _calc_open_trade_value(self, amount: float, open_rate: float) -> float:
        open_value = FtPrecise(amount) * FtPrecise(open_rate)
        fees = open_value * FtPrecise(self.fee_open)
        if self.is_short:
            return float(open_value - fees)
        return float(open_value + fees)

    def recalc_open_trade_value(self) -> None:
        self.open_trade_value = self._calc_open_trade_value(self.amount, self.open_rate)

    def calculate_interest(self) -> FtPrecise:
        zero = FtPrecise(0.0)
        if self.trading_mode != TradingMode.MARGIN or self.has_no_leverage:
            return zero

        open_date = self.open_date.replace(tzinfo=None)
        now = (self.close_date or datetime.now(UTC)).replace(tzinfo=None)
        sec_per_hour = FtPrecise(3600)
        total_seconds = FtPrecise((now - open_date).total_seconds())
        hours = total_seconds / sec_per_hour or zero

        rate = FtPrecise(self.interest_rate)
        borrowed = FtPrecise(self.borrowed)
        return interest(exchange_name=self.exchange, borrowed=borrowed, rate=rate, hours=hours)

    def _calc_base_close(self, amount: FtPrecise, rate: float, fee: float | None) -> FtPrecise:
        close_value = amount * FtPrecise(rate)
        fees = close_value * FtPrecise(fee or 0.0)
        if self.is_short:
            return close_value + fees
        return close_value - fees

    def calc_close_trade_value(self, rate: float, amount: float | None = None) -> float:
        if rate is None and not self.close_rate:
            return 0.0

        amount1 = FtPrecise(amount or self.amount)
        trading_mode = self.trading_mode or TradingMode.SPOT

        if trading_mode == TradingMode.SPOT:
            return float(self._calc_base_close(amount1, rate, self.fee_close))
        if trading_mode == TradingMode.MARGIN:
            total_interest = self.calculate_interest()
            if self.is_short:
                amount1 = amount1 + total_interest
                return float(self._calc_base_close(amount1, rate, self.fee_close))
            return float(self._calc_base_close(amount1, rate, self.fee_close) - total_interest)
        if trading_mode == TradingMode.FUTURES:
            funding_fees = self.funding_fees or 0.0
            if self.is_short:
                return float(self._calc_base_close(amount1, rate, self.fee_close)) - funding_fees
            return float(self._calc_base_close(amount1, rate, self.fee_close)) + funding_fees
        raise OperationalException(
            f"{self.trading_mode} trading is not yet available using VulcanTrader"
        )

    def calc_profit(
        self, rate: float, amount: float | None = None, open_rate: float | None = None
    ) -> float:
        return self.calculate_profit(rate, amount, open_rate).profit_abs

    def calculate_profit(
        self, rate: float, amount: float | None = None, open_rate: float | None = None
    ) -> ProfitStruct:
        close_trade_value = self.calc_close_trade_value(rate, amount)
        if amount is None or open_rate is None:
            open_trade_value = self.open_trade_value
        else:
            open_trade_value = self._calc_open_trade_value(amount, open_rate)

        if self.is_short:
            profit_abs = open_trade_value - close_trade_value
        else:
            profit_abs = close_trade_value - open_trade_value

        try:
            if self.is_short:
                profit_ratio = (1 - (close_trade_value / open_trade_value)) * self.leverage
            else:
                profit_ratio = ((close_trade_value / open_trade_value) - 1) * self.leverage
            profit_ratio = float(f"{profit_ratio:.8f}")
        except ZeroDivisionError:
            profit_ratio = 0.0

        total_profit_abs = profit_abs + self.realized_profit
        if self.max_stake_amount:
            max_stake = self.max_stake_amount * (
                (1 - self.fee_open) if self.is_short else (1 + self.fee_open)
            )
            total_profit_ratio = total_profit_abs / max_stake
            total_profit_ratio = float(f"{total_profit_ratio:.8f}")
        else:
            total_profit_ratio = 0.0
        profit_abs = float(f"{profit_abs:.8f}")
        total_profit_abs = float(f"{total_profit_abs:.8f}")

        return ProfitStruct(
            profit_abs=profit_abs,
            profit_ratio=profit_ratio,
            total_profit=total_profit_abs,
            total_profit_ratio=total_profit_ratio,
        )

    def calc_profit_ratio(
        self, rate: float, amount: float | None = None, open_rate: float | None = None
    ) -> float:
        close_trade_value = self.calc_close_trade_value(rate, amount)
        if (amount is None) and (open_rate is None):
            open_trade_value = self.open_trade_value
        else:
            open_trade_value = self._calc_open_trade_value(
                amount or self.amount, open_rate or self.open_rate
            )

        if open_trade_value == 0.0:
            return 0.0
        if self.is_short:
            profit_ratio = (1 - (close_trade_value / open_trade_value)) * self.leverage
        else:
            profit_ratio = ((close_trade_value / open_trade_value) - 1) * self.leverage
        return float(f"{profit_ratio:.8f}")

    def recalc_trade_from_orders(self, *, is_closing: bool = False) -> None:
        ZERO = FtPrecise(0.0)
        current_amount = FtPrecise(0.0)
        current_stake = FtPrecise(0.0)
        max_stake_amount = FtPrecise(0.0)
        total_stake = 0.0
        avg_price = FtPrecise(0.0)
        close_profit = 0.0
        close_profit_abs = 0.0
        self.funding_fees = 0.0
        funding_fees = 0.0
        ordercount = len(self.orders) - 1
        prof = None
        for i, o in enumerate(self.orders):
            if o.ft_is_open or not o.filled:
                continue
            funding_fees += o.funding_fee or 0.0
            tmp_amount = FtPrecise(o.safe_amount_after_fee)
            tmp_price = FtPrecise(o.safe_price)

            is_exit = o.ft_order_side != self.entry_side
            side = FtPrecise(-1 if is_exit else 1)
            price = tmp_price
            if tmp_amount > ZERO and tmp_price is not None:
                current_amount += tmp_amount * side
                price = avg_price if is_exit else tmp_price
                current_stake += price * tmp_amount * side
                if current_amount > ZERO and not is_exit:
                    avg_price = current_stake / current_amount

            if is_exit:
                if i == ordercount and is_closing:
                    self.funding_fees = funding_fees
                exit_rate = o.safe_price
                exit_amount = o.safe_amount_after_fee
                prof = self.calculate_profit(exit_rate, exit_amount, float(avg_price))
                close_profit_abs += prof.profit_abs
                if total_stake > 0:
                    close_profit = (close_profit_abs / total_stake) * self.leverage
            else:
                total_stake += self._calc_open_trade_value(tmp_amount, price)
                max_stake_amount += tmp_amount * price
        self.funding_fees = funding_fees
        self.max_stake_amount = float(max_stake_amount) / (self.leverage or 1.0)

        if close_profit and prof is not None:
            self.close_profit = close_profit
            self.realized_profit = close_profit_abs
            self.close_profit_abs = prof.profit_abs

        current_amount_tr = amount_to_contract_precision(
            float(current_amount), self.amount_precision, self.precision_mode, self.contract_size
        )
        if current_amount_tr > 0.0:
            self.open_rate = price_to_precision(
                float(current_stake / current_amount),
                self.price_precision,
                self.precision_mode_price,
            )
            self.amount = current_amount_tr
            self.stake_amount = float(current_stake) / (self.leverage or 1.0)
            self.fee_open_cost = self.fee_open * float(self.max_stake_amount)
            self.recalc_open_trade_value()
            if self.stop_loss_pct is not None and self.open_rate is not None:
                self.adjust_stop_loss(self.open_rate, self.stop_loss_pct)
        elif is_closing and total_stake > 0:
            self.close_profit = (close_profit_abs / total_stake) * self.leverage
            self.close_profit_abs = close_profit_abs

    def select_order_by_order_id(self, order_id: str) -> Order | None:
        for o in self.orders:
            if o.order_id == order_id:
                return o
        return None

    def select_order(
        self,
        order_side: str | None = None,
        is_open: bool | None = None,
        only_filled: bool = False,
    ) -> Order | None:
        orders = self.orders
        if order_side:
            orders = [o for o in orders if o.ft_order_side == order_side]
        if is_open is not None:
            orders = [o for o in orders if o.ft_is_open == is_open]
        if is_open is False and only_filled:
            orders = [o for o in orders if o.filled and o.status in NON_OPEN_EXCHANGE_STATES]
        if len(orders) > 0:
            return orders[-1]
        return None

    def select_filled_orders(self, order_side: str | None = None) -> list[Order]:
        return [
            o
            for o in self.orders
            if ((o.ft_order_side == order_side) or (order_side is None))
            and o.ft_is_open is False
            and o.filled
            and o.status in NON_OPEN_EXCHANGE_STATES
        ]

    def select_filled_or_open_orders(self) -> list[Order]:
        return [
            o
            for o in self.orders
            if (
                o.ft_is_open is False
                and (o.filled or 0) > 0
                and o.status in NON_OPEN_EXCHANGE_STATES
            )
            or (o.ft_is_open is True and o.status is not None)
        ]

    def set_custom_data(self, key: str, value: Any) -> None:
        CustomDataWrapper.set_custom_data(trade_id=self.id, key=key, value=value)

    def get_custom_data(self, key: str, default: Any = None) -> Any:
        data = CustomDataWrapper.get_custom_data(trade_id=self.id, key=key)
        if data:
            return data[0].value
        return default

    def get_custom_data_entry(self, key: str) -> _CustomData | None:
        data = CustomDataWrapper.get_custom_data(trade_id=self.id, key=key)
        if data:
            return data[0]
        return None

    def get_all_custom_data(self) -> list[_CustomData]:
        return CustomDataWrapper.get_custom_data(trade_id=self.id)

    @property
    def nr_of_successful_entries(self) -> int:
        return len(self.select_filled_orders(self.entry_side))

    @property
    def nr_of_successful_exits(self) -> int:
        return len(self.select_filled_orders(self.exit_side))

    @property
    def nr_of_successful_buys(self) -> int:
        return len(self.select_filled_orders("buy"))

    @property
    def nr_of_successful_sells(self) -> int:
        return len(self.select_filled_orders("sell"))

    @property
    def sell_reason(self) -> str | None:
        return self.exit_reason

    @property
    def safe_close_rate(self) -> float:
        return self.close_rate or self.close_rate_requested or 0.0

    @staticmethod
    def get_trades_proxy(
        *,
        pair: str | None = None,
        is_open: bool | None = None,
        open_date: datetime | None = None,
        close_date: datetime | None = None,
    ) -> list["LocalTrade"]:
        if is_open is not None:
            sel_trades = LocalTrade.bt_trades_open if is_open else LocalTrade.bt_trades
        else:
            sel_trades = list(LocalTrade.bt_trades + LocalTrade.bt_trades_open)

        if pair:
            sel_trades = [t for t in sel_trades if t.pair == pair]
        if open_date:
            sel_trades = [t for t in sel_trades if t.open_date > open_date]
        if close_date:
            sel_trades = [t for t in sel_trades if t.close_date and t.close_date > close_date]
        return sel_trades

    @staticmethod
    def close_bt_trade(trade: "LocalTrade") -> None:
        LocalTrade.bt_trades_open.remove(trade)
        LocalTrade.bt_trades_open_pp[trade.pair].remove(trade)
        LocalTrade.bt_open_open_trade_count -= 1
        LocalTrade.bt_trades.append(trade)
        LocalTrade.bt_total_profit += trade.close_profit_abs

    @staticmethod
    def add_bt_trade(trade: "LocalTrade") -> None:
        if trade.is_open:
            LocalTrade.bt_trades_open.append(trade)
            LocalTrade.bt_trades_open_pp[trade.pair].append(trade)
            LocalTrade.bt_open_open_trade_count += 1
        else:
            LocalTrade.bt_trades.append(trade)

    @staticmethod
    def remove_bt_trade(trade: "LocalTrade") -> None:
        LocalTrade.bt_trades_open.remove(trade)
        LocalTrade.bt_trades_open_pp[trade.pair].remove(trade)
        LocalTrade.bt_open_open_trade_count -= 1

    @staticmethod
    def get_open_trades() -> list[Any]:
        return Trade.get_trades_proxy(is_open=True)

    @staticmethod
    def get_open_trade_count() -> int:
        if Trade.use_db:
            return sum(1 for t in Trade._instances if t.is_open)
        return LocalTrade.bt_open_open_trade_count

    @staticmethod
    def stoploss_reinitialization(desired_stoploss: float) -> None:
        for trade in Trade.get_open_trades():
            logger.info(f"Found open trade: {trade}")
            if not trade.is_stop_loss_trailing and trade.initial_stop_loss_pct != desired_stoploss:
                logger.info(f"Stoploss for {trade} needs adjustment...")
                trade.stop_loss = 0.0
                trade.initial_stop_loss_pct = None
                trade.adjust_stop_loss(trade.open_rate, desired_stoploss)
                logger.info(f"New stoploss: {trade.stop_loss}.")

    @classmethod
    def from_json(cls, json_str: str) -> Self:
        from uuid import uuid4

        import rapidjson

        data = rapidjson.loads(json_str)
        trade = cls(
            __FROM_JSON=True,
            id=data.get("trade_id"),
            pair=data["pair"],
            base_currency=data.get("base_currency"),
            stake_currency=data.get("quote_currency"),
            is_open=data["is_open"],
            exchange=data.get("exchange", "import"),
            amount=data["amount"],
            amount_requested=data.get("amount_requested", data["amount"]),
            stake_amount=data["stake_amount"],
            strategy=data.get("strategy"),
            enter_tag=data["enter_tag"],
            timeframe=data.get("timeframe"),
            fee_open=data["fee_open"],
            fee_open_cost=data.get("fee_open_cost"),
            fee_open_currency=data.get("fee_open_currency"),
            fee_close=data["fee_close"],
            fee_close_cost=data.get("fee_close_cost"),
            fee_close_currency=data.get("fee_close_currency"),
            open_date=datetime.fromtimestamp(data["open_timestamp"] // 1000, tz=UTC),
            open_rate=data["open_rate"],
            open_rate_requested=data.get("open_rate_requested", data["open_rate"]),
            open_trade_value=data.get("open_trade_value"),
            close_date=(
                datetime.fromtimestamp(data["close_timestamp"] // 1000, tz=UTC)
                if data["close_timestamp"]
                else None
            ),
            realized_profit=data.get("realized_profit", 0),
            close_rate=data["close_rate"],
            close_rate_requested=data.get("close_rate_requested", data["close_rate"]),
            close_profit=data.get("close_profit", data.get("profit_ratio")),
            close_profit_abs=data.get("close_profit_abs", data.get("profit_abs")),
            exit_reason=data["exit_reason"],
            exit_order_status=data.get("exit_order_status"),
            stop_loss=data["stop_loss_abs"],
            stop_loss_pct=data["stop_loss_ratio"],
            initial_stop_loss=data["initial_stop_loss_abs"],
            initial_stop_loss_pct=data["initial_stop_loss_ratio"],
            min_rate=data["min_rate"],
            max_rate=data["max_rate"],
            leverage=data["leverage"],
            interest_rate=data.get("interest_rate"),
            liquidation_price=data.get("liquidation_price"),
            is_short=data["is_short"],
            trading_mode=data.get("trading_mode"),
            funding_fees=data.get("funding_fees"),
            amount_precision=data.get("amount_precision", None),
            price_precision=data.get("price_precision", None),
            precision_mode=data.get("precision_mode", None),
            precision_mode_price=data.get(
                "precision_mode_price", data.get("precision_mode", None)
            ),
            contract_size=data.get("contract_size", None),
        )
        for order in data["orders"]:
            order_obj = Order(
                amount=order["amount"],
                ft_amount=order["amount"],
                ft_order_side=order["ft_order_side"],
                ft_pair=order.get("pair", data["pair"]),
                ft_is_open=order.get("is_open", False),
                order_id=order.get("order_id", uuid4().hex),
                status=order.get("status"),
                average=order.get("average", order.get("safe_price")),
                cost=order["cost"],
                filled=order.get("filled", order["amount"]),
                order_date=datetime.strptime(order["order_date"], DATETIME_PRINT_FORMAT)
                if order.get("order_date")
                else None,
                order_filled_date=(
                    datetime.fromtimestamp(order["order_filled_timestamp"] // 1000, tz=UTC)
                    if order["order_filled_timestamp"]
                    else None
                ),
                order_type=order.get("order_type"),
                price=order.get("price", order.get("safe_price")),
                ft_price=order.get("price", order.get("safe_price")),
                remaining=order.get("remaining", 0.0),
                funding_fee=order.get("funding_fee", None),
                ft_order_tag=order.get("ft_order_tag", None),
                ft_fee_base=order.get("ft_fee_base", None),
            )
            trade.orders.append(order_obj)

        return trade


# ---------------------------------------------------------------------------
#  Trade -- persisted, JSON-backed
# ---------------------------------------------------------------------------


def _apply_filters(trades: Sequence["Trade"], trade_filter: TradeFilter) -> list["Trade"]:
    """Apply a callable predicate (or list of callables) to a sequence of trades."""
    if trade_filter is None:
        return list(trades)
    if callable(trade_filter):
        return [t for t in trades if trade_filter(t)]
    if isinstance(trade_filter, list):
        result: list[Trade] = list(trades)
        for f in trade_filter:
            if callable(f):
                result = [t for t in result if f(t)]
            else:
                logger.warning(
                    "Ignoring non-callable trade filter %r -- pass a lambda instead.", f
                )
        return result
    raise OperationalException(f"Unsupported trade filter type: {type(trade_filter)!r}")


class Trade(ModelBase, LocalTrade):
    """
    Persisted trade -- backed by a JSON file via
    :func:`VulcanTrader.persistence.models.init_db`.
    """

    __tablename__ = "trades"
    use_db: bool = True

    def __init__(self, **kwargs: Any) -> None:
        from_json = kwargs.pop("__FROM_JSON", None)
        # Default for ``record_version``.
        kwargs.setdefault("record_version", 2)
        super().__init__(**kwargs)
        if not from_json:
            self.realized_profit = 0
            self.recalc_open_trade_value()
        # Truncate long string fields to mimic the prior column constraints.
        if self.enter_tag:
            self.enter_tag = _truncate(self.enter_tag, CUSTOM_TAG_MAX_LENGTH)
        if self.exit_reason:
            self.exit_reason = _truncate(self.exit_reason, CUSTOM_TAG_MAX_LENGTH)

    # ----- serialisation ---------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        def _iso(d: datetime | None) -> str | None:
            return d.isoformat() if d else None

        return {
            "id": self.id,
            "exchange": self.exchange,
            "pair": self.pair,
            "base_currency": self.base_currency,
            "stake_currency": self.stake_currency,
            "is_open": self.is_open,
            "fee_open": self.fee_open,
            "fee_open_cost": self.fee_open_cost,
            "fee_open_currency": self.fee_open_currency,
            "fee_close": self.fee_close,
            "fee_close_cost": self.fee_close_cost,
            "fee_close_currency": self.fee_close_currency,
            "open_rate": self.open_rate,
            "open_rate_requested": self.open_rate_requested,
            "open_trade_value": self.open_trade_value,
            "close_rate": self.close_rate,
            "close_rate_requested": self.close_rate_requested,
            "realized_profit": self.realized_profit,
            "close_profit": self.close_profit,
            "close_profit_abs": self.close_profit_abs,
            "stake_amount": self.stake_amount,
            "max_stake_amount": self.max_stake_amount,
            "amount": self.amount,
            "amount_requested": self.amount_requested,
            "open_date": _iso(getattr(self, "open_date", None)),
            "close_date": _iso(self.close_date),
            "stop_loss": self.stop_loss,
            "stop_loss_pct": self.stop_loss_pct,
            "initial_stop_loss": self.initial_stop_loss,
            "initial_stop_loss_pct": self.initial_stop_loss_pct,
            "is_stop_loss_trailing": self.is_stop_loss_trailing,
            "max_rate": self.max_rate,
            "min_rate": self.min_rate,
            "exit_reason": self.exit_reason,
            "exit_order_status": self.exit_order_status,
            "strategy": self.strategy,
            "enter_tag": self.enter_tag,
            "timeframe": self.timeframe,
            "trading_mode": self.trading_mode.value
            if isinstance(self.trading_mode, TradingMode)
            else self.trading_mode,
            "amount_precision": self.amount_precision,
            "price_precision": self.price_precision,
            "precision_mode": self.precision_mode,
            "precision_mode_price": self.precision_mode_price,
            "contract_size": self.contract_size,
            "leverage": self.leverage,
            "is_short": self.is_short,
            "liquidation_price": self.liquidation_price,
            "interest_rate": self.interest_rate,
            "funding_fees": self.funding_fees,
            "funding_fee_running": self.funding_fee_running,
            "record_version": self.record_version,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Trade":
        def _parse(v: Any) -> datetime | None:
            if v is None:
                return None
            return datetime.fromisoformat(v) if isinstance(v, str) else v

        tm = data.get("trading_mode")
        if isinstance(tm, str):
            try:
                tm = TradingMode(tm)
            except ValueError:
                pass

        kwargs = dict(data)
        kwargs["trading_mode"] = tm
        kwargs["open_date"] = _parse(data.get("open_date"))
        kwargs["close_date"] = _parse(data.get("close_date"))
        kwargs["__FROM_JSON"] = True
        # Drop unknown keys silently.
        return cls(**kwargs)

    # ----- standard CRUD ---------------------------------------------------

    def delete(self) -> None:
        for order in list(self.orders):
            Order.session.delete(order)
        CustomDataWrapper.delete_custom_data(trade_id=self.id)
        Trade.session.delete(self)
        Trade.commit()

    @staticmethod
    def commit() -> None:
        Trade.session.commit()

    @staticmethod
    def rollback() -> None:
        Trade.session.rollback()

    # ----- queries ---------------------------------------------------------

    @staticmethod
    def get_trades_proxy(
        *,
        pair: str | None = None,
        is_open: bool | None = None,
        open_date: datetime | None = None,
        close_date: datetime | None = None,
    ) -> list["LocalTrade"]:
        if Trade.use_db:
            trades = list(Trade._instances)  # type: ignore[arg-type]
            if pair:
                trades = [t for t in trades if t.pair == pair]
            if open_date:
                trades = [t for t in trades if t.open_date > open_date]
            if close_date:
                trades = [
                    t for t in trades if t.close_date is not None and t.close_date > close_date
                ]
            if is_open is not None:
                trades = [t for t in trades if t.is_open is is_open]
            return trades  # type: ignore[return-value]
        return LocalTrade.get_trades_proxy(
            pair=pair, is_open=is_open, open_date=open_date, close_date=close_date
        )

    @staticmethod
    def get_trades(
        trade_filter: TradeFilter = None, include_orders: bool = True
    ) -> "_ResultProxy":
        """
        Return all trades matching ``trade_filter``.

        ``trade_filter`` may be ``None``, a single callable predicate
        (``lambda t: t.pair == 'BTC/USDT'``), or a list of such callables.

        The returned object exposes ``.all()`` and iteration -- preserving
        the prior call-site shape ``Trade.get_trades(...).all()``.
        """
        if not Trade.use_db:
            raise NotImplementedError("`Trade.get_trades()` not supported in backtesting mode.")
        trades = _apply_filters(list(Trade._instances), trade_filter)  # type: ignore[arg-type]
        return _ResultProxy(trades)

    @staticmethod
    def get_open_trades_without_assigned_fees() -> list["Trade"]:
        return [
            t
            for t in Trade._instances
            if t.fee_open_currency is None and t.is_open is True and t.orders
        ]  # type: ignore[return-value]

    @staticmethod
    def get_closed_trades_without_assigned_fees() -> list["Trade"]:
        return [
            t
            for t in Trade._instances
            if t.fee_close_currency is None and t.is_open is False and t.orders
        ]  # type: ignore[return-value]

    @staticmethod
    def get_total_closed_profit() -> float:
        if Trade.use_db:
            return sum(
                (t.close_profit_abs or 0)
                for t in Trade._instances
                if t.is_open is False
            )
        return sum(
            t.close_profit_abs  # type: ignore[misc]
            for t in LocalTrade.get_trades_proxy(is_open=False)
        ) or 0

    @staticmethod
    def total_open_trades_stakes() -> float:
        if Trade.use_db:
            return sum(t.stake_amount for t in Trade._instances if t.is_open is True) or 0
        return sum(t.stake_amount for t in LocalTrade.get_trades_proxy(is_open=True)) or 0

    # ----- aggregations / performance --------------------------------------

    @staticmethod
    def _generic_pair_costs(filters: list[Callable[["Trade"], bool]]) -> dict[int, float]:
        """Return ``{trade_id: total_buy_cost_per_leverage_unit}``."""
        costs: dict[int, float] = {}
        for t in Trade._instances:
            if not all(f(t) for f in filters):
                continue
            entry_side = "sell" if t.is_short else "buy"
            cost = 0.0
            for o in t.orders:
                if o.ft_order_side != entry_side:
                    continue
                if not o.filled or o.filled <= 0:
                    continue
                price = o.average or o.price or o.ft_price or 0.0
                amt = o.filled if o.filled is not None else (o.amount or 0.0)
                cost += (amt * price) / (t.leverage or 1)
            costs[t.id] = cost
        return costs

    @staticmethod
    def _grouped_performance(
        key_fn: Callable[["Trade"], Any],
        filters: list[Callable[["Trade"], bool]],
        fallback: str = "",
    ) -> list[tuple[Any, float, float, int]]:
        """
        Group trades by ``key_fn`` and return rows
        ``(key, profit_ratio, profit_sum_abs, count)``.
        """
        pair_costs = Trade._generic_pair_costs(filters)
        groups: dict[Any, dict[str, float]] = defaultdict(
            lambda: {"profit_sum_abs": 0.0, "count": 0, "cost": 0.0}
        )
        for t in Trade._instances:
            if not all(f(t) for f in filters):
                continue
            key = key_fn(t)
            if key is None:
                key = fallback
            groups[key]["profit_sum_abs"] += t.close_profit_abs or 0.0
            groups[key]["count"] += 1
            groups[key]["cost"] += pair_costs.get(t.id, 0.0)
        rows = []
        for key, agg in groups.items():
            ratio = (agg["profit_sum_abs"] / agg["cost"]) if agg["cost"] else 0.0
            rows.append((key, ratio, agg["profit_sum_abs"], int(agg["count"])))
        rows.sort(key=lambda r: r[2], reverse=True)
        return rows

    @staticmethod
    def get_overall_performance(start_date: datetime | None = None) -> list[dict[str, Any]]:
        filters: list[Callable[["Trade"], bool]] = [lambda t: t.is_open is False]
        if start_date:
            filters.append(lambda t: t.close_date is not None and t.close_date >= start_date)
        rows = Trade._grouped_performance(lambda t: t.pair, filters)
        return [
            {
                "pair": pair,
                "profit_ratio": profit,
                "profit": round(profit * 100, 2),
                "profit_pct": round(profit * 100, 2),
                "profit_abs": profit_abs,
                "count": count,
            }
            for pair, profit, profit_abs, count in rows
        ]

    @staticmethod
    def get_enter_tag_performance(pair: str | None) -> list[dict[str, Any]]:
        filters: list[Callable[["Trade"], bool]] = [lambda t: t.is_open is False]
        if pair is not None:
            filters.append(lambda t: t.pair == pair)
        rows = Trade._grouped_performance(lambda t: t.enter_tag, filters, "Other")
        return [
            {
                "enter_tag": enter_tag if enter_tag is not None else "Other",
                "profit_ratio": profit,
                "profit_pct": round(profit * 100, 2),
                "profit_abs": profit_abs,
                "count": count,
            }
            for enter_tag, profit, profit_abs, count in rows
        ]

    @staticmethod
    def get_exit_reason_performance(pair: str | None) -> list[dict[str, Any]]:
        filters: list[Callable[["Trade"], bool]] = [lambda t: t.is_open is False]
        if pair is not None:
            filters.append(lambda t: t.pair == pair)
        rows = Trade._grouped_performance(lambda t: t.exit_reason, filters, "Other")
        return [
            {
                "exit_reason": exit_reason if exit_reason is not None else "Other",
                "profit_ratio": profit,
                "profit_pct": round(profit * 100, 2),
                "profit_abs": profit_abs,
                "count": count,
            }
            for exit_reason, profit, profit_abs, count in rows
        ]

    @staticmethod
    def get_mix_tag_performance(pair: str | None) -> list[dict[str, Any]]:
        filters: list[Callable[["Trade"], bool]] = [lambda t: t.is_open is False]
        if pair is not None:
            filters.append(lambda t: t.pair == pair)
        resp: list[dict] = []
        for t in Trade._instances:
            if not all(f(t) for f in filters):
                continue
            enter_tag = t.enter_tag if t.enter_tag is not None else "Other"
            exit_reason = t.exit_reason if t.exit_reason is not None else "Other"
            mix_tag = f"{enter_tag} {exit_reason}"
            profit = t.close_profit or 0.0
            profit_abs = t.close_profit_abs or 0.0
            existing = next((item for item in resp if item["mix_tag"] == mix_tag), None)
            if existing is None:
                resp.append(
                    {
                        "mix_tag": mix_tag,
                        "profit_ratio": profit,
                        "profit_pct": round(profit * 100, 2),
                        "profit_abs": profit_abs,
                        "count": 1,
                    }
                )
            else:
                existing["profit_ratio"] += profit
                existing["profit_pct"] = round(existing["profit_ratio"] * 100, 2)
                existing["profit_abs"] += profit_abs
                existing["count"] += 1
        resp.sort(key=lambda r: r["profit_abs"], reverse=True)
        return resp

    @staticmethod
    def get_best_pair(
        trade_filter: TradeFilter = None,
    ) -> tuple[str, float, float, int] | None:
        filters: list[Callable[["Trade"], bool]] = [lambda t: t.is_open is False]
        if trade_filter is not None:
            if callable(trade_filter):
                filters.append(trade_filter)
            elif isinstance(trade_filter, list):
                for f in trade_filter:
                    if callable(f):
                        filters.append(f)
        rows = Trade._grouped_performance(lambda t: t.pair, filters)
        return rows[0] if rows else None

    @staticmethod
    def get_trading_volume(trade_filter: TradeFilter = None) -> float:
        filters: list[Callable[["Trade"], bool]] = []
        if trade_filter is not None:
            if callable(trade_filter):
                filters.append(trade_filter)
            elif isinstance(trade_filter, list):
                for f in trade_filter:
                    if callable(f):
                        filters.append(f)
        total = 0.0
        for t in Trade._instances:
            if not all(f(t) for f in filters):
                continue
            for o in t.orders:
                if o.status == "closed" and o.cost is not None:
                    total += o.cost
        return total or 0.0


class _ResultProxy:
    """Tiny wrapper so call sites can keep using ``Trade.get_trades(...).all()``."""

    def __init__(self, items: list[Trade]) -> None:
        self._items = items

    def all(self) -> list[Trade]:
        return list(self._items)

    def first(self) -> Trade | None:
        return self._items[0] if self._items else None

    def __iter__(self):
        return iter(self._items)

    def __len__(self) -> int:
        return len(self._items)
