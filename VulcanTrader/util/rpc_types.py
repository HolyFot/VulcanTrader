"""Minimal RPC message TypedDicts.

Ported (subset) from freqtrade.rpc.rpc_types — only what the rest of the
package actually imports. Extend as needed.
"""

from datetime import datetime
from typing import Any, Literal, TypedDict

from VulcanTrader.constants import PairWithTimeframe
from VulcanTrader.enums import RPCMessageType


class RPCSendMsgBase(TypedDict):
    pass


class _AnalyzedDFData(TypedDict):
    key: PairWithTimeframe
    df: Any
    la: datetime


class RPCAnalyzedDFMsg(RPCSendMsgBase):
    """New Analyzed dataframe message."""

    type: Literal[RPCMessageType.ANALYZED_DF]
    data: _AnalyzedDFData


class RPCNewCandleMsg(RPCSendMsgBase):
    """New candle ping message, issued once per new candle/pair."""

    type: Literal[RPCMessageType.NEW_CANDLE]
    data: PairWithTimeframe
