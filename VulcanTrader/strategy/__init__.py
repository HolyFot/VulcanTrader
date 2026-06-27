"""Public strategy API surface for VulcanTrader.

Mirrors the ``freqtrade.strategy`` re-exports so user strategies can write
``from VulcanTrader.strategy import IStrategy, BooleanParameter, ...``.
"""

from VulcanTrader.strategy.interface import IStrategy
from VulcanTrader.strategy.parameters import (
    BaseParameter,
    BooleanParameter,
    CategoricalParameter,
    DecimalParameter,
    IntParameter,
    NumericParameter,
    RealParameter,
)
from VulcanTrader.strategy.strategy_helper import merge_informative_pair, stoploss_from_open
from VulcanTrader.strategy.informative_decorator import informative

__all__ = [
    "BaseParameter",
    "BooleanParameter",
    "CategoricalParameter",
    "DecimalParameter",
    "informative",
    "IntParameter",
    "IStrategy",
    "NumericParameter",
    "RealParameter",
    "merge_informative_pair",
    "stoploss_from_open",
]
