"""Compatibility shim for legacy ``VulcanTrader.trader_types`` imports.

Re-exports the type aliases that live under :mod:`VulcanTrader.util`.
"""

from VulcanTrader.util.plot_annotation_type import (
    AnnotationType,
    AnnotationTypeTA,
    AreaAnnotationType,
    LineAnnotationType,
)
from VulcanTrader.util.valid_exchanges_type import TradeModeType, ValidExchangesType


__all__ = [
    "AnnotationType",
    "AnnotationTypeTA",
    "AreaAnnotationType",
    "LineAnnotationType",
    "TradeModeType",
    "ValidExchangesType",
]
