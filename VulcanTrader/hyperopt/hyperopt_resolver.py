"""
Resolver for hyperopt loss functions.

Loads a named IHyperOptLoss class from the built-in hyperopt_loss package
or from a user-supplied path.
"""

from __future__ import annotations

import importlib
import logging
from pathlib import Path

from VulcanTrader.constants import Config
from VulcanTrader.hyperopt.hyperopt_loss.hyperopt_loss_interface import IHyperOptLoss
from VulcanTrader.util.exceptions import OperationalException


logger = logging.getLogger(__name__)

# Map of built-in loss function names → module base names
_BUILTIN_LOSS_MAP: dict[str, str] = {
    "ShortTradeDurHyperOptLoss":         "hyperopt_loss_short_trade_dur",
    "OnlyProfitHyperOptLoss":            "hyperopt_loss_onlyprofit",
    "SharpeHyperOptLoss":                "hyperopt_loss_sharpe",
    "SharpeHyperOptLossDaily":           "hyperopt_loss_sharpe_daily",
    "SortinoHyperOptLoss":               "hyperopt_loss_sortino",
    "SortinoHyperOptLossDaily":          "hyperopt_loss_sortino_daily",
    "CalmarHyperOptLoss":                "hyperopt_loss_calmar",
    "MaxDrawDownHyperOptLoss":           "hyperopt_loss_max_drawdown",
    "MaxDrawDownRelativeHyperOptLoss":   "hyperopt_loss_max_drawdown_relative",
    "MaxDrawDownPerPairHyperOptLoss":    "hyperopt_loss_max_drawdown_per_pair",
    "ProfitDrawDownHyperOptLoss":        "hyperopt_loss_profit_drawdown",
    "MultiMetricHyperOptLoss":           "hyperopt_loss_multi_metric",
}

_BUILTIN_PKG = "VulcanTrader.hyperopt.hyperopt_loss"


class HyperOptLossResolver:
    @staticmethod
    def load_hyperoptloss(config: Config) -> IHyperOptLoss:
        loss_name: str = config.get("hyperopt_loss", "SharpeHyperOptLossDaily")

        # 1. Try built-in loss functions
        if loss_name in _BUILTIN_LOSS_MAP:
            module_name = f"{_BUILTIN_PKG}.{_BUILTIN_LOSS_MAP[loss_name]}"
            module = importlib.import_module(module_name)
            cls = getattr(module, loss_name)
            logger.info(f"Using built-in hyperopt loss: {loss_name}")
            return cls()

        # 2. Try user-supplied loss in user_data/hyperopts/
        user_data_dir: Path = config.get("user_data_dir", Path("user_data"))
        user_loss_dir = user_data_dir / "hyperopts"
        candidate = user_loss_dir / f"{loss_name}.py"
        if candidate.is_file():
            spec = importlib.util.spec_from_file_location(loss_name, candidate)
            module = importlib.util.module_from_spec(spec)  # type: ignore
            spec.loader.exec_module(module)  # type: ignore
            cls = getattr(module, loss_name, None)
            if cls is not None:
                logger.info(f"Using user-supplied hyperopt loss: {loss_name}")
                return cls()

        raise OperationalException(
            f"Hyperopt loss function '{loss_name}' not found. "
            f"Available built-ins: {sorted(_BUILTIN_LOSS_MAP.keys())}"
        )
