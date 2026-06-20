"""
Hyperopt parameter space classes.

Mirrors freqtrade.optimize.space so that strategy HyperOpt classes using
Integer / Real / SKDecimal / Categorical continue to work unchanged.

Each class inherits from the corresponding optuna distribution so instances
can be handed directly to optuna as distribution objects, while also carrying
the `name` attribute that freqtrade-style code relies on.
"""

from __future__ import annotations

from typing import Any, Protocol, Sequence, runtime_checkable

import optuna.distributions as od


@runtime_checkable
class DimensionProtocol(Protocol):
    name: str


class ft_IntDistribution(od.IntDistribution):
    """IntDistribution with a name attribute for hyperopt bookkeeping."""

    def __init__(self, low: int, high: int, name: str = "", *, step: int = 1, log: bool = False):
        super().__init__(low=low, high=high, step=step, log=log)
        self.name = name


class ft_FloatDistribution(od.FloatDistribution):
    """FloatDistribution with a name attribute for hyperopt bookkeeping."""

    def __init__(
        self,
        low: float,
        high: float,
        name: str = "",
        *,
        step: float | None = None,
        log: bool = False,
    ):
        super().__init__(low=low, high=high, step=step, log=log)
        self.name = name


class ft_CategoricalDistribution(od.CategoricalDistribution):
    """CategoricalDistribution with a name attribute for hyperopt bookkeeping."""

    def __init__(self, choices: Sequence[Any], name: str = ""):
        super().__init__(choices=list(choices))
        self.name = name


class SKDecimal(ft_FloatDistribution):
    """Decimal parameter rounded to a fixed number of decimal places."""

    def __init__(self, low: float, high: float, decimals: int = 3, name: str = "", **kwargs):
        step = round(10 ** -decimals, decimals)
        super().__init__(low=low, high=high, name=name, step=step)
        self.decimals = decimals


# Convenience aliases used by strategy parameters and hyperopt interface
Integer = ft_IntDistribution
Real = ft_FloatDistribution
Categorical = ft_CategoricalDistribution
Dimension = DimensionProtocol  # backwards-compat alias
