"""Stub HyperoptStateContainer.

Hyperopt is not implemented in VulcanTrader. This container exists so
parameter logic in :mod:`VulcanTrader.strategy.parameters` can compare the
current state against ``HyperoptState.OPTIMIZE`` and decide that no
hyperopt-time behaviour applies.
"""

from VulcanTrader.enums import HyperoptState


class HyperoptStateContainer:
    """Holds the current hyperopt state. Defaults to ``STARTUP`` (i.e. not optimizing)."""

    state: HyperoptState = HyperoptState.STARTUP

    @classmethod
    def set_state(cls, value: HyperoptState) -> None:
        cls.state = value
