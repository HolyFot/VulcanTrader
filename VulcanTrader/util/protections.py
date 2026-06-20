"""Stub for the deprecated/legacy protections subsystem.

The full ``protections`` feature was removed from the configuration schema
(see :func:`VulcanTrader.config.config_validation` which raises
``ConfigurationError`` when ``protections`` is present in a config file).

This module exists purely so that the resolver chain still imports
cleanly. ``IProtection`` is provided as an empty placeholder base class.
"""

from __future__ import annotations


class IProtection:
    """Placeholder base class. Protections are not supported in VulcanTrader."""

    has_global_stop: bool = False
    has_local_stop: bool = False

    def __init__(self, config: dict, protection_config: dict) -> None:
        self._config = config
        self._protection_config = protection_config
