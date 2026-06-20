"""Compatibility shim for legacy ``VulcanTrader.commands.arguments`` imports.

Only exposes the names actually referenced inside ``VulcanTrader.config`` and
elsewhere in the package. Add more entries here as needed.
"""

# Subcommand names that may run without a config. Used by Configuration to
# decide whether to read environment variables. Our CLI never sets
# ``args["command"]``, so this list is essentially advisory.
NO_CONF_ALLOWED: list[str] = [
    "create-userdir",
    "list-exchanges",
    "list-markets",
    "list-pairs",
    "list-timeframes",
    "new-config",
    "new-strategy",
    "show-config",
]
