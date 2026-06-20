"""Dry-run wallet helper."""

from VulcanTrader.constants import Config


def get_dry_run_wallet(config: Config) -> float:
    """Return dry-run wallet balance in stake currency.

    Supports both scalar ``dry_run_wallet`` and dict mode keyed by currency.
    """
    start_cap = config["dry_run_wallet"]
    if isinstance(start_cap, (int, float)):
        return start_cap
    return start_cap.get(config["stake_currency"], 0.0)
