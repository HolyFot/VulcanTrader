"""Backtest caching helpers (ported from freqtrade.optimize.backtest_caching)."""

from __future__ import annotations

import hashlib
from copy import deepcopy
from pathlib import Path

import rapidjson


def get_strategy_run_id(strategy) -> str:
    """Generate unique identification hash for a backtest run.

    Identical config + strategy source produce an identical hash.
    """
    digest = hashlib.sha1()  # noqa: S324
    config = deepcopy(strategy.config)

    not_important_keys = ("strategy_list", "original_config", "telegram", "api_server")
    for k in not_important_keys:
        if k in config:
            del config[k]

    digest.update(
        rapidjson.dumps(config, default=str, number_mode=rapidjson.NM_NAN).encode("utf-8")
    )
    digest.update(
        rapidjson.dumps(
            getattr(strategy, "_ft_params_from_file", {}),
            default=str,
            number_mode=rapidjson.NM_NAN,
        ).encode("utf-8")
    )
    strat_file = getattr(strategy, "__file__", None)
    if strat_file:
        with Path(strat_file).open("rb") as fp:
            digest.update(fp.read())
    return digest.hexdigest().lower()


def get_backtest_metadata_filename(filename: Path | str) -> Path:
    """Return metadata filename for specified backtest results file."""
    filename = Path(filename)
    return filename.parent / Path(f"{filename.stem}.meta.json")
