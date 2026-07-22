# =============================================================================
#  VulcanTrader  ::  Exchange rate-limit probe  (ratelimit_probe.py)
# =============================================================================
#
#  Purpose
#  -------
#  Empirically finds how many pairs can be requested in a single
#  Exchange.refresh_latest_ohlcv() batch - the exact call DataCollector /
#  data_server.py makes every tick - before an exchange starts pushing back
#  with rate-limit / WAF-block signals (429, 403, DDoSProtection, "too many
#  requests", etc). Uses the real Exchange classes and the real ccxt_config
#  (enableRateLimit + rateLimit) this repo already trades with, so results
#  reflect this system's actual operating conditions, not some theoretical
#  exchange-side ceiling.
#
#  Method
#  ------
#  For each exchange:
#    1. Build a throwaway config (cloned from an existing working config as a
#       schema-valid template, exchange name swapped in, pair whitelist
#       cleared) and load a real Exchange instance via ExchangeResolver
#       (validate=False, dry_run - no orders are ever placed, this only reads
#       public market/candle data).
#    2. Pull the exchange's own tradable-pairs list via get_markets() - no
#       curated pair list needed.
#    3. Walk an increasing sequence of pair counts (5, 10, 20, 40, 80, 150,
#       200 by default). For each count, call refresh_latest_ohlcv() on that
#       many pairs (same 5m timeframe throughout) and watch for rate-limit
#       signals via a temporary logging capture (refresh_latest_ohlcv catches
#       per-pair exceptions internally and only logs them - it does not
#       re-raise - so a log capture is the only way to see them from outside).
#    4. Stop escalating for that exchange as soon as a rate-limit signal
#       appears, recording the pair count it first showed up at. Otherwise
#       report "not rate-limited up to N".
#    5. Cool down between steps and between exchanges so one exchange's test
#       doesn't compound into a longer ban, and so back-to-back exchanges
#       don't get treated as a coordinated burst.
#
#  This is a live network test against real exchange APIs using this
#  machine's IP. It intentionally escalates until something pushes back, so
#  expect exactly that on at least some exchanges - that's the point. Nothing
#  here places orders or touches API keys/secrets (key/secret are blanked in
#  the throwaway config; only public market-data endpoints are used).
#
#  Usage
#  -----
#      python -m VulcanTrader.ratelimit_probe
#      python -m VulcanTrader.ratelimit_probe --exchanges binance okx
#      python -m VulcanTrader.ratelimit_probe --all --counts 5 10 20 40 80
# =============================================================================

"""Probe exchanges to find how many pairs in one OHLCV refresh trigger rate limiting."""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

if sys.platform == "win32":
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except Exception:
        pass

from VulcanTrader.config.configuration import Configuration
from VulcanTrader.enums import CandleType, RunMode
from VulcanTrader.resolvers import ExchangeResolver


logger = logging.getLogger(__name__)

# Exchanges explicitly named by the user, plus the trading_mode each needs to
# see its perpetual/futures markets (matches how this repo's own configs use
# them). Anything not listed here defaults to spot, the safest universal
# assumption for exchanges only reachable via `--all`.
EXCHANGE_PROFILES: dict[str, dict[str, Any]] = {
    "binance": {"trading_mode": "futures", "margin_mode": "isolated"},
    "hyperliquid": {"trading_mode": "futures", "margin_mode": "isolated"},
    "drift": {"trading_mode": "futures", "margin_mode": "isolated"},
    "okx": {"trading_mode": "futures", "margin_mode": "isolated"},
    "bybit": {"trading_mode": "futures", "margin_mode": "isolated"},
    "bitunix": {"trading_mode": "futures", "margin_mode": "isolated"},
}
DEFAULT_PROFILE = {"trading_mode": "spot", "margin_mode": ""}

# Other exchange modules present in VulcanTrader/exchange/ - only probed with --all.
OTHER_EXCHANGES = [
    "bitget", "bitmart", "bitpanda", "coinex", "cryptocom", "hitbtc", "kraken", "kucoin",
]

# Drift's own Data API (data.api.drift.trade) is CloudFront/WAF-fronted and returns a
# hard 403 from some networks (confirmed: not a code bug, a real block - verified by
# hitting the endpoint directly and getting `{"message":"Forbidden"}` from CloudFront).
# drift.py already has a designed-in fallback for exactly this case: when the Data API
# is unreachable and the pairlist doesn't require live tickers (i.e. not VolumePairList
# in ticker mode), it builds markets from `exchange.pair_whitelist` instead of failing.
# That fallback needs a non-empty pair_whitelist to do anything, so - unlike every other
# exchange here, where pair_whitelist is deliberately cleared to discover the exchange's
# full live market list - Drift gets a curated whitelist of well-known real perp markets
# so it can still be probed from networks where the Data API is blocked.
DRIFT_FALLBACK_PAIRS = [
    "SOL-PERP", "BTC-PERP", "ETH-PERP", "AVAX-PERP", "BNB-PERP", "XRP-PERP", "LTC-PERP",
    "DOGE-PERP", "LINK-PERP", "SUI-PERP", "JUP-PERP", "JTO-PERP", "INJ-PERP", "WIF-PERP",
    "HYPE-PERP", "PENGU-PERP", "FARTCOIN-PERP", "XPL-PERP", "ASTER-PERP", "PAXG-PERP",
    "ZEC-PERP", "DRIFT-PERP", "1MBONK-PERP",
]

RATE_LIMIT_MARKERS = (
    "ddosprotection", "ddos protection", "ratelimitexceeded", "rate limit",
    "too many requests", "429", "403", "forbidden", "banned", "restricted",
    "cloudflare", "captcha",
)


def _looks_rate_limited(messages: list[str]) -> list[str]:
    hits = []
    for m in messages:
        low = m.lower()
        if any(marker in low for marker in RATE_LIMIT_MARKERS):
            hits.append(m)
    return hits


class _LogCapture(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.setFormatter(logging.Formatter("%(message)s"))
        self.records: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self.records.append(self.format(record))
        except Exception:
            pass


def _build_probe_config_file(
    template: dict, exchange_name: str, profile: dict, scratch_dir: Path
) -> Path:
    cfg = copy.deepcopy(template)
    cfg["trading_mode"] = profile.get("trading_mode", "spot")
    cfg["margin_mode"] = profile.get("margin_mode", "")
    cfg["dry_run"] = True

    ex = cfg.setdefault("exchange", {})
    ex["name"] = exchange_name
    ex["key"] = ""
    ex["secret"] = ""
    # See DRIFT_FALLBACK_PAIRS: Drift needs a non-empty whitelist to fall back on when
    # its Data API is unreachable. Every other exchange gets an empty whitelist so
    # get_markets() discovers its full live market list instead of a curated subset.
    ex["pair_whitelist"] = list(DRIFT_FALLBACK_PAIRS) if exchange_name == "drift" else []
    ex["pair_blacklist"] = []
    ex.setdefault("ccxt_config", {"enableRateLimit": True, "rateLimit": 50})

    cfg["bot_name"] = f"RateLimitProbe-{exchange_name}"
    cfg["pairlists"] = [{"method": "StaticPairList"}]

    path = scratch_dir / f"probe_{exchange_name}.json"
    path.write_text(json.dumps(cfg, indent=2))
    return path


def _ensure_non_ccxt_exchange_registered(exchange_name: str) -> None:
    """VulcanTrader.exchange.drift is a complete custom Exchange subclass, but
    unlike its sibling custom exchange "bitunix", "drift" is missing from
    exchange.common.NON_CCXT_EXCHANGES - so Configuration's check_exchange() (which
    only knows real ccxt exchange ids plus that list) rejects it as "not known to
    ccxt" before we ever get a chance to load it. This patches the in-memory list
    for this process only, so it can still be probed; it does not touch the source
    file. Worth registering permanently in exchange/common.py if "drift" should be
    selectable through the normal config/CLI path."""
    import ccxt

    from VulcanTrader.exchange import common as exchange_common

    if exchange_name in ccxt.exchanges or exchange_name in exchange_common.NON_CCXT_EXCHANGES:
        return
    exchange_common.NON_CCXT_EXCHANGES.append(exchange_name)
    logger.warning(
        "'%s' is not registered in exchange.common.NON_CCXT_EXCHANGES (unlike its "
        "sibling custom exchange 'bitunix') - patched it into this process's copy "
        "so it can be probed. Consider registering it permanently in "
        "VulcanTrader/exchange/common.py if you want it selectable through the "
        "normal config/CLI path.",
        exchange_name,
    )


def probe_exchange(
    exchange_name: str,
    profile: dict,
    template: dict,
    scratch_dir: Path,
    counts: list[int],
    timeframe: str,
    step_cooldown: float,
) -> dict:
    logger.info("=" * 70)
    logger.info("Probing %s", exchange_name)
    logger.info("=" * 70)

    result: dict[str, Any] = {"exchange": exchange_name, "steps": [], "status": "ok"}

    config_path = _build_probe_config_file(template, exchange_name, profile, scratch_dir)

    _ensure_non_ccxt_exchange_registered(exchange_name)

    try:
        config = Configuration(
            {
                "config": [str(config_path)],
                "user_data_dir": str(scratch_dir / "user_data"),
                "verbosity": 0,
            },
            RunMode.UTIL_EXCHANGE,
        ).get_config()
    except Exception as e:
        logger.exception("Failed to build config for %s", exchange_name)
        result["status"] = f"config_error: {e}"
        return result

    try:
        exchange = ExchangeResolver.load_exchange(config, validate=False)
    except Exception as e:
        logger.exception("Failed to load exchange %s", exchange_name)
        result["status"] = f"load_error: {e}"
        return result

    # With validate=False, Exchange.__init__ skips its own (cheap, tier-free) initial
    # market load. That leaves `_last_markets_refresh` at 0, so the *first* call
    # below that touches markets (get_markets()) ends up doing its own implicit
    # reload_markets() - whose default is load_leverage_tiers=True. For futures
    # markets that fires one leverage-tier request per pair, which is unrelated to
    # what this script measures and can trip a rate limit all by itself before the
    # OHLCV escalation even starts. Load markets explicitly first, without tiers.
    try:
        exchange.reload_markets(True, load_leverage_tiers=False)
    except Exception:
        logger.exception(
            "Failed to pre-load markets (without leverage tiers) for %s - continuing anyway",
            exchange_name,
        )

    # validate_config() is the only normal caller of _set_startup_candle_count(), and
    # it never runs with validate=False. Without it, self._startup_candle_count is
    # simply missing, and _process_ohlcv_df's cache-merge path (hit whenever a pair
    # in the batch already has cached klines to merge against) raises AttributeError
    # for the *entire* refresh_latest_ohlcv() call - not a rate-limit signal, but
    # easy to mistake for one since it also surfaces as a hard failure.
    try:
        exchange._set_startup_candle_count(config)
    except Exception:
        logger.exception(
            "Failed to set _startup_candle_count for %s - continuing anyway", exchange_name
        )

    try:
        markets = list(exchange.get_markets(tradable_only=True, active_only=True).keys())
    except Exception as e:
        logger.exception("Failed to fetch markets for %s", exchange_name)
        result["status"] = f"markets_error: {e}"
        try:
            exchange.close()
        except Exception:
            pass
        return result

    result["available_pairs"] = len(markets)
    logger.info("%s: %d tradable pairs available", exchange_name, len(markets))
    if not markets:
        result["status"] = "no_markets"
        try:
            exchange.close()
        except Exception:
            pass
        return result

    candle_type: CandleType = config.get("candle_type_def", CandleType.SPOT)
    tested_counts = sorted({c for c in counts if c <= len(markets)})
    if not tested_counts:
        tested_counts = [len(markets)]

    rate_limited_at: int | None = None
    exchange_logger = logging.getLogger("VulcanTrader.exchange")

    for n in tested_counts:
        pairlist = [(p, timeframe, candle_type) for p in markets[:n]]
        cap = _LogCapture()
        exchange_logger.addHandler(cap)
        t0 = time.time()
        error: str | None = None
        try:
            results = exchange.refresh_latest_ohlcv(pairlist)
        except Exception as e:
            results = {}
            error = str(e)
        elapsed = time.time() - t0
        exchange_logger.removeHandler(cap)

        hits = _looks_rate_limited(cap.records)
        # A batch-level exception is only evidence of rate-limiting if its message
        # actually looks like one - otherwise it's an unrelated bug/crash (e.g. a
        # missing attribute this script's minimal exchange setup didn't populate)
        # and must not be misreported as a rate-limit finding.
        unexpected_error: str | None = None
        if error:
            error_hits = _looks_rate_limited([error])
            if error_hits:
                hits = hits + error_hits
            else:
                unexpected_error = error

        ok_count = sum(1 for df in results.values() if df is not None and not df.empty)
        step = {
            "pairs_requested": n,
            "pairs_ok": ok_count,
            "elapsed_s": round(elapsed, 2),
            "rate_limited": bool(hits),
            "sample_messages": hits[:3],
            "unexpected_error": unexpected_error,
        }
        result["steps"].append(step)
        logger.info(
            "%-12s n=%-4d ok=%-4d elapsed=%6.2fs rate_limited=%s%s",
            exchange_name, n, ok_count, elapsed, bool(hits),
            f" unexpected_error={unexpected_error}" if unexpected_error else "",
        )

        if hits:
            rate_limited_at = n
            logger.warning(
                "%s: rate-limit signal detected at %d pairs - stopping escalation. "
                "Sample: %s",
                exchange_name, n, hits[0],
            )
            break

        if unexpected_error:
            logger.error(
                "%s: unrelated error (NOT a rate-limit signal) at %d pairs - stopping "
                "escalation: %s",
                exchange_name, n, unexpected_error,
            )
            result["status"] = f"unexpected_error at n={n}: {unexpected_error}"
            break

        time.sleep(step_cooldown)

    result["rate_limited_at"] = rate_limited_at
    try:
        exchange.close()
    except Exception:
        logger.exception("Error closing %s (continuing)", exchange_name)
    return result


def main(argv: list[str] | None = None) -> int:
    import argparse

    from VulcanTrader.util.logger import setup as setup_logging

    parser = argparse.ArgumentParser(
        description="Probe exchanges to find how many pairs in one OHLCV refresh "
        "trigger rate limiting."
    )
    parser.add_argument(
        "--exchanges", nargs="+", default=["binance", "hyperliquid", "drift", "okx"],
        help="Exchange names to probe (default: binance hyperliquid drift okx).",
    )
    parser.add_argument(
        "--all", action="store_true",
        help="Also probe every other exchange module in VulcanTrader/exchange/.",
    )
    parser.add_argument(
        "--counts", nargs="+", type=int, default=[5, 10, 20, 40, 80, 150, 200],
        help="Increasing pair counts to test (capped per-exchange to its available pairs).",
    )
    parser.add_argument("--timeframe", default="5m", help="Timeframe used for every OHLCV request.")
    parser.add_argument(
        "--cooldown", type=float, default=3.0, help="Seconds to wait between escalation steps."
    )
    parser.add_argument(
        "--exchange-cooldown", type=float, default=10.0, help="Seconds to wait between exchanges."
    )
    parser.add_argument(
        "--template",
        default=None,
        help="Path to a working config to clone as the schema-valid template "
        "(default: user_data/configs/configBinance.json).",
    )
    parser.add_argument("--scratch-dir", default=None, help="Where to write throwaway configs/data.")
    parser.add_argument("-v", "--verbose", action="count", default=1)
    args = parser.parse_args(argv)

    setup_logging(level=logging.DEBUG if args.verbose >= 2 else logging.INFO)

    exchanges = list(dict.fromkeys(args.exchanges))
    if args.all:
        for name in OTHER_EXCHANGES:
            if name not in exchanges:
                exchanges.append(name)

    repo_root = Path(__file__).resolve().parent.parent.parent
    template_path = Path(args.template) if args.template else repo_root / "user_data/configs/configBinance.json"
    template = json.loads(template_path.read_text())

    scratch_dir = Path(args.scratch_dir) if args.scratch_dir else Path(
        tempfile.mkdtemp(prefix="ratelimit_probe_")
    )
    scratch_dir.mkdir(parents=True, exist_ok=True)
    # create_userdata_dir() requires the parent to already exist (create_dir=False);
    # its required sub-directories (data/, logs/, etc.) are created automatically.
    (scratch_dir / "user_data").mkdir(parents=True, exist_ok=True)
    logger.warning(
        "Starting live rate-limit probe against %d exchange(s): %s. "
        "This makes real public-API requests and deliberately escalates until "
        "something pushes back. Scratch dir: %s",
        len(exchanges), ", ".join(exchanges), scratch_dir,
    )

    all_results = []
    for i, name in enumerate(exchanges):
        profile = EXCHANGE_PROFILES.get(name, DEFAULT_PROFILE)
        res = probe_exchange(
            name, profile, template, scratch_dir, args.counts, args.timeframe, args.cooldown
        )
        all_results.append(res)
        if i < len(exchanges) - 1:
            time.sleep(args.exchange_cooldown)

    out_path = scratch_dir / "results.json"
    out_path.write_text(json.dumps(all_results, indent=2, default=str))

    print("\n" + "=" * 70)
    print("RATE LIMIT PROBE SUMMARY")
    print("=" * 70)
    for res in all_results:
        name = res["exchange"]
        if res.get("status") != "ok":
            print(f"{name:15s} FAILED: {res.get('status')}")
            continue
        rl = res.get("rate_limited_at")
        avail = res.get("available_pairs")
        if rl:
            print(f"{name:15s} rate-limited at {rl} pairs (of {avail} available)")
        else:
            max_tested = res["steps"][-1]["pairs_requested"] if res["steps"] else 0
            print(f"{name:15s} NOT rate-limited up to {max_tested} pairs (of {avail} available)")
    print(f"\nFull results written to: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
