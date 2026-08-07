#!/usr/bin/env python3
"""
Two-phase rust-engine backtest pipeline over the auto-discovered "main"
strategy set: one entry per top-level .py file directly under
user_data/strategies (see find_primary_strategies for exactly how a file's
primary strategy is told apart from same-file ablation-variant subclasses).
Nothing under a subdirectory (New/, OLD/, examples/, HistoricalDailyMarkets/,
...) is included - pass --strategies to target those explicitly by name.

Phase 1 (pair discovery): each strategy is backtested over the last N days
(default 30) against the FULL configHyper pair whitelist, purely to see
which pairs it actually opens trades on.

Phase 2 (deep backtest): each strategy is re-backtested over the full
lookback (default ~1.5y) restricted to --pairs <only the pairs it traded
in phase 1>. This is both cheaper (skips dead pairs) and closer to how the
strategy would actually be deployed.

Outputs:
  - scratch_bt/suite_<timestamp>/metrics_summary.txt   (per-strategy FULL
    METRICS SUMMARY block, reusing show_backtest_results)
  - user_data/configs/configBot<Strategy>.json          (configHyper clone
    with pair_whitelist narrowed to the strategy's discovered pairs)

Usage:
    python -m VulcanTrader.util.run_backtest_suite
    python -m VulcanTrader.util.run_backtest_suite --config configHyper --jobs 4
"""

import argparse
import ast
import contextlib
import copy
import io
import json
import re
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, UTC
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))
CONFIGS_DIR = REPO_ROOT / "user_data" / "configs"
STRATEGIES_DIR = REPO_ROOT / "user_data" / "strategies"


def _scan_classes(strategies_dir: Path) -> tuple[dict[str, set[str]], dict[str, Path]]:
    """
    AST-scan every .py file under `strategies_dir` (recursively, including
    excluded subdirs - callers filter those) and return:
      - base_names: {class_name: {base_name, ...}} as written in source
      - file_of:    {class_name: file_path} (first definition found)
    AST-based rather than importing, so it can't be fooled by import side
    effects (e.g. a class name appearing only in a module docstring's usage
    example) and can't crash on a broken strategy file.
    """
    base_names: dict[str, set[str]] = {}
    file_of: dict[str, Path] = {}
    # Sorted to match VulcanTrader.resolvers.iresolver.IResolver's own
    # `for entry in sorted(directory.rglob("*.py"))` - a class name defined
    # in more than one file (e.g. `DonchianBreakout` exists at top level
    # AND under examples/ and OLD/) must resolve to the SAME file here as
    # it would when the bot actually loads that strategy name, or this
    # script's idea of "what will run" silently diverges from reality.
    for py_file in sorted(strategies_dir.rglob("*.py")):
        if "__pycache__" in py_file.parts:
            continue
        try:
            tree = ast.parse(py_file.read_text(encoding="utf-8", errors="ignore"))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                bases = set()
                for b in node.bases:
                    if isinstance(b, ast.Name):
                        bases.add(b.id)
                    elif isinstance(b, ast.Attribute):
                        bases.add(b.attr)
                # A class name can appear more than once (e.g. re-exported);
                # keep the first definition found.
                base_names.setdefault(node.name, bases)
                file_of.setdefault(node.name, py_file)
    return base_names, file_of


def find_istrategy_classes(strategies_dir: Path) -> dict[str, Path]:
    """
    Return {class_name: file_path} for every class anywhere under
    `strategies_dir` that (transitively) inherits from IStrategy - e.g.
    `class Foo(IStrategy)` or `class Foo(SomeMixin, IStrategy)` or
    `class FooSpot(Foo)` where Foo itself derives from IStrategy. Used to
    validate an explicit --strategies override; does not filter by
    directory (a user asking for a specific name by hand should get it
    wherever it lives, including OLD/ or examples/).
    """
    base_names, file_of = _scan_classes(strategies_dir)
    resolved: dict[str, bool] = {}

    def is_istrategy(name: str, seen: set[str]) -> bool:
        if name in resolved:
            return resolved[name]
        if name in seen:
            return False
        seen.add(name)
        bases = base_names.get(name)
        if bases is None:
            return False
        result = "IStrategy" in bases or any(is_istrategy(b, seen) for b in bases)
        resolved[name] = result
        return result

    return {name: file_of[name] for name in base_names if is_istrategy(name, set())}


def find_primary_strategies(strategies_dir: Path) -> dict[str, Path]:
    """
    Auto-discover the "main" strategy set: one entry per top-level .py file
    directly under `strategies_dir` - not in any subdirectory (New/, OLD/,
    OLD/Swans/, examples/, HistoricalDailyMarkets/, etc. are all excluded
    this way, automatically, without needing to name them).

    A file can define more than one real strategy (e.g. DonchianBreakout.py
    documents both DonchianBreakout and DonchianReversion) alongside any
    number of ablation-variant subclasses used for A/B testing (e.g.
    `class DonchianTrend(DonchianBreakout)`,
    `class CalendarHourEdgeDirCap(CalendarHourEdge)`). The distinguishing
    signal is inheritance shape, not naming: a variant subclasses ANOTHER
    class defined in the SAME file, whereas a primary strategy's bases are
    only IStrategy itself or something imported from elsewhere (a mixin).
    This reliably separates the two without relying on file/class name
    matching, which breaks the moment a file defines more than one primary
    (DonchianBreakout.py) or names its primary class differently from the
    file (none currently do, but nothing enforces it).
    """
    base_names, file_of = _scan_classes(strategies_dir)
    istrategy = find_istrategy_classes(strategies_dir)

    result: dict[str, Path] = {}
    for name, path in file_of.items():
        if name not in istrategy:
            continue
        if path.parent != strategies_dir:
            continue  # only direct children of strategies_dir, not New/OLD/examples/...
        same_file_siblings = {
            b for b in base_names[name] if file_of.get(b) == path and b != name
        }
        if same_file_siblings:
            continue  # ablation/variant subclass of a sibling in this file
        result[name] = path
    return result


_NAME_COL_MAX = 40


def _short_name(name: str, width: int = _NAME_COL_MAX) -> str:
    """Truncates with an ellipsis rather than letting one long class name
    (e.g. an 80+ char auto-imported one) blow out every row's width."""
    return name if len(name) <= width else name[: width - 1] + "…"


def _fmt_ratio(x: float, width: int, comma: bool = False) -> str:
    """Fixed-width numeric formatting that degrades to scientific notation
    instead of overflowing the column when a strategy's stats blow up
    (e.g. a near-zero drawdown sending Calmar/CAGR into the billions -
    that's a real anomaly worth seeing, not something to hide, but it
    shouldn't wreck every other column's alignment)."""
    if abs(x) >= 1e6:
        return f"{x:.1e}".rjust(width)
    return f"{x:>{width},.2f}" if comma else f"{x:>{width}.2f}"


def _fmt_pct(x: float, width: int) -> str:
    pct = x * 100
    if abs(pct) >= 1e6:
        return f"{pct:.1e}%".rjust(width)
    return f"{pct:>{width - 1}.1f}%"


def build_summary_table(rows: list[tuple[str, dict | None, str]]) -> str:
    """
    Build a single ranked leaderboard across every strategy in the run.

    `rows` is (name, stats, skip_reason) per strategy - `stats` is the raw
    per-strategy dict from a phase-2 result JSON (`data["strategy"][name]`,
    same shape `show_backtest_results` consumes) for a completed run, or
    None (with `skip_reason` set) for one that was skipped/failed. Ranked
    strategies are sorted by Sharpe ratio (descending); skipped ones are
    listed at the bottom with their reason, not mixed into the ranking.
    """
    ranked = [(n, s) for n, s, _ in rows if s is not None]
    skipped = [(n, r) for n, s, r in rows if s is None]
    ranked.sort(key=lambda ns: ns[1].get("sharpe", 0.0) or 0.0, reverse=True)

    name_w = max([len(_short_name(n)) for n, _, _ in rows] + [len("Strategy")])
    header = (f"  {'Strategy':<{name_w}} {'Trades':>7} {'WinR':>6} {'PF':>6} "
              f"{'Sharpe':>7} {'Sortino':>7} {'Calmar':>8} {'CAGR':>8} "
              f"{'MaxDD':>7} {'Net P&L':>12} {'Net%':>7}")
    sep = "  " + "-" * (len(header) - 2)
    lines = [
        f"  {'OVERALL RESULTS - ' + str(len(ranked)) + ' ranked, ' + str(len(skipped)) + ' skipped':^{len(header) - 2}}",
        "=" * len(header),
        header,
        sep,
    ]
    for name, st in ranked:
        trades = st.get("total_trades", 0)
        wins = st.get("wins", 0)
        winrate = st.get("winrate", wins / trades if trades else 0.0) or 0.0
        pf = st.get("profit_factor", 0.0) or 0.0
        sharpe = st.get("sharpe", 0.0) or 0.0
        sortino = st.get("sortino", 0.0) or 0.0
        calmar = st.get("calmar", 0.0) or 0.0
        cagr = st.get("cagr", 0.0) or 0.0
        max_dd = st.get("max_relative_drawdown", st.get("max_drawdown_account", 0.0)) or 0.0
        pnl_abs = st.get("profit_total_abs", 0.0) or 0.0
        pnl_pct = st.get("profit_total", 0.0) or 0.0
        lines.append(
            f"  {_short_name(name):<{name_w}} {trades:>7} {winrate * 100:>5.1f}% {pf:>6.2f} "
            f"{_fmt_ratio(sharpe, 7)} {_fmt_ratio(sortino, 7)} {_fmt_ratio(calmar, 8)} "
            f"{_fmt_pct(cagr, 8)} {_fmt_pct(max_dd, 7)} "
            f"{_fmt_ratio(pnl_abs, 12, comma=True)} {_fmt_pct(pnl_pct, 7)}"
        )
    lines.append(sep)
    if skipped:
        lines.append("")
        lines.append("  Skipped:")
        for name, reason in skipped:
            lines.append(f"    {name}: {reason}")
    lines.append("=" * len(header))
    return "\n".join(lines)


def _run(cmd: list[str]) -> int:
    print(f"  $ {' '.join(cmd)}")
    return subprocess.run(cmd, cwd=REPO_ROOT).returncode


def _result_label(name: str) -> str:
    """Mirrors optimize_reports.store_backtest_results' exact filename
    sanitization (sub non-word/dash chars, truncate to 80 chars) - a long
    class name (e.g. an 84-char auto-imported one) gets truncated THERE, so
    matching on the raw untruncated name here would never find its file."""
    return re.sub(r"[^\w\-]", "_", name)[:80]


def _result_files(directory: Path, name: str) -> list[Path]:
    """Result JSONs for `name`, sorted oldest-to-newest, excluding the
    `.meta.json` sidecar (also matched by `{label}-*.json` since it shares
    the stem)."""
    label = _result_label(name)
    return sorted(
        p for p in directory.glob(f"{label}-*.json") if not p.name.endswith(".meta.json")
    )


def _base_config_path(name: str) -> Path:
    p = Path(name)
    if p.suffix == "":
        p = p.with_suffix(".json")
    if not p.is_absolute() and not p.exists():
        p = CONFIGS_DIR / p.name
    return p


def discover_pairs(name: str, phase1_dir: Path, min_trades: int) -> tuple[list[str] | None, str]:
    """Returns (pairs_or_None, reason). `reason` explains a None result -
    a strategy that failed to even load (bad import, syntax error, ...) in
    phase 1 is a very different problem than one that legitimately traded
    nothing, so callers should report them differently rather than lumping
    both under "no pairs"."""
    matches = _result_files(phase1_dir, name)
    if not matches:
        msg = "phase-1 backtest failed to produce a result (strategy likely failed to load - check phase1.log)"
        print(f"  [{name}] {msg}")
        return None, msg
    with open(matches[-1], encoding="utf-8") as f:
        data = json.load(f)
    st = (data.get("strategy") or {}).get(name)
    if not st:
        msg = "phase-1 result file has no stats for this strategy"
        print(f"  [{name}] {msg}")
        return None, msg
    pairs = sorted(
        p["key"] for p in st.get("results_per_pair", [])
        if p.get("key") != "TOTAL" and p.get("trades", 0) >= min_trades
    )
    if not pairs:
        msg = "0 pairs met the trade threshold in the pair scan"
        print(f"  [{name}] {msg}")
        return None, msg
    print(f"  [{name}] {len(pairs)} pairs: {', '.join(pairs)}")
    return pairs, ""


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--config", default="configHyper", help="Base config (default: configHyper)")
    ap.add_argument("--jobs", type=int, default=4, help="Parallel backtest processes")
    ap.add_argument("--scan-days", type=int, default=30, help="Phase-1 pair-discovery lookback")
    ap.add_argument("--deep-days", type=int, default=548, help="Phase-2 lookback (default ~1.5y)")
    ap.add_argument("--min-trades", type=int, default=1, help="Min trades in phase-1 for a pair to carry into phase-2")
    ap.add_argument("--outdir", default=None, help="Override output directory")
    ap.add_argument("--strategies", nargs="+", default=None,
                     help="Override auto-discovery with an explicit strategy list (for testing a subset)")
    args = ap.parse_args()

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    outdir = Path(args.outdir) if args.outdir else REPO_ROOT / "scratch_bt" / f"suite_{stamp}"
    phase1_dir = outdir / "phase1_pairscan"
    phase2_dir = outdir / "phase2_deep"
    phase1_dir.mkdir(parents=True, exist_ok=True)
    phase2_dir.mkdir(parents=True, exist_ok=True)

    if args.strategies:
        print("=== Verifying strategy classes ===")
        istrategy_classes = find_istrategy_classes(STRATEGIES_DIR)
        all_strats = []
        for name in args.strategies:
            if name not in istrategy_classes:
                print(f"  [{name}] SKIPPED - no IStrategy subclass found under user_data/strategies")
                continue
            all_strats.append((name, "custom"))
    else:
        print("=== Auto-discovering strategies (top-level files under user_data/strategies) ===")
        primary = find_primary_strategies(STRATEGIES_DIR)
        all_strats = [(name, "auto") for name in sorted(primary)]
        for name, path in sorted(primary.items()):
            print(f"  [{name}] {path.relative_to(REPO_ROOT)}")

    strat_names = [n for n, _ in all_strats]
    if not strat_names:
        print("No valid IStrategy strategies to run.")
        return 1

    today = datetime.now(UTC).date()
    scan_range = f"{(today - timedelta(days=args.scan_days)):%Y%m%d}-{today:%Y%m%d}"
    deep_range = f"{(today - timedelta(days=args.deep_days)):%Y%m%d}-{today:%Y%m%d}"

    print(f"=== Phase 1: pair discovery ({scan_range}) across {len(strat_names)} strategies ===")
    rc = _run([
        sys.executable, "-m", "VulcanTrader.bot", "backtest",
        "-c", args.config, "--engine", "rust",
        "--strategies", *strat_names,
        "--jobs", str(args.jobs),
        "--timerange", scan_range,
        "--exportfilename", str(phase1_dir),
        "-v", "--logfile", str(phase1_dir / "phase1.log"),
    ])
    if rc != 0:
        print(f"  (phase 1 exited {rc} - some strategies may have failed, continuing with what we have)")

    print("\n=== Collecting discovered pairs ===")
    strat_pairs: dict[str, list[str]] = {}
    skip_reasons: dict[str, str] = {}
    for name in strat_names:
        pairs, reason = discover_pairs(name, phase1_dir, args.min_trades)
        if pairs:
            strat_pairs[name] = pairs
        else:
            skip_reasons[name] = reason

    print(f"\n=== Phase 2: deep backtest ({deep_range}) on {len(strat_pairs)} strategies, {args.jobs} at a time ===")

    def run_deep(name: str) -> tuple[str, int]:
        rc = _run([
            sys.executable, "-m", "VulcanTrader.bot", "backtest",
            "-c", args.config, "--engine", "rust",
            "-s", name,
            "--pairs", *strat_pairs[name],
            "--timerange", deep_range,
            "--exportfilename", str(phase2_dir),
            "-v", "--logfile", str(phase2_dir / f"{name}.log"),
        ])
        return name, rc

    with ThreadPoolExecutor(max_workers=max(1, args.jobs)) as ex:
        futs = [ex.submit(run_deep, n) for n in strat_pairs]
        for fut in as_completed(futs):
            name, rc = fut.result()
            print(f"  [{name}] {'ok' if rc == 0 else f'FAILED rc={rc}'}")

    print("\n=== Building report + per-strategy configs ===")
    from VulcanTrader.util.optimize_reports import show_backtest_results  # noqa: E402

    base_config_path = _base_config_path(args.config)
    with open(base_config_path, encoding="utf-8") as f:
        base_config = json.load(f)

    detail_blocks: list[str] = []
    table_rows: list[tuple[str, dict | None, str]] = []

    for name, group in all_strats:
        header = f"{'=' * 72}\n  {name}  [{group}]\n{'=' * 72}"
        if name not in strat_pairs:
            reason = skip_reasons.get(name, "unknown reason")
            detail_blocks.append(f"{header}\n  SKIPPED - {reason}\n")
            table_rows.append((name, None, reason))
            continue

        matches = _result_files(phase2_dir, name)
        if not matches:
            detail_blocks.append(f"{header}\n  DEEP BACKTEST FAILED - no result file\n")
            table_rows.append((name, None, "deep backtest failed - no result file"))
            continue

        with open(matches[-1], encoding="utf-8") as f:
            data = json.load(f)
        stats = (data.get("strategy") or {}).get(name)
        table_rows.append((name, stats, ""))

        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            show_backtest_results({}, data)
        detail_blocks.append(buf.getvalue())

        cfg = copy.deepcopy(base_config)
        cfg["strategy"] = name
        cfg["exchange"]["pair_whitelist"] = strat_pairs[name]
        # Base config's db_url/bot_name are shared across every clone - give
        # each strategy its own so running several configBotX.json bots at
        # once doesn't have them scribble over one shared trade ledger.
        if "db_url" in cfg:
            cfg["db_url"] = f"json:user_data/accounts/trades-{name}.json"
        if "bot_name" in cfg:
            cfg["bot_name"] = name
        cfg_path = CONFIGS_DIR / f"configBot{name}.json"
        with open(cfg_path, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=4)
            f.write("\n")
        print(f"  [{name}] wrote {cfg_path.relative_to(REPO_ROOT)}")

    report_lines = [
        f"VulcanTrader backtest suite - {stamp}",
        f"Base config: {args.config}   Pair-scan: {scan_range}   Deep: {deep_range}",
        "",
        build_summary_table(table_rows),
        "",
        *detail_blocks,
    ]

    report_path = outdir / "metrics_summary.txt"
    report_path.write_text("\n".join(report_lines), encoding="utf-8")
    print(f"\nDone. Report: {report_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
