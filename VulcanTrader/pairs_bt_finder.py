#!/usr/bin/env python3
"""
Individual Pair Backtesting Script
===================================
Backtests each pair from a config file individually and ranks them by selected metric.

Usage:
    python pairs_bt_finder.py
    python pairs_bt_finder.py --config configHyperDonchian.json --strategy DonchianTrend
    python pairs_bt_finder.py -c configHyperAll.json -s DonchianTrend -t 20250101- -w 8
"""

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# =============================================================================
# CONFIGURATION - Edit these defaults as needed
# =============================================================================

# Default config file to use
DEFAULT_CONFIG = "configHyperAll.json"

# Default strategy name
DEFAULT_STRATEGY = "DonchianTrend"

# Default timerange (from date to present)
DEFAULT_TIMERANGE = "20250101-"

# Number of parallel workers
DEFAULT_WORKERS = 5

# Number of top pairs to show
DEFAULT_TOP_N = 15

# Primary ranking metric: "roi", "sharpe", "expectancy", "calmar", "sortino", "lowdd", "composite"
PRIMARY_RANKING_METRIC = "composite"

# =============================================================================
# END CONFIGURATION
# =============================================================================

# Thread-safe print lock
print_lock = threading.Lock()


def load_config(config_path: str) -> dict:
    """Load and parse the config JSON file."""
    with open(config_path, 'r') as f:
        return json.load(f)


def get_pairs_from_config(config: dict) -> List[str]:
    """Extract pair whitelist from config."""
    return config.get('exchange', {}).get('pair_whitelist', [])


def run_backtest(
    config_path: str,
    strategy: str,
    pair: str,
    timerange: str,
    results_dir: str,
    pair_index: int = 0,
    total_pairs: int = 0
) -> Optional[Dict]:
    """
    Run a single backtest for one pair and return the results.
    """
    # Sanitize pair name for filename (replace / and : with _)
    pair_safe = pair.replace('/', '_').replace(':', '_')
    
    # Use freqtrade from venv if available, otherwise use system freqtrade
    freqtrade_cmd = 'freqtrade'
    venv_freqtrade = Path('.venv/Scripts/freqtrade.exe')
    if venv_freqtrade.exists():
        freqtrade_cmd = str(venv_freqtrade)
    
    cmd = [
        freqtrade_cmd, 'backtesting',
        '--config', config_path,
        '--strategy', strategy,
        '--pairs', pair,
        '--timerange', timerange,
        '--cache', 'none',
        '--export', 'none',  # Don't export trades to speed up
    ]
    
    with print_lock:
        print(f"[{pair_index}/{total_pairs}] Testing: {pair}")
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600  # 10 minute timeout per pair
        )
        
        output = result.stdout + result.stderr
        
        # Parse results from output
        parsed = parse_backtest_output(output, pair)
        
        with print_lock:
            if parsed:
                print(f"  [{pair_index}/{total_pairs}] {pair}: "
                      f"ROI: {parsed.get('total_profit_pct', 0):.2f}% | "
                      f"Sharpe: {parsed.get('sharpe', 0):.3f} | "
                      f"Trades: {parsed.get('trades', 0)} | "
                      f"Win%: {parsed.get('win_rate', 0):.1f}%")
            else:
                # Check for common error patterns
                if 'No data found' in output:
                    print(f"  [{pair_index}/{total_pairs}] {pair}: NO DATA - pair may not exist on exchange")
                elif 'No trades made' in output or 'BACKTESTING REPORT' not in output:
                    print(f"  [{pair_index}/{total_pairs}] {pair}: NO TRADES - strategy didn't trigger")
                elif 'error' in output.lower() or 'Error' in output:
                    # Extract error message
                    error_match = re.search(r'(Error|ERROR).*?$', output, re.MULTILINE)
                    error_msg = error_match.group(0)[:80] if error_match else "Unknown error"
                    print(f"  [{pair_index}/{total_pairs}] {pair}: ERROR - {error_msg}")
                else:
                    print(f"  [{pair_index}/{total_pairs}] {pair}: PARSE FAILED - check output format")
            
        return parsed
        
    except subprocess.TimeoutExpired:
        with print_lock:
            print(f"  [{pair_index}/{total_pairs}] {pair}: TIMEOUT - skipping")
        return None
    except Exception as e:
        with print_lock:
            print(f"  [{pair_index}/{total_pairs}] {pair}: ERROR - {e}")
        return None


def parse_backtest_output(output: str, pair: str) -> Optional[Dict]:
    """
    Parse the backtest output to extract key metrics.
    """
    result = {
        'pair': pair,
        'trades': 0,
        'total_profit_pct': 0.0,
        'avg_profit_pct': 0.0,
        'win_rate': 0.0,
        'sharpe': 0.0,
        'sortino': 0.0,
        'calmar': 0.0,
        'max_drawdown': 0.0,
        'profit_factor': 0.0,
        'expectancy': 0.0,
    }
    
    # Look for the STRATEGY SUMMARY section - format:
    # | StrategyName |    269 |         2.73 |       96776.559 |       806.47 |      3:01:00 |  133     0   136  49.4 | 6364.648 USDC  11.96% |
    # Match any strategy name (word characters), handle negative numbers
    
    strategy_match = re.search(
        r'STRATEGY SUMMARY.*?\|\s*(\w+)\s*\|\s*(\d+)\s*\|\s*([-\d.]+)\s*\|\s*([-\d.,]+)\s*\|\s*([-\d.]+)\s*\|'
        r'\s*[\d:]+\s*\|\s*(\d+)\s+(\d+)\s+(\d+)\s+([\d.]+)\s*\|',
        output, re.DOTALL
    )
    if strategy_match:
        result['trades'] = int(strategy_match.group(2))
        result['avg_profit_pct'] = float(strategy_match.group(3))
        result['total_profit_pct'] = float(strategy_match.group(5))
        result['win_rate'] = float(strategy_match.group(9))
    
    # Alternative: Look for TOTAL row in BACKTESTING REPORT if strategy match failed
    if result['trades'] == 0:
        total_match = re.search(
            r'\|\s*TOTAL\s*\|\s*(\d+)\s*\|\s*([-\d.]+)\s*\|\s*([-\d.,]+)\s*\|\s*([-\d.]+)\s*\|'
            r'\s*[\d:]+\s*\|\s*(\d+)\s+(\d+)\s+(\d+)\s+([\d.]+)\s*\|',
            output
        )
        if total_match:
            result['trades'] = int(total_match.group(1))
            result['avg_profit_pct'] = float(total_match.group(2))
            result['total_profit_pct'] = float(total_match.group(4))
            result['win_rate'] = float(total_match.group(8))
    
    # Fallback: Try simpler patterns if above failed
    if result['trades'] == 0:
        # Look for "Total/Daily Avg Trades" line: Total/Daily Avg Trades        | 269 / 0.72
        trades_match = re.search(r'Total/Daily Avg Trades\s+\|\s*(\d+)', output)
        if trades_match:
            result['trades'] = int(trades_match.group(1))
        
        # Look for "Total profit %" line
        profit_match = re.search(r'Total profit %\s+\|\s*([-\d.]+)%?', output)
        if profit_match:
            result['total_profit_pct'] = float(profit_match.group(1))
    
    # Find Sharpe ratio - in SUMMARY METRICS section
    # Format: Sharpe                        | 3.66
    sharpe_match = re.search(r'Sharpe\s+\|\s*([-\d.]+)', output)
    if sharpe_match:
        result['sharpe'] = float(sharpe_match.group(1))
    
    # Find Sortino ratio
    sortino_match = re.search(r'Sortino\s+\|\s*([-\d.]+)', output)
    if sortino_match:
        result['sortino'] = float(sortino_match.group(1))
    
    # Find Calmar ratio
    calmar_match = re.search(r'Calmar\s+\|\s*([-\d.]+)', output)
    if calmar_match:
        result['calmar'] = float(calmar_match.group(1))
    
    # Find Max Drawdown - look for "Absolute drawdown" with percentage in parens
    dd_match = re.search(r'Absolute drawdown.*?\(([\d.]+)%\)', output)
    if dd_match:
        result['max_drawdown'] = float(dd_match.group(1))
    else:
        # Alternative: look for Max % of account underwater
        dd_match2 = re.search(r'Max % of account underwater\s+\|\s*([\d.]+)%?', output)
        if dd_match2:
            result['max_drawdown'] = float(dd_match2.group(1))
    
    # Find Profit Factor
    pf_match = re.search(r'Profit factor\s+\|\s*([\d.]+)', output)
    if pf_match:
        result['profit_factor'] = float(pf_match.group(1))
    
    # Find Expectancy - format: Expectancy (Ratio)            | 359.76 (0.69)
    exp_match = re.search(r'Expectancy.*?\|\s*([-\d.]+)', output)
    if exp_match:
        result['expectancy'] = float(exp_match.group(1))
    
    # Check if pair had no trading data (OHLCV data missing)
    if 'No data found' in output or 'No trades made' in output:
        return None
    
    # Only return if we got meaningful data (trades > 0 OR we have profit data)
    if result['trades'] > 0 or result['total_profit_pct'] != 0 or result['sharpe'] != 0:
        return result
    
    return None


def get_metric_value(result: Dict, metric: str) -> float:
    """Get the value for a given metric from a result dict."""
    # For lowdd, return negative max_drawdown so lower DD ranks higher when sorting descending
    if metric.lower() == 'lowdd':
        return -abs(result.get('max_drawdown', 100.0))
    
    metric_map = {
        'roi': 'total_profit_pct',
        'sharpe': 'sharpe',
        'sortino': 'sortino',
        'calmar': 'calmar',
        'expectancy': 'expectancy',
        'profit_factor': 'profit_factor',
        'win_rate': 'win_rate',
        'composite': 'composite_score',
    }
    key = metric_map.get(metric.lower(), 'composite_score')
    return result.get(key, 0.0)


def calculate_composite_score(result: Dict) -> float:
    """
    Calculate a composite score based on ROI, Sharpe, and Low Drawdown.
    
    Score = (Normalized ROI * 0.35) + (Normalized Sharpe * 0.35) + (Normalized LowDD * 0.30)
    
    Also applies penalties for:
    - Very few trades (< 5)
    - Poor win rate (< 30%)
    """
    roi = result.get('total_profit_pct', 0)
    sharpe = result.get('sharpe', 0)
    trades = result.get('trades', 0)
    win_rate = result.get('win_rate', 0)
    max_dd = abs(result.get('max_drawdown', 0))
    
    # Normalize each component to 0-100 scale
    # ROI: assume range -50 to 100
    roi_score = max(0, min(100, (roi + 50) / 150 * 100))
    
    # Sharpe: assume range -2 to 5
    sharpe_score = max(0, min(100, (sharpe + 2) / 7 * 100))
    
    # Low DD: 0% DD = 100 score, 50% DD = 0 score (lower is better)
    dd_score = max(0, min(100, (50 - max_dd) / 50 * 100))
    
    # Weighted composite: ROI 35%, Sharpe 35%, Low DD 30%
    base_score = (roi_score * 0.35) + (sharpe_score * 0.35) + (dd_score * 0.30)
    
    # Penalties for low trade count and win rate
    penalty = 1.0
    
    if trades < 3:
        penalty *= 0.3  # Heavy penalty for very few trades
    elif trades < 5:
        penalty *= 0.6
    elif trades < 10:
        penalty *= 0.8
    
    if win_rate < 30 and trades > 5:
        penalty *= 0.7
    
    return base_score * penalty


def rank_results(results: List[Dict], top_n: int = 15) -> List[Dict]:
    """
    Rank results by composite score (ROI + Sharpe).
    """
    # Calculate composite score for each
    for r in results:
        r['composite_score'] = calculate_composite_score(r)
    
    # Sort by composite score descending
    sorted_results = sorted(results, key=lambda x: x['composite_score'], reverse=True)
    
    return sorted_results[:top_n]


def print_results_table(results: List[Dict], title: str = "TOP PAIRS", metric: str = "composite"):
    """Print a formatted results table."""
    print(f"\n{'='*125}")
    print(f" {title}")
    print(f"{'='*125}")
    print(f"{'Rank':<5} {'Pair':<22} {'Trades':>7} {'ROI %':>10} {'Sharpe':>8} "
          f"{'Calmar':>8} {'Expect':>8} {'PF':>8} {'Win %':>7} {'MaxDD %':>8} {'Score':>8}")
    print(f"{'-'*125}")
    
    for i, r in enumerate(results, 1):
        print(f"{i:<5} {r['pair']:<22} {r['trades']:>7} {r['total_profit_pct']:>10.2f} "
              f"{r['sharpe']:>8.3f} {r['calmar']:>8.2f} {r['expectancy']:>8.2f} "
              f"{r['profit_factor']:>8.2f} {r['win_rate']:>7.1f} {r['max_drawdown']:>8.2f} "
              f"{r['composite_score']:>8.2f}")
    
    print(f"{'='*125}")


def save_results_json(results: List[Dict], output_path: str):
    """Save results to a JSON file."""
    with open(output_path, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description='Backtest pairs individually and rank by selected metric'
    )
    parser.add_argument(
        '--config', '-c',
        default=DEFAULT_CONFIG,
        help=f'Path to config file (default: {DEFAULT_CONFIG})'
    )
    parser.add_argument(
        '--strategy', '-s',
        default=DEFAULT_STRATEGY,
        help=f'Strategy name (default: {DEFAULT_STRATEGY})'
    )
    parser.add_argument(
        '--timerange', '-t',
        default=DEFAULT_TIMERANGE,
        help=f'Timerange for backtest (default: {DEFAULT_TIMERANGE})'
    )
    parser.add_argument(
        '--top', '-n',
        type=int,
        default=DEFAULT_TOP_N,
        help=f'Number of top pairs to show (default: {DEFAULT_TOP_N})'
    )
    parser.add_argument(
        '--output', '-o',
        default=None,
        help='Output JSON file for results (optional)'
    )
    parser.add_argument(
        '--pairs',
        nargs='*',
        default=None,
        help='Specific pairs to test (optional, otherwise uses config whitelist)'
    )
    parser.add_argument(
        '--workers', '-w',
        type=int,
        default=DEFAULT_WORKERS,
        help=f'Number of parallel backtests to run (default: {DEFAULT_WORKERS})'
    )
    parser.add_argument(
        '--metric', '-m',
        default=PRIMARY_RANKING_METRIC,
        choices=['roi', 'sharpe', 'sortino', 'calmar', 'expectancy', 'lowdd', 'composite'],
        help=f'Primary ranking metric (default: {PRIMARY_RANKING_METRIC})'
    )
    
    args = parser.parse_args()
    
    # Load config
    print(f"\nLoading config: {args.config}")
    config = load_config(args.config)
    
    # Get pairs
    if args.pairs:
        pairs = args.pairs
    else:
        pairs = get_pairs_from_config(config)
    
    if not pairs:
        print("ERROR: No pairs found in config!")
        sys.exit(1)
    
    print(f"Found {len(pairs)} pairs to test")
    print(f"Strategy: {args.strategy}")
    print(f"Timerange: {args.timerange}")
    print(f"Parallel workers: {args.workers}")
    print(f"Ranking metric: {args.metric.upper()}")
    
    # Create results directory
    results_dir = Path('user_data/backtest_results/individual_pairs')
    results_dir.mkdir(parents=True, exist_ok=True)
    
    # Run backtests in parallel
    all_results = []
    start_time = datetime.now()
    total_pairs = len(pairs)
    
    print(f"\n{'='*60}")
    print(f" Starting parallel backtests ({args.workers} workers)")
    print(f"{'='*60}\n")
    
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        # Submit all tasks
        future_to_pair = {
            executor.submit(
                run_backtest,
                config_path=args.config,
                strategy=args.strategy,
                pair=pair,
                timerange=args.timerange,
                results_dir=str(results_dir),
                pair_index=i,
                total_pairs=total_pairs
            ): pair
            for i, pair in enumerate(pairs, 1)
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_pair):
            pair = future_to_pair[future]
            try:
                result = future.result()
                if result:
                    all_results.append(result)
            except Exception as e:
                with print_lock:
                    print(f"  {pair}: Exception - {e}")
    
    elapsed = datetime.now() - start_time
    print(f"\n\nCompleted {len(pairs)} backtests in {elapsed}")
    print(f"Successful results: {len(all_results)}")
    
    if not all_results:
        print("ERROR: No successful backtest results!")
        sys.exit(1)
    
    # Rank results by primary metric
    top_results = rank_results(all_results, args.top)
    
    # Sort by the selected primary metric
    metric_name = args.metric.upper()
    primary_sorted = sorted(
        all_results, 
        key=lambda x: get_metric_value(x, args.metric), 
        reverse=True
    )[:args.top]
    
    # Print top pairs by selected metric
    print_results_table(primary_sorted, f"TOP {args.top} PAIRS BY {metric_name}", args.metric)
    
    # Also show top by composite if different
    if args.metric != 'composite':
        print_results_table(top_results, f"TOP {args.top} PAIRS BY COMPOSITE SCORE")
    
    # Show top by other key metrics
    roi_sorted = sorted(all_results, key=lambda x: x['total_profit_pct'], reverse=True)[:args.top]
    sharpe_sorted = sorted(all_results, key=lambda x: x['sharpe'], reverse=True)[:args.top]
    calmar_sorted = sorted(all_results, key=lambda x: x['calmar'], reverse=True)[:args.top]
    expectancy_sorted = sorted(all_results, key=lambda x: x['expectancy'], reverse=True)[:args.top]
    lowdd_sorted = sorted(all_results, key=lambda x: x['max_drawdown'])[:args.top]  # Lower DD is better, no reverse
    
    if args.metric != 'roi':
        print_results_table(roi_sorted, f"TOP {args.top} PAIRS BY ROI")
    if args.metric != 'sharpe':
        print_results_table(sharpe_sorted, f"TOP {args.top} PAIRS BY SHARPE")
    if args.metric != 'calmar':
        print_results_table(calmar_sorted, f"TOP {args.top} PAIRS BY CALMAR")
    if args.metric != 'expectancy':
        print_results_table(expectancy_sorted, f"TOP {args.top} PAIRS BY EXPECTANCY")
    if args.metric != 'lowdd':
        print_results_table(lowdd_sorted, f"TOP {args.top} PAIRS BY LOW DRAWDOWN")
    
    # Save results if output specified
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = args.output or f'user_data/backtest_results/pair_rankings_{args.strategy}_{timestamp}.json'
    
    save_data = {
        'timestamp': timestamp,
        'config': args.config,
        'strategy': args.strategy,
        'timerange': args.timerange,
        'primary_metric': args.metric,
        'total_pairs_tested': len(pairs),
        'successful_tests': len(all_results),
        f'top_by_{args.metric}': primary_sorted,
        'top_by_composite': top_results,
        'top_by_roi': roi_sorted,
        'top_by_sharpe': sharpe_sorted,
        'top_by_calmar': calmar_sorted,
        'top_by_expectancy': expectancy_sorted,
        'top_by_lowdd': lowdd_sorted,
        'all_results': sorted(all_results, key=lambda x: get_metric_value(x, args.metric), reverse=True)
    }
    
    save_results_json(save_data, output_file)
    
    # Print summary
    print(f"\n{'='*60}")
    print(" SUMMARY")
    print(f"{'='*60}")
    print(f"Total pairs tested: {len(pairs)}")
    print(f"Successful tests: {len(all_results)}")
    print(f"Failed/skipped: {len(pairs) - len(all_results)}")
    print(f"Primary ranking metric: {metric_name}")
    best = primary_sorted[0]
    print(f"\nBest pair by {metric_name}: {best['pair']} "
          f"(ROI: {best['total_profit_pct']:.2f}%, "
          f"Sharpe: {best['sharpe']:.3f}, "
          f"Calmar: {best['calmar']:.3f})")
    print(f"Best pair by ROI: {roi_sorted[0]['pair']} "
          f"({roi_sorted[0]['total_profit_pct']:.2f}%)")
    print(f"Best pair by Sharpe: {sharpe_sorted[0]['pair']} "
          f"({sharpe_sorted[0]['sharpe']:.3f})")
    print(f"Best pair by Calmar: {calmar_sorted[0]['pair']} "
          f"({calmar_sorted[0]['calmar']:.3f})")
    print(f"Best pair by Low DD: {lowdd_sorted[0]['pair']} "
          f"({lowdd_sorted[0]['max_drawdown']:.2f}%)")
    print(f"\nResults saved to: {output_file}")
    
    # Print top pairs in config-ready format
    print(f"\n{'='*60}")
    print(f" TOP PAIRS BY {metric_name} (CONFIG-READY FORMAT)")
    print(f"{'='*60}")
    print(f"\n// Top {args.top} pairs by {metric_name}:")
    print('"pair_whitelist": [')
    for i, r in enumerate(primary_sorted):
        comma = "," if i < len(primary_sorted) - 1 else ""
        pf = r.get('profit_factor', 0)
        print(f'    "{r["pair"]}"{comma}  // ROI: {r["total_profit_pct"]:.1f}% | PF: {pf:.2f} | Sharpe: {r["sharpe"]:.2f}')
    print(']')
    
    # Also print as single line for easy copy
    print(f"\n// Single line format:")
    pairs_str = ', '.join([f'"{r["pair"]}"' for r in primary_sorted])
    print(f"[{pairs_str}]")


if __name__ == '__main__':
    main()
