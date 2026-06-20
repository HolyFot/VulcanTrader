# ruff: noqa

import asyncio
import gc
import io
import json
import logging
import os
import uuid
from datetime import UTC, datetime, timedelta
from math import isnan
from pathlib import Path
from typing import Any, List, Optional, Tuple

import aiohttp
import pandas as pd
import polars as pl
from pandas import DataFrame

from VulcanTrader.constants import DEFAULT_DATAFRAME_COLUMNS, DEFAULT_TRADES_COLUMNS
from VulcanTrader.data.converter import (
    ohlcv_to_dataframe,
    trades_list_to_df,
)
from VulcanTrader.enums import CandleType, MarginMode, TradingMode
from VulcanTrader.util.exceptions import DDosProtection, ExchangeError, OperationalException, TemporaryError
from VulcanTrader.exchange import Exchange
from VulcanTrader.exchange.exchange_types import TraderHas, OrderBook, Ticker, Tickers
from VulcanTrader.exchange.exchange_utils_timeframe import timeframe_to_msecs
from VulcanTrader.util import FtTTLCache


logger = logging.getLogger(__name__)


class HTTPStatusHandler:
    """
    Handles HTTP status codes with specific logic for different response types.
    Follows Single Responsibility Principle - only manages HTTP status interpretation.
    """

    @staticmethod
    def handle_response_status(status_code, date):
        """
        Handle HTTP response status codes and return appropriate messages.

        Args:
            status_code (int): HTTP status code
            date (datetime): Date being processed

        Returns:
            tuple: (success: bool, message: str, should_retry: bool)
        """
        if status_code == 200:
            return True, "Success", False
        elif status_code == 403:
            return False, "No data (403 - Forbidden)", False
        elif status_code == 404:
            return False, "No data (404 - Not Found)", False
        elif status_code == 429:
            return False, "Rate limited (429)", True
        elif 500 <= status_code < 600:
            return False, f"Server error ({status_code})", True
        else:
            return False, f"HTTP {status_code}", True

    @staticmethod
    def is_access_denied(status_msg):
        """Check if status message indicates access denial (403)"""
        return "403 - Forbidden" in status_msg


class AsyncHTTPDownloader:
    """
    Custom Async HTTP class for downloading Drift Protocol data with retry logic and interval management
    Implements persistent disk-based storage in user_data/data/drift to enable data reuse
    """

    def __init__(self, max_concurrent=10, retry_delay=20, max_retries=3, request_timeout=30, data_dir=None):
        """
        Initialize AsyncHTTPDownloader

        Args:
            max_concurrent (int): Maximum concurrent requests
            retry_delay (int): Delay in seconds before retrying failed requests
            max_retries (int): Maximum number of retries per request
            request_timeout (int): Timeout for individual requests in seconds
            data_dir (str): Custom data directory path (defaults to user_data/data/drift)
        """
        self.max_concurrent = max_concurrent
        self.retry_delay = retry_delay
        self.max_retries = max_retries
        self.request_timeout = request_timeout
        self.session = None

        # Use user_data/data/drift/temp directory for temporary files
        if data_dir:
            self.temp_dir = Path(data_dir) / "temp"
        else:
            self.temp_dir = Path("user_data") / "data" / "drift" / "temp"

        # Ensure directory exists
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.temp_files = []
        logger.info(
            f"Using persistent temp directory for downloads: {self.temp_dir}")

    async def __aenter__(self):
        """Async context manager entry"""
        connector = aiohttp.TCPConnector(limit=self.max_concurrent)
        timeout = aiohttp.ClientTimeout(total=self.request_timeout)
        self.session = aiohttp.ClientSession(
            connector=connector, timeout=timeout)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
        # Note: No longer cleaning up temp files - they are preserved for reuse

    async def download_single_date(self, base_url, market, date, semaphore):
        """
        Download data for a single date with pagination support

        Args:
            base_url (str): Base URL for the API
            market (str): Market symbol
            date (datetime): Date being processed
            semaphore (asyncio.Semaphore): Semaphore to limit concurrent requests

        Returns:
            tuple: (date, DataFrame or None, status_message)
        """
        async with semaphore:
            # Add minimal delay to spread out requests slightly
            # 500ms to slow down requests significantly
            await asyncio.sleep(0.5)

            # Use temp directory instead of system temp
            import uuid
            temp_day_filename = f'drift_day_{date.strftime("%Y%m%d")}_{uuid.uuid4().hex[:8]}.parquet'
            temp_day_path = self.temp_dir / temp_day_filename

            page = 0
            total_trades = 0
            first_page = True

            while True:
                year = date.strftime('%Y')
                month = date.strftime('%m')
                day = date.strftime('%d')
                url = f"{base_url}/market/{market}/trades/{year}/{month}/{day}?format=csv&limit=100000&page={page}"

                for attempt in range(self.max_retries + 1):
                    try:
                        async with self.session.get(url) as response:
                            success, message, should_retry = HTTPStatusHandler.handle_response_status(
                                response.status, date)

                            if success:  # status == 200
                                content = await response.text()
                                if content and len(content.strip()) > 0:
                                    # Parse CSV data with infer_schema_length=0 to read all as strings first
                                    csv_data = io.StringIO(content)
                                    day_df = pl.read_csv(
                                        csv_data,
                                        infer_schema_length=0  # Read all columns as strings to avoid type conflicts
                                    )

                                    if len(day_df) > 0:
                                        # Optional: Add download day label for diagnostics (avoid conflicting with 'date' used for OHLCV)
                                        day_df = day_df.with_columns(
                                            pl.lit(date.date()).alias('dl_day'))

                                        page_size = len(day_df)
                                        total_trades += page_size

                                        # Store page data in memory for combining at the end (Windows file locking fix)
                                        if first_page:
                                            accumulated_data = day_df.clone()
                                            first_page = False
                                        else:
                                            # Accumulate data in memory instead of file appending
                                            try:
                                                accumulated_data = pl.concat(
                                                    [accumulated_data, day_df], how="diagonal")
                                            except Exception as concat_error:
                                                logger.warning(
                                                    f"Concat failed, reinitializing: {concat_error}")
                                                accumulated_data = day_df.clone()

                                        del day_df  # Clean up immediately

                                        # If we got less than 5000 trades, we've reached the end
                                        if page_size < 5000:
                                            # Write accumulated data to temp file once and return
                                            try:
                                                if 'accumulated_data' in locals() and len(accumulated_data) > 0:
                                                    accumulated_data.write_parquet(
                                                        temp_day_path)
                                                    final_day_df = accumulated_data.clone()
                                                    return date, final_day_df, f"{total_trades:,} trades ({page+1} pages)"
                                                else:
                                                    return date, None, "No data accumulated"
                                            except Exception as e:
                                                logger.warning(
                                                    f"Error writing accumulated data for {date}: {e}")
                                                return date, None, f"Error writing data: {str(e)[:50]}"
                                        else:
                                            # Got full page, continue to next page
                                            page += 1
                                            break  # Break retry loop, continue pagination loop
                                    else:
                                        # Empty page, we're done
                                        try:
                                            if 'accumulated_data' in locals() and len(accumulated_data) > 0:
                                                accumulated_data.write_parquet(
                                                    temp_day_path)
                                                final_day_df = accumulated_data.clone()
                                                return date, final_day_df, f"{total_trades:,} trades ({page} pages)"
                                            else:
                                                # Clean up temp file - DISABLED to preserve files
                                                return date, None, "No data (empty page)"
                                        except Exception as e:
                                            logger.warning(
                                                f"Error handling empty page for {date}: {e}")
                                            # Clean up temp file on error - DISABLED to preserve files
                                            # os.unlink(temp_day_path)
                                            return date, None, f"Error: {str(e)[:50]}"
                                else:
                                    # Empty response, we're done
                                    try:
                                        if os.path.exists(temp_day_path) and os.path.getsize(temp_day_path) > 0:
                                            final_day_df = pl.read_parquet(
                                                temp_day_path)
                                            # Clean up temp file
                                            # Preserve temp file for reuse
                                            # os.unlink(temp_day_path)
                                            return date, final_day_df, f"{total_trades:,} trades ({page} pages)"
                                        else:
                                            # Preserve temp file for reuse
                                            # os.unlink(temp_day_path)
                                            return date, None, "No data (empty response)"
                                    except Exception as e:
                                        logger.warning(
                                            f"Error handling empty response for {date}: {e}")
                                        # Preserve temp file for reuse
                                        # os.unlink(temp_day_path)
                                        return date, None, f"Error: {str(e)[:50]}"
                            else:
                                # Handle non-200 responses
                                if should_retry and attempt < self.max_retries:
                                    await asyncio.sleep(self.retry_delay)
                                    continue
                                else:
                                    # Return partial data if available
                                    try:
                                        if os.path.exists(temp_day_path) and os.path.getsize(temp_day_path) > 0:
                                            final_day_df = pl.read_parquet(
                                                temp_day_path)
                                            # Preserve temp file for reuse
                                            # os.unlink(temp_day_path)
                                            return date, final_day_df, f"{total_trades:,} trades (partial: {message})"
                                        else:
                                            # Preserve temp file for reuse
                                            # os.unlink(temp_day_path)
                                            return date, None, message
                                    except Exception as e:
                                        logger.warning(
                                            f"Error handling non-200 response for {date}: {e}")
                                        # Preserve temp file for reuse
                                        # os.unlink(temp_day_path)
                                        return date, None, f"Error: {str(e)[:50]}"

                    except asyncio.TimeoutError:
                        if attempt < self.max_retries:
                            await asyncio.sleep(self.retry_delay)
                            continue
                        else:
                            # Return partial data if available
                            try:
                                if os.path.exists(temp_day_path) and os.path.getsize(temp_day_path) > 0:
                                    final_day_df = pl.read_parquet(
                                        temp_day_path)
                                    # Preserve temp file for reuse
                                    # os.unlink(temp_day_path)
                                    return date, final_day_df, f"{total_trades:,} trades (timeout on page {page})"
                                else:
                                    # Preserve temp file for reuse
                                    # os.unlink(temp_day_path)
                                    return date, None, f"Timeout after {self.request_timeout}s"
                            except Exception as e:
                                logger.warning(
                                    f"Error handling timeout for {date}: {e}")
                                # Preserve temp file for reuse
                                # os.unlink(temp_day_path)
                                return date, None, f"Timeout error: {str(e)[:50]}"

                    except Exception as e:
                        if attempt < self.max_retries:
                            await asyncio.sleep(self.retry_delay)
                            continue
                        else:
                            # Return partial data if available
                            try:
                                if os.path.exists(temp_day_path) and os.path.getsize(temp_day_path) > 0:
                                    final_day_df = pl.read_parquet(
                                        temp_day_path)
                                    # Preserve temp file for reuse
                                    # os.unlink(temp_day_path)
                                    return date, final_day_df, f"{total_trades:,} trades (error on page {page}: {str(e)[:50]})"
                                else:
                                    # Preserve temp file for reuse
                                    # os.unlink(temp_day_path)
                                    return date, None, f"Error: {str(e)[:50]}"
                            except Exception as cleanup_error:
                                logger.warning(
                                    f"Error during cleanup for {date}: {cleanup_error}")
                                # Preserve temp file for reuse
                                # os.unlink(temp_day_path)
                                return date, None, f"Error: {str(e)[:50]}"

            # Should never reach here, but just in case
            try:
                if os.path.exists(temp_day_path) and os.path.getsize(temp_day_path) > 0:
                    final_day_df = pl.read_parquet(temp_day_path)
                    # Preserve temp file for reuse
                    # os.unlink(temp_day_path)
                    return date, final_day_df, f"{total_trades:,} trades ({page} pages)"
                else:
                    # Preserve temp file for reuse
                    # os.unlink(temp_day_path)
                    return date, None, "No data"
            except Exception as e:
                logger.warning(f"Error in final fallback for {date}: {e}")
                # Preserve temp file for reuse
                # os.unlink(temp_day_path)
                return date, None, f"Final error: {str(e)[:50]}"

    def _save_temp_data(self, date: datetime, df: pl.DataFrame, market: str = None) -> str:
        """Save DataFrame to persistent file with market and date in filename"""
        if market:
            temp_filename = f"drift_{market}_{date.strftime('%Y%m%d')}.parquet"
        else:
            temp_filename = f"drift_data_{date.strftime('%Y%m%d')}.parquet"
        temp_filepath = self.temp_dir / temp_filename
        df.write_parquet(str(temp_filepath))
        self.temp_files.append(str(temp_filepath))
        logger.debug(
            f"Saved {len(df)} rows to persistent file: {temp_filename}")
        return str(temp_filepath)

    def _check_existing_file(self, date: datetime, market: str = None) -> str | None:
        """Check if data file already exists for the given date and market"""
        if market:
            temp_filename = f"drift_{market}_{date.strftime('%Y%m%d')}.parquet"
        else:
            temp_filename = f"drift_data_{date.strftime('%Y%m%d')}.parquet"
        temp_filepath = self.temp_dir / temp_filename

        if temp_filepath.exists():
            logger.info(f"Found existing data file: {temp_filename}")
            return str(temp_filepath)
        return None

    def _get_existing_files_for_range(self, market: str, start_date: datetime, end_date: datetime) -> List[str]:
        """Get list of existing files that cover the requested date range"""
        existing_files = []
        current_date = start_date

        while current_date <= end_date:
            existing_file = self._check_existing_file(current_date, market)
            if existing_file:
                existing_files.append(existing_file)
            current_date += timedelta(days=1)

        return existing_files

    def _load_all_temp_data(self) -> List[pl.DataFrame]:
        """Load all temporary data files and return list of DataFrames"""
        dataframes = []
        for temp_file in self.temp_files:
            try:
                if os.path.exists(temp_file):
                    df = pl.read_parquet(temp_file)
                    dataframes.append(df)
                    logger.debug(f"Loaded {len(df)} rows from {temp_file}")
            except Exception as e:
                logger.warning(
                    f"Failed to load temporary file {temp_file}: {e}")
        return dataframes

    async def download_funding_rates_date_range(self, base_url, market, dates, limit=None):
        """
        Download funding rate data for multiple dates concurrently with rate limiting

        Args:
            base_url (str): Base URL for Drift data API
            market (str): Market symbol (e.g., 'BTC-PERP')
            dates (list): List of datetime objects
            limit (int): Maximum number of records to return

        Returns:
            tuple: (funding_rates_list, successful_days,
                    failed_days, empty_days)
        """
        semaphore = asyncio.Semaphore(self.max_concurrent)

        successful_days = 0
        failed_days = 0
        empty_days = 0
        all_funding_rates = []
        total_fetched = 0

        logger.info(
            f"Downloading funding rates for {market} over {len(dates)} dates")

        # Create tasks for all dates
        tasks = []
        for date in dates:
            if limit and total_fetched >= limit:
                break
            task = asyncio.create_task(
                self.download_funding_rates_single_date(
                    base_url, market, date, semaphore, limit - total_fetched if limit else None
                )
            )
            tasks.append(task)

        # Process results as they complete
        for completed_task in asyncio.as_completed(tasks):
            try:
                date, funding_rates, status = await completed_task

                if funding_rates:
                    all_funding_rates.extend(funding_rates)
                    total_fetched += len(funding_rates)
                    successful_days += 1
                    logger.info(f"{date.strftime('%Y-%m-%d')}: {status}")

                    # Check limit
                    if limit and total_fetched >= limit:
                        # Cancel remaining tasks
                        for task in tasks:
                            if not task.done():
                                task.cancel()
                        break
                elif "No data" in status:
                    empty_days += 1
                    logger.debug(f"{date.strftime('%Y-%m-%d')}: {status}")
                else:
                    failed_days += 1
                    logger.warning(f"{date.strftime('%Y-%m-%d')}: {status}")

            except Exception as e:
                failed_days += 1
                logger.error(f"Error processing funding rates task: {e}")

        return all_funding_rates, successful_days, failed_days, empty_days

    async def download_funding_rates_single_date(self, base_url, market, date, semaphore, limit=None):
        """
        Download funding rate data for a single date with pagination support

        Args:
            base_url (str): Base URL for the API
            market (str): Market symbol
            date (datetime): Date being processed
            semaphore (asyncio.Semaphore): Semaphore to limit concurrent requests
            limit (int): Maximum number of records to return for this date

        Returns:
            tuple: (date, funding_rates_list or None, status_message)
        """
        async with semaphore:
            # Add delay to spread out requests - increased for funding rate API
            # Increased to 1000ms for funding rate API rate limiting
            await asyncio.sleep(1.0)

            date_funding_rates = []
            page = 1
            total_records = 0

            # Format date for API
            date_str = date.strftime("%Y/%m/%d")

            while True:
                # Check limit
                if limit and total_records >= limit:
                    break

                url = f"{base_url}/market/{market}/fundingRates/{date_str}?page={page}&format=csv"

                for attempt in range(self.max_retries + 1):
                    try:
                        async with self.session.get(url) as response:
                            if response.status == 200:
                                csv_content = await response.text()
                                csv_content = csv_content.strip()

                                if not csv_content or csv_content == "":
                                    # Empty page, we're done
                                    if page == 1:
                                        return date, None, "No funding rate data available"
                                    else:
                                        # End of pagination
                                        break

                                # Parse CSV content
                                import csv
                                from io import StringIO

                                try:
                                    csv_reader = csv.DictReader(
                                        StringIO(csv_content))
                                    page_records = list(csv_reader)

                                    if not page_records:
                                        # End of pagination
                                        break

                                    # Process records for this page
                                    for record in page_records:
                                        if limit and total_records >= limit:
                                            break

                                        # Process funding rate record (using existing method)
                                        processed_record = self._process_funding_rate_record(
                                            record, market)
                                        if processed_record:
                                            date_funding_rates.append(
                                                processed_record)
                                            total_records += 1

                                    # If we got less than expected records, this might be the last page
                                    # Assuming page size is around 100
                                    if len(page_records) < 100:
                                        break

                                    page += 1

                                    # Add delay between pages to avoid rate limiting
                                    # 500ms delay between pages
                                    await asyncio.sleep(0.5)

                                    break  # Break retry loop, continue pagination loop

                                except Exception as csv_error:
                                    logger.warning(
                                        f"Error parsing CSV for {market} on {date_str}, page {page}: {csv_error}")
                                    if attempt < self.max_retries:
                                        await asyncio.sleep(self.retry_delay)
                                        continue
                                    else:
                                        return date, date_funding_rates if date_funding_rates else None, f"CSV parse error: {str(csv_error)[:50]}"

                            else:
                                # Handle non-200 responses
                                if response.status == 404:
                                    # No data for this date
                                    return date, None, f"No funding rate data (404)"
                                elif attempt < self.max_retries:
                                    logger.warning(
                                        f"HTTP {response.status} for {url}, attempt {attempt + 1}")
                                    await asyncio.sleep(self.retry_delay)
                                    continue
                                else:
                                    return date, date_funding_rates if date_funding_rates else None, f"CSV parse error: {str(csv_error)[:50]}"

                    except asyncio.TimeoutError:
                        if attempt < self.max_retries:
                            logger.warning(
                                f"Timeout for {url}, attempt {attempt + 1}")
                            await asyncio.sleep(self.retry_delay)
                            continue
                        else:
                            return date, date_funding_rates if date_funding_rates else None, f"HTTP {response.status} (final attempt)"
                    except Exception as e:
                        if attempt < self.max_retries:
                            logger.warning(
                                f"Request error for {url}, attempt {attempt + 1}: {e}")
                            await asyncio.sleep(self.retry_delay)
                            continue
                        else:
                            return date, date_funding_rates if date_funding_rates else None, f"Timeout (final attempt)"

                # End pagination loop if we've exhausted retries
                break

            if date_funding_rates:
                return date, date_funding_rates, f"{len(date_funding_rates)} funding rate records ({page} pages)"
            else:
                return date, None, "No funding rate data found"

    def _process_funding_rate_record(self, record: dict, market: str) -> dict | None:
        """
        Process a single funding rate record from Drift CSV format into VulcanTrader format.

        :param record: Raw funding rate record from CSV
        :param market: Market identifier
        :return: Processed funding rate dictionary or None if invalid
        """
        try:
            # Extract timestamp
            timestamp = int(record.get('ts', 0))
            if timestamp <= 0:
                return None

            # Convert timestamp to milliseconds for VulcanTrader
            timestamp_ms = timestamp * 1000

            # Extract funding rate (convert from percentage to decimal)
            funding_rate = float(record.get('fundingRate', 0))

            # Extract additional Drift-specific data
            funding_rate_long = float(record.get(
                'fundingRateLong', funding_rate))
            funding_rate_short = float(record.get(
                'fundingRateShort', funding_rate))

            # Oracle and mark prices
            oracle_price = float(record.get('oraclePriceTwap', 0))
            mark_price = float(record.get('markPriceTwap', 0))

            # Create VulcanTrader-compatible funding rate record
            funding_record = {
                'symbol': market,
                'timestamp': timestamp_ms,
                'datetime': datetime.fromtimestamp(timestamp, tz=UTC).isoformat(),
                'fundingRate': funding_rate,
                'fundingTime': timestamp_ms,
                # Drift-specific additional fields
                'fundingRateLong': funding_rate_long,
                'fundingRateShort': funding_rate_short,
                'cumulativeFundingRateLong': float(record.get('cumulativeFundingRateLong', 0)),
                'cumulativeFundingRateShort': float(record.get('cumulativeFundingRateShort', 0)),
                'oraclePriceTwap': oracle_price,
                'markPriceTwap': mark_price,
                'periodRevenue': float(record.get('periodRevenue', 0)),
                'marketIndex': int(record.get('marketIndex', 0)),
                'recordId': record.get('recordId', ''),
                'txSig': record.get('txSig', ''),
            }

            return funding_record

        except (ValueError, KeyError) as e:
            logger.warning(f"Error processing funding rate record: {e}")
            return None

    async def download_date_range(self, base_url, market, dates, progress_callback=None):
        """
        Download data for multiple dates concurrently with rate limiting
        Checks for existing files first and only downloads missing data

        Args:
            base_url (str): Base URL for Drift data
            market (str): Market symbol (e.g., 'BTC-PERP')
            dates (list): List of datetime objects
            progress_callback (callable): Optional callback for progress updates

        Returns:
            tuple: (temp_files_list, successful_days, failed_days, empty_days)
        """
        semaphore = asyncio.Semaphore(self.max_concurrent)

        # Check for existing files and filter out dates we already have
        dates_to_download = []
        existing_files = []

        for date in dates:
            existing_file = self._check_existing_file(date, market)
            if existing_file:
                existing_files.append(existing_file)
                logger.info(
                    f"Using existing file for {date.strftime('%Y-%m-%d')}: {Path(existing_file).name}")
            else:
                dates_to_download.append(date)

        # Add existing files to temp_files list
        self.temp_files.extend(existing_files)

        logger.info(
            f"Found {len(existing_files)} existing files, need to download {len(dates_to_download)} dates")

        # Track statistics, but don't store data in memory
        # Count existing files as successful
        successful_days = len(existing_files)
        failed_days = 0
        empty_days = 0
        consecutive_403s = 0
        # Stop after 3 consecutive 403s (indicates access restriction)
        max_consecutive_403s = 3

        # Only download dates we don't already have
        if not dates_to_download:
            logger.info(
                "All requested data already exists, no downloads needed")
            return self.temp_files, successful_days, failed_days, empty_days

        # Process dates in very small batches to minimize memory usage
        batch_size = 1  # Reduced from 3 to 1 for minimal memory footprint

        for batch_start in range(0, len(dates_to_download), batch_size):
            batch_end = min(batch_start + batch_size, len(dates_to_download))
            batch_dates = dates_to_download[batch_start:batch_end]

            # Create tasks for this batch
            tasks = []
            for date in batch_dates:
                task = asyncio.create_task(
                    self.download_single_date(
                        base_url, market, date, semaphore)
                )
                tasks.append(task)

            # Wait for batch to complete
            batch_results = await asyncio.gather(*tasks)

            # Process batch results
            for i, (date, day_df, status_msg) in enumerate(batch_results):
                overall_index = batch_start + i + 1
                progress_pct = (overall_index / len(dates_to_download)) * 100
                progress_msg = f"[ASYNC] Day {overall_index}/{len(dates_to_download)} ({progress_pct:.1f}%) - {date.strftime('%Y-%m-%d')} {status_msg}"

                if progress_callback:
                    progress_callback(progress_msg)
                else:
                    logger.info(progress_msg)

                # Categorize results and save to disk instead of keeping in memory
                if day_df is not None:
                    # Save DataFrame to persistent file with market info
                    self._save_temp_data(date, day_df, market)
                    successful_days += 1
                    consecutive_403s = 0  # Reset consecutive 403 counter on success
                elif HTTPStatusHandler.is_access_denied(status_msg):
                    empty_days += 1
                    consecutive_403s += 1
                elif "No data" in status_msg:
                    empty_days += 1
                    consecutive_403s = 0  # Reset for other types of empty data
                else:
                    failed_days += 1
                    consecutive_403s = 0  # Reset for other types of failures

                # Check if we should stop due to too many consecutive 403s
                if consecutive_403s >= max_consecutive_403s:
                    logger.warning(
                        f"Stopping download after {consecutive_403s} consecutive 403s")
                    logger.info(
                        f"This indicates access restrictions or rate limiting")
                    logger.info(
                        f"Last accessible date: {date.strftime('%Y-%m-%d')}")
                    break

            # Break out of batch loop if we hit the consecutive 403 limit
            if consecutive_403s >= max_consecutive_403s:
                break

            # Add minimal delay between batches
            if batch_end < len(dates):  # Don't delay after the last batch
                await asyncio.sleep(0.2)  # 200ms delay between batches

        # Return temp files list instead of data in memory
        return self.temp_files, successful_days, failed_days, empty_days


class Drift(Exchange):
    """
    Drift Protocol exchange implementation with orderflow construction and perp trading.
    Drift is a decentralized perpetuals exchange built on Solana.
    """

    trader_has: TraderHas = {
        # Drift live connector currently does not implement private stoploss order placement
        # via the ccxt adapter. Keeping this disabled avoids VulcanTrader trying to place
        # stoploss orders on-exchange through ccxt.
        "stoploss_on_exchange": False,
        "stop_price_param": "stopPrice",
        "stop_price_prop": "stopPrice",
        "stoploss_order_types": {"limit": "stop_limit", "market": "stop_market"},
        "stoploss_blocks_assets": False,  # Stoploss orders don't block assets on Drift
        "order_time_in_force": ["GTC", "IOC", "FOK", "POST_ONLY"],
        "trades_pagination": "time",
        "trades_pagination_arg": "since",
        "trades_has_history": True,
        "fetch_orders_limit_minutes": None,  # No limit on fetching orders
        "l2_limit_range": [25, 50, 100],
        "ws_enabled": False,  # WebSocket not implemented for Drift adapter
        "ohlcv_has_history": True,
        "ohlcv_partial_candle": True,
        "ohlcv_require_since": False,
        "download_data_parallel_quick": False,
        "tickers_have_quoteVolume": True,
        "tickers_have_percentage": True,
        # Drift tickers come from the Data API /contracts endpoint and do not provide
        # best bid/ask. For realistic pricing, we use the DLOB orderbook endpoint.
        "tickers_have_bid_ask": False,
        "tickers_have_price": True,
        "funding_fee_candle_limit": 1000,
        "mark_ohlcv_price": "mark",
        "mark_ohlcv_timeframe": "8h",
        "funding_fee_timeframe": "8h",
        "floor_leverage": True,
        "uses_leverage_tiers": False,  # Drift doesn't use leverage tiers
        "needs_trading_fees": True,
        "order_props_in_contracts": ["amount", "cost", "filled", "remaining"],
    }

    trader_has_futures: TraderHas = {
        "stoploss_order_types": {"limit": "stop", "market": "stop_market"},
        "stoploss_blocks_assets": False,
        "tickers_have_price": True,
        "floor_leverage": True,
        "fetch_orders_limit_minutes": None,
        "stop_price_type_field": "workingType",
        "order_props_in_contracts": ["amount", "cost", "filled", "remaining"],
    }

    _supported_trading_mode_margin_pairs: list[tuple[TradingMode, MarginMode]] = [
        # Drift primarily supports perpetual futures
        (TradingMode.FUTURES, MarginMode.CROSS),
        (TradingMode.FUTURES, MarginMode.ISOLATED),
        # Spot trading is also available
        (TradingMode.SPOT, MarginMode.NONE),
    ]

    # Drift-specific constants (from DRIFT.txt)
    BASE_PRECISION = 10 ** 9
    PRICE_PRECISION = 10 ** 6
    QUOTE_PRECISION = 10 ** 6

    # Public endpoints used for market discovery + tickers
    _DATA_API_BASE = "https://data.api.drift.trade"
    _DATA_API_CONTRACTS = f"{_DATA_API_BASE}/contracts"

    # In-memory cache TTL for contracts.
    # This is short on purpose to avoid “locking in” stale volumes for too long,
    # but prevents duplicate requests during startup (load_markets + first get_tickers).
    _CONTRACTS_MEM_CACHE_TTL = timedelta(minutes=5)

    # Drift Data API is often protected by bot mitigation.
    # Using a browser-like UA and explicit accept headers avoids frequent 403 HTML responses.
    _PUBLIC_HTTP_HEADERS: dict[str, str] = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json,text/plain,*/*",
        # Some Drift Data API endpoints are behind bot mitigation.
        # These additional headers can help reduce HTML/403 responses.
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://app.drift.trade",
        "Referer": "https://app.drift.trade/",
    }

    # Drift DLOB (orderbook) public endpoint.
    # This provides real-time L2 data without requiring private keys.
    _DLOB_API_BASE = "https://dlob.drift.trade"

    # Cache orderbook responses to respect API limits.
    # Default: 1 second, configurable via exchange.orderbook_cache_ttl.
    _ORDERBOOK_CACHE_TTL_DEFAULT_S = 1

    # Drift Data API candles endpoint sometimes expects timestamps in a specific order.
    # Some deployments appear to accept `startTs > endTs` (newest -> oldest), while others
    # accept the conventional `startTs < endTs` (oldest -> newest). We start with the
    # historically observed behavior, but will auto-detect and switch if we observe a 403
    # and the alternate ordering succeeds.
    _CANDLES_TS_ORDER_DEFAULT: str = "reversed"  # "reversed" | "normal"

    # Avoid log-spam when the Drift Data API is protected by WAF/bot mitigation.
    # We will log at most once per (endpoint+market) per TTL.
    _DATA_API_WARN_TTL_S: int = 300

    def get_tickers(
        self,
        symbols: list[str] | None = None,
        *,
        cached: bool = False,
        market_type: TradingMode | None = None,
    ) -> Tickers:
        """Return tickers for Drift.

        VulcanTrader's VolumePairList requires `quoteVolume` (key name: quoteVolume).
        Drift Data API provides `quote_volume` (snake_case) per contract.

        We map Data API fields into ccxt-like ticker dicts.
        """

        # Since this is a lightweight HTTP call and we already do caching in PairListManager,
        # we ignore `cached` for now.
        contracts = self._fetch_contracts(for_tickers=True)

        tickers: dict[str, dict[str, Any]] = {}
        for c in contracts:
            symbol = str(c.get("ticker_id", "") or "").strip()
            if not symbol or not symbol.endswith("-PERP"):
                continue
            if symbols and symbol not in symbols:
                continue

            # Map fields (all numbers arrive as strings)
            last = float(c.get("last_price") or 0.0)
            high = float(c.get("high") or 0.0)
            low = float(c.get("low") or 0.0)
            base_volume = float(c.get("base_volume") or 0.0)
            quote_volume = float(c.get("quote_volume") or 0.0)

            # Minimal ccxt-like ticker structure used by pairlists
            tickers[symbol] = {
                "symbol": symbol,
                "last": last,
                "high": high,
                "low": low,
                "baseVolume": base_volume,
                "quoteVolume": quote_volume,
                # Optional fields
                "bid": None,
                "ask": None,
                "percentage": None,
                "info": c,
            }

        return tickers

    def fetch_l2_order_book(self, pair: str, limit: int = 100) -> OrderBook:
        """Fetch L2 orderbook for Drift perps.

        This enables realistic pricing in dry-run / live modes when `use_order_book = true`.

        Source: Drift DLOB public endpoint: `GET https://dlob.drift.trade/l2?marketName=BTC-PERP&depth=25`.

        Notes:
        - `price` is returned as integer in PRICE_PRECISION (1e6).
        - `size` is returned as integer in BASE_PRECISION (1e9) for perps.
        """

        # Lazily init cache (Exchange.__init__ runs before our __init__ finishes).
        if not hasattr(self, "_l2_orderbook_cache"):
            ttl_s = int(
                self._config.get("exchange", {}).get(
                    "orderbook_cache_ttl", self._ORDERBOOK_CACHE_TTL_DEFAULT_S
                )
            )
            # A small cache is enough - we key by pair+limit.
            self._l2_orderbook_cache = FtTTLCache(
                maxsize=512, ttl=max(1, ttl_s))

        cache_key = f"{pair}:{limit}"
        cached = self._l2_orderbook_cache.get(cache_key)
        if cached is not None:
            return cached

        market = self._convert_pair_to_drift_market(pair, CandleType.FUTURES)

        # Keep payload small - a few levels are enough for pricing and slippage estimation.
        # VulcanTrader often calls with 1, 20. We'll request at least 5 levels to be useful.
        depth = int(min(max(limit, 5), 100))

        import requests

        url = f"{self._DLOB_API_BASE}/l2"
        try:
            resp = requests.get(
                url,
                params={"marketName": market, "depth": depth},
                timeout=10,
                headers=self._PUBLIC_HTTP_HEADERS,
            )
        except Exception as e:
            raise TemporaryError(
                f"Drift DLOB orderbook request failed: {e}") from e

        if resp.status_code != 200:
            raise TemporaryError(
                f"Drift DLOB orderbook error {resp.status_code} for {market}: {resp.text[:200]}"
            )

        try:
            data = resp.json()
        except Exception as e:
            raise TemporaryError(
                f"Drift DLOB orderbook returned non-JSON for {market}: {resp.text[:200]}"
            ) from e

        def _parse_side(side: str) -> list[list[float]]:
            out: list[list[float]] = []
            for lvl in data.get(side, []) or []:
                try:
                    # price is in PRICE_PRECISION, size is in BASE_PRECISION
                    p = float(int(lvl.get("price"))) / self.PRICE_PRECISION
                    a = float(int(lvl.get("size"))) / self.BASE_PRECISION
                    if p > 0 and a > 0:
                        out.append([p, a])
                except Exception:
                    continue
            return out

        orderbook: OrderBook = {
            "symbol": pair,
            "bids": _parse_side("bids"),
            "asks": _parse_side("asks"),
            "timestamp": int(datetime.now(tz=UTC).timestamp() * 1000),
            "datetime": datetime.now(tz=UTC).isoformat(),
            "nonce": None,
        }

        # Cache and return
        self._l2_orderbook_cache[cache_key] = orderbook
        return orderbook

    def exchange_has(self, endpoint: str) -> bool:
        """Check if exchange has specific endpoint capability.

        We expose fetchTickers since VolumePairList relies on Exchange.get_tickers().
        """
        capabilities = {
            "fetchOHLCV": True,
            "fetchTrades": True,
            "fetchOrderBook": True,
            "fetchL2OrderBook": True,
            "fetchTicker": True,
            "fetchTickers": True,
            "fetchMyTrades": True,
            "fetchOrders": True,
            "fetchOpenOrders": True,
            "fetchClosedOrders": True,
            # Private endpoints not implemented via the adapter yet.
            "createOrder": False,
            "cancelOrder": False,
            "editOrder": False,
        }
        return capabilities.get(endpoint, False)

    # Processing configuration
    # Set to False for simple in-memory processing (faster but uses more memory)
    USE_CHUNKED_PROCESSING = True

    def __init__(self, *args, **kwargs) -> None:
        """Initialize Drift exchange connector"""
        # NOTE:
        # Exchange.__init__ may call `reload_markets()` (when validate=True), which calls
        # Drift.load_markets() -> Drift._fetch_contracts().
        # Therefore, any attributes used by `_fetch_contracts()` must exist *before*
        # calling `super().__init__()`.

        # Used for diagnostics / logging (VolumePairList relies on full market discovery).
        # Values: "data_api" | "memory_cache" | "disk_cache" | "config_fallback" | None
        self._contracts_source: str | None = None

        # Data API diagnostics / mitigations
        # Tracks next allowed log timestamp for repetitive warnings.
        self._data_api_warn_next_ts: dict[str, int] = {}
        # Adaptive ordering for candles endpoint timestamps.
        self._candles_ts_order: str = self._CANDLES_TS_ORDER_DEFAULT

        # Contracts cache (real data only).
        self._contracts_cache: list[dict[str, Any]] | None = None
        self._contracts_cache_ts: datetime | None = None
        self._contracts_fetch_count: int = 0

        super().__init__(*args, **kwargs)

        # Concurrency limiter for Drift Data API candle endpoint.
        # The base Exchange will schedule many fetch_ohlcv coroutines at once (one per pair).
        # Drift's public Data API is often WAF/bot-mitigated and can respond with mass 403s
        # when flooded. This semaphore caps concurrent candle HTTP requests.
        max_conc = int(
            self._config.get("exchange", {}).get(
                "drift_data_api_max_concurrent", 2) or 2
        )
        self._candles_api_semaphore = asyncio.Semaphore(max(1, max_conc))
        # Small jitter/delay between candle requests (seconds).
        self._candles_api_delay_s = float(
            self._config.get("exchange", {}).get(
                "drift_data_api_request_delay_s", 0.05) or 0.0
        )

        # Initialize Drift-specific attributes
        self._drift_client = None
        self._orderflow_cache = {}
        self._market_data_cache = {}

        # Add missing attributes for cleanup compatibility
        self._exchange_ws = None
        self._ws_async = None

        # Initialize event loop for async operations
        try:
            self.loop = asyncio.get_event_loop()
        except RuntimeError:
            # No event loop in current thread, create a new one
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

    def _data_api_should_log(self, key: str, *, ttl_s: int | None = None) -> bool:
        """Return True if we should emit a warning log for `key`.

        This is used to suppress repetitive 403/404 warnings from public Drift Data API.
        """
        ttl_s = int(ttl_s or self._DATA_API_WARN_TTL_S)
        now_s = int(datetime.now(tz=UTC).timestamp())
        nxt = self._data_api_warn_next_ts.get(key, 0)
        if now_s >= nxt:
            self._data_api_warn_next_ts[key] = now_s + ttl_s
            return True
        return False

    def _get_public_http_headers(self) -> dict[str, str]:
        """Return headers for Drift public HTTP endpoints.

        Allows users to inject additional headers (e.g. cookies / clearance tokens)
        via config:

            "exchange": {
              ...,
              "drift_data_api_headers": {
                "Cookie": "cf_clearance=..."
              }
            }
        """
        base = dict(self._PUBLIC_HTTP_HEADERS)
        extra = self._config.get("exchange", {}).get(
            "drift_data_api_headers", {})
        if isinstance(extra, dict):
            # Only keep string->string headers to avoid aiohttp issues.
            for k, v in extra.items():
                if isinstance(k, str) and isinstance(v, str) and k and v:
                    base[k] = v
        return base

    @property
    def _contracts_cache_path(self) -> Path:
        """Path to disk cache for Drift contracts.

        This uses the configured `datadir` (defaults to `user_data/data/drift`).
        """
        datadir = Path(self._config.get("datadir", "user_data/data/drift"))
        datadir.mkdir(parents=True, exist_ok=True)
        return datadir / "contracts_cache.json"

    def _load_contracts_disk_cache(self) -> list[dict[str, Any]] | None:
        path = self._contracts_cache_path
        if not path.exists():
            return None
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            contracts = raw.get("contracts")
            if not isinstance(contracts, list):
                return None
            # Keep only perps
            return [c for c in contracts if str(c.get("product_type", "")).upper() == "PERP"]
        except Exception as e:
            logger.warning(
                f"Failed to read Drift contracts disk cache {path}: {e}")
            return None

    def _save_contracts_disk_cache(self, contracts: list[dict[str, Any]]) -> None:
        try:
            payload = {
                "timestamp": datetime.now(tz=UTC).isoformat(),
                "contracts": contracts,
            }
            self._contracts_cache_path.write_text(
                json.dumps(payload, ensure_ascii=False), encoding="utf-8"
            )
        except Exception as e:
            logger.warning(f"Failed to write Drift contracts disk cache: {e}")

    def _init_ccxt(self, exchange_config: dict, sync: bool, ccxt_kwargs: dict) -> None:
        """
        Override ccxt initialization for Drift exchange.
        Drift is not a ccxt exchange, so we return a ccxt-like adapter object.

        VulcanTrader's Exchange base class expects a ccxt compatible object for both
        sync (`self._api`) and async (`self._api_async`) clients.
        For Drift we emulate the small subset of the ccxt interface that VulcanTrader
        actually uses for live trading (fetch_ohlcv / fetch_trades / fetch_ticker / calculate_fee).
        """
        from urllib.parse import urlencode

        class DriftCCXTAdapter:
            """A minimal ccxt-like adapter for Drift.

            This exists so VulcanTrader can interact with Drift like it does with real ccxt exchanges
            (e.g. Binance): the base `Exchange` code calls into `self._api_async.fetch_ohlcv` etc.
            """

            def __init__(self, parent: "Drift", *, is_async: bool) -> None:
                self._parent = parent
                self._is_async = is_async

                self.id = "drift"
                self.name = "Drift"
                self.markets: dict = {}
                self.symbols: list[str] = []

                # ccxt objects expose `options` - VulcanTrader accesses this for timeframes.
                self.precisionMode = 2  # DECIMAL_PLACES
                self.timeframes = {
                    "1m": 60,
                    "3m": 180,
                    "5m": 300,
                    "15m": 900,
                    "30m": 1800,
                    "1h": 3600,
                    "2h": 7200,
                    "4h": 14400,
                    "6h": 21600,
                    "8h": 28800,
                    "12h": 43200,
                    "1d": 86400,
                }
                # Used by VulcanTrader.exchange.exchange.Exchange.timeframes property.
                self.options = {
                    "timeframes": {
                        "spot": self.timeframes,
                        "swap": self.timeframes,
                    }
                }

                # Capabilities - used by some parts of VulcanTrader.
                self.has = {
                    "fetchOHLCV": True,
                    "fetchTrades": True,
                    "fetchTicker": True,
                    "fetchTickers": True,
                    # Other endpoints are still mocked/unsupported for now
                    "createOrder": False,
                    "cancelOrder": False,
                    "fetchBalance": False,
                }

                # Features - used by ccxt feature helpers in VulcanTrader.
                # Only implement what we need for candle limits.
                self.features = {
                    "spot": {"fetchOHLCV": {"limit": 1000}},
                    "swap": {"linear": {"fetchOHLCV": {"limit": 1000}}},
                }

                # aiohttp session for async adapter.
                self.session: aiohttp.ClientSession | None = None

            def set_markets_from_exchange(self, other: Any) -> None:
                """ccxt helper used by VulcanTrader on real exchanges."""
                self.markets = getattr(other, "markets", {}) or {}
                self.symbols = list(self.markets.keys())

            def load_markets(self, *args, **kwargs):
                # Drift markets are managed by the parent `Drift` exchange.
                self.markets = self._parent.markets
                self.symbols = list(self.markets.keys())
                return self.markets

            async def _get_json(self, url: str, params: dict[str, Any] | None = None) -> Any:
                params = params or {}
                if self.session is None:
                    # Drift endpoints can be slow, especially when fetching public trades.
                    # Use a slightly higher timeout and lower connector concurrency to reduce
                    # timeouts caused by too many concurrent requests.
                    timeout = aiohttp.ClientTimeout(total=60)
                    connector = aiohttp.TCPConnector(limit=10)
                    self.session = aiohttp.ClientSession(
                        timeout=timeout,
                        connector=connector,
                        headers=self._parent._get_public_http_headers(),
                    )
                async with self.session.get(url, params=params) as resp:
                    txt = await resp.text()

                    # Drift Data API frequently responds with 403 HTML pages.
                    # For live trading, prefer degraded behavior (return empty) over crashing.
                    if resp.status in (403, 404):
                        # WAF / bot mitigation is very common here. Do not spam logs for each pair.
                        # Only emit once per market+endpoint per TTL.
                        market = params.get(
                            "market") or params.get("marketName")
                        log_key = f"data_api:{resp.status}:{url}:{market or ''}"
                        if self._parent._data_api_should_log(log_key):
                            logger.warning(
                                f"Drift Data API returned {resp.status} for {url}?{urlencode(params)}. "
                                "Treating as empty response (bot mitigation / no data)."
                            )
                        return {}

                    if resp.status != 200:
                        raise TemporaryError(
                            f"Drift Data API error {resp.status} for {url}?{urlencode(params)}: {txt[:200]}"
                        )
                    try:
                        return await resp.json()
                    except Exception:
                        raise TemporaryError(
                            f"Drift Data API returned non-JSON for {url}?{urlencode(params)}: {txt[:200]}"
                        )

            async def _get_json_with_status(
                self, url: str, params: dict[str, Any] | None = None
            ) -> tuple[int, Any, str]:
                """Like _get_json but also returns the http status and raw text.

                This is used by the candles endpoint to implement adaptive timestamp ordering.
                """
                params = params or {}
                if self.session is None:
                    timeout = aiohttp.ClientTimeout(total=60)
                    connector = aiohttp.TCPConnector(limit=10)
                    self.session = aiohttp.ClientSession(
                        timeout=timeout,
                        connector=connector,
                        headers=self._parent._get_public_http_headers(),
                    )
                async with self.session.get(url, params=params) as resp:
                    txt = await resp.text()
                    if resp.status == 200:
                        try:
                            return resp.status, await resp.json(), txt
                        except Exception:
                            return resp.status, {}, txt
                    return resp.status, {}, txt

            def _symbol_to_market(self, symbol: str) -> str:
                # VulcanTrader futures config uses "BTC-PERP" already.
                if "-PERP" in symbol:
                    return symbol
                if "/" in symbol:
                    base = symbol.split("/")[0]
                    return f"{base}-PERP"
                return symbol

            def _timeframe_to_resolution_minutes(self, timeframe: str) -> int:
                # Drift Data API candles endpoint uses integers in the path (minutes).
                # We convert common VulcanTrader/ccxt timeframes to minutes.
                if timeframe.endswith("m"):
                    return int(timeframe[:-1])
                if timeframe.endswith("h"):
                    return int(timeframe[:-1]) * 60
                if timeframe.endswith("d"):
                    return int(timeframe[:-1]) * 60 * 24
                if timeframe.endswith("w"):
                    return int(timeframe[:-1]) * 60 * 24 * 7
                raise OperationalException(
                    f"Unsupported timeframe for Drift candles: {timeframe}")

            async def fetch_ohlcv(
                self,
                symbol: str,
                timeframe: str = "1m",
                since: int | None = None,
                limit: int | None = None,
                params: dict[str, Any] | None = None,
            ) -> list[list]:
                """ccxt-compatible async OHLCV.

                Uses Drift Data API candles endpoint:
                  GET /market/{market}/candles/{resolution_minutes}

                IMPORTANT: This endpoint expects `startTs` to be AFTER `endTs` (reversed).
                Both values are in unix seconds.
                """
                market = self._symbol_to_market(symbol)
                res_m = self._timeframe_to_resolution_minutes(timeframe)
                tf_s = res_m * 60
                now_s = int(datetime.now(tz=UTC).timestamp())

                candle_limit = int(limit or 500)
                if since is not None:
                    start_range_s = int(since / 1000)
                    end_range_s = start_range_s + candle_limit * tf_s
                    if end_range_s > now_s:
                        end_range_s = now_s
                else:
                    end_range_s = now_s
                    start_range_s = now_s - candle_limit * tf_s

                # Decide timestamp ordering for Drift candles endpoint.
                # Some deployments expect reversed ordering (newest->oldest).
                if self._parent._candles_ts_order == "normal":
                    startTs = start_range_s
                    endTs = end_range_s
                else:
                    # default: reversed
                    startTs = end_range_s
                    endTs = start_range_s

                url = f"{self._parent._DATA_API_BASE}/market/{market}/candles/{res_m}"

                # Cap concurrency to avoid mass-403 when many pairs are refreshed at once.
                async with self._parent._candles_api_semaphore:
                    if self._parent._candles_api_delay_s:
                        await asyncio.sleep(self._parent._candles_api_delay_s)

                    params_ = {"startTs": startTs,
                               "endTs": endTs, "limit": candle_limit}
                    status, data, txt = await self._get_json_with_status(url, params=params_)

                    # If we got a 403, try the alternative ordering once.
                    # If it succeeds, lock that ordering in for this bot run.
                    if status == 403:
                        alt_params = dict(params_)
                        alt_params["startTs"], alt_params["endTs"] = (
                            alt_params["endTs"],
                            alt_params["startTs"],
                        )
                        alt_status, alt_data, _ = await self._get_json_with_status(
                            url, params=alt_params
                        )
                        if alt_status == 200 and isinstance(alt_data, dict) and isinstance(
                            alt_data.get("records"), list
                        ):
                            # Switch ordering for subsequent requests.
                            self._parent._candles_ts_order = (
                                "normal" if self._parent._candles_ts_order == "reversed" else "reversed"
                            )
                            data = alt_data
                            status = alt_status
                        else:
                            # Rate-limit warning logs.
                            log_key = f"candles:{market}:{res_m}:{status}"
                            if self._parent._data_api_should_log(log_key):
                                logger.warning(
                                    "Drift Data API returned %s for candles %s (timeframe=%s, startTs=%s, endTs=%s). "
                                    "Treating as empty response (bot mitigation / no data).",
                                    status,
                                    market,
                                    timeframe,
                                    params_["startTs"],
                                    params_["endTs"],
                                )
                            return []
                    elif status in (404,):
                        return []
                    elif status == 429:
                        # Best-effort backoff. Don't make this fatal.
                        log_key = f"candles:{market}:{res_m}:429"
                        if self._parent._data_api_should_log(log_key, ttl_s=60):
                            logger.warning(
                                "Drift Data API rate-limited (429) for candles %s (timeframe=%s). "
                                "Treating as empty response.",
                                market,
                                timeframe,
                            )
                        return []
                    elif status != 200:
                        raise TemporaryError(
                            f"Drift Data API error {status} for {url}?{urlencode(params_)}: {txt[:200]}"
                        )

                records = data.get("records") if isinstance(
                    data, dict) else None
                if not isinstance(records, list):
                    return []

                # Convert to ccxt OHLCV lists: [timestamp_ms, open, high, low, close, volume]
                out: list[list] = []
                for r in records:
                    try:
                        ts_s = int(r.get("ts"))
                        out.append(
                            [
                                ts_s * 1000,
                                float(r.get("fillOpen")),
                                float(r.get("fillHigh")),
                                float(r.get("fillLow")),
                                float(r.get("fillClose")),
                                float(r.get("baseVolume") or 0.0),
                            ]
                        )
                    except Exception:
                        continue
                # Ensure ascending order (ccxt usually returns asc)
                out.sort(key=lambda x: x[0])
                return out

            async def fetch_trades(
                self,
                symbol: str,
                since: int | None = None,
                limit: int | None = None,
                params: dict[str, Any] | None = None,
            ) -> list[dict[str, Any]]:
                """ccxt-compatible async trades.

                NOTE: Drift Data API trade endpoint is currently date-based and may return large
                responses. We cache results per market/day and then filter by `since`.
                """
                market = self._symbol_to_market(symbol)
                limit_ = int(limit or 1000)
                # Fallback: if since is not given, only look back 1 hour to keep payload small.
                now_s = int(datetime.now(tz=UTC).timestamp())
                since_ms = since if since is not None else (
                    now_s - 3600) * 1000
                since_s = int(since_ms / 1000) if since_ms is not None else 0

                # Drift trades are exposed as daily CSV files.
                # Page 0 contains the newest trades (descending ts), which lets us stop early
                # once we hit trades older than `since`.
                max_pages_per_day = 25  # hard cap to avoid runaway downloads
                max_days = 2  # keep bounded for live usage (today + yesterday)

                if self.session is None:
                    timeout = aiohttp.ClientTimeout(total=60)
                    connector = aiohttp.TCPConnector(limit=10)
                    self.session = aiohttp.ClientSession(
                        timeout=timeout,
                        connector=connector,
                        headers=self._parent._get_public_http_headers(),
                    )

                trades: list[dict[str, Any]] = []
                reached_since = False

                # iterate days (today, yesterday) until we have enough data
                day = datetime.now(tz=UTC).date()
                for day_offset in range(max_days):
                    if reached_since or (limit_ and len(trades) >= limit_):
                        break
                    d = day - timedelta(days=day_offset)
                    y, m, dd = d.year, d.month, d.day
                    url = f"{self._parent._DATA_API_BASE}/market/{market}/trades/{y}/{m:02d}/{dd:02d}"

                    for page in range(max_pages_per_day):
                        if reached_since or (limit_ and len(trades) >= limit_):
                            break

                        try:
                            async with self.session.get(
                                url,
                                params={"format": "csv", "page": page},
                            ) as resp:
                                txt = await resp.text()
                                if resp.status != 200:
                                    # Drift Data API frequently responds with 403 HTML pages.
                                    # For live orderflow, treat this as "no trades" instead of
                                    # crashing the bot.
                                    if resp.status in (403, 404):
                                        log_key = f"trades:{market}:{resp.status}"
                                        if self._parent._data_api_should_log(log_key):
                                            logger.warning(
                                                "Drift trades API returned %s for %s (%s, page=%s). "
                                                "Treating as empty trades (bot mitigation / no data). "
                                                "If this persists, consider adding `exchange.drift_data_api_headers` "
                                                "(e.g. cf_clearance cookie).",
                                                resp.status,
                                                market,
                                                url,
                                                page,
                                            )
                                        return []
                                    raise TemporaryError(
                                        f"Drift trades API error {resp.status} for {url}: {txt[:200]}"
                                    )
                        except asyncio.TimeoutError as e:
                            # For live orderflow, a timeout should not be fatal.
                            logger.warning(
                                f"Timeout fetching Drift trades for {market} page={page} - treating as empty trades."
                            )
                            return []

                        import csv
                        import io

                        rows = list(csv.DictReader(io.StringIO(txt)))
                        if not rows:
                            break

                        # Convert rows into ccxt-like trade dicts.
                        # Rows are newest-first, so we can stop early when ts < since.
                        for row in rows:
                            try:
                                ts_s = int(float(row.get("ts") or 0))
                                if since_s and ts_s < since_s:
                                    reached_since = True
                                    break

                                oracle_price = float(
                                    row.get("oraclePrice") or 0)
                                price = (
                                    oracle_price
                                    if oracle_price > 1000
                                    else oracle_price / self._parent.PRICE_PRECISION
                                )

                                amount = float(
                                    row.get("baseAssetAmountFilled") or 0)
                                cost = float(
                                    row.get("quoteAssetAmountFilled") or 0)
                                direction = (
                                    row.get("takerOrderDirection") or "long").lower()
                                side = "buy" if direction == "long" else "sell"

                                trade_id = str(
                                    row.get("fillRecordId") or row.get("txSig") or "")
                                trades.append(
                                    {
                                        "id": trade_id,
                                        # Required by VulcanTrader.data.converter.trades_dict_to_list
                                        # DEFAULT_TRADES_COLUMNS = [timestamp, id, type, side, price, amount, cost]
                                        "type": "limit",
                                        "timestamp": ts_s * 1000,
                                        "datetime": datetime.fromtimestamp(ts_s, tz=UTC).isoformat(),
                                        "symbol": symbol,
                                        "side": side,
                                        "price": price,
                                        "amount": amount,
                                        "cost": cost,
                                        "info": row,
                                    }
                                )
                                if limit_ and len(trades) >= limit_:
                                    break
                            except Exception:
                                continue

                        # Heuristic: if we got fewer than 5000 rows, we're at the end of the day.
                        # (Drift seems to paginate around 5000 trades per page)
                        if len(rows) < 5000:
                            break

                trades.sort(key=lambda t: t["timestamp"])
                if limit_ and len(trades) > limit_:
                    trades = trades[-limit_:]
                return trades

            def fetch_status(self):
                return {"status": "ok", "updated": None}

            def calculate_fee(
                self,
                symbol,
                type,
                side,
                amount,
                price,
                takerOrMaker="taker",
                params=None,
            ):
                """Mock calculate_fee method for Drift exchange."""
                fee_rate = 0.001  # 0.1% fee
                return {
                    "type": takerOrMaker,
                    "currency": "USDC",
                    "rate": fee_rate,
                    "cost": amount * price * fee_rate if price else 0,
                }

            def fetch_ticker(self, symbol: str) -> dict[str, Any]:
                # Build a minimal ccxt-like ticker from Drift Data API contracts data.
                tickers = self._parent.get_tickers(
                    symbols=[symbol], cached=False)
                t = tickers.get(symbol, {})
                last = t.get("last")
                return {
                    "symbol": symbol,
                    "last": last,
                    "bid": t.get("bid"),
                    "ask": t.get("ask"),
                    "high": t.get("high"),
                    "low": t.get("low"),
                    "info": t.get("info"),
                }

            async def close(self):
                if self.session is not None:
                    await self.session.close()
                    self.session = None

        return DriftCCXTAdapter(self, is_async=not sync)

    def additional_exchange_init(self) -> None:
        """
        Additional initialization for Drift exchange.
        Sets up Drift client connection and validates configuration.
        """
        try:
            # Safety defaults - required by several codepaths (especially trades/orderflow cache)
            self._config.setdefault("dataformat_trades", "feather")
            self._config.setdefault("dataformat_ohlcv", "feather")

            if not self._config["dry_run"]:
                # Verify Drift-specific configuration
                if self.trading_mode == TradingMode.FUTURES:
                    # Ensure proper margin mode configuration
                    if self.margin_mode not in [MarginMode.CROSS, MarginMode.ISOLATED]:
                        raise OperationalException(
                            f"Drift only supports CROSS or ISOLATED margin modes, not {self.margin_mode}"
                        )

                # Initialize Drift protocol connection if needed
                # This would typically involve connecting to Solana RPC
                logger.info("Drift exchange initialized successfully")

        except Exception as e:
            raise OperationalException(
                f"Failed to initialize Drift exchange: {e}") from e

    @property
    def name(self) -> str:
        """Return exchange name"""
        return "drift"

    @property
    def id(self) -> str:
        """Return exchange id"""
        return "drift"

    @property
    def precisionMode(self) -> int:
        """Return precision mode for Drift"""
        return 2  # DECIMAL_PLACES

    def load_markets(self) -> dict[str, Any]:
        """Load market data for Drift.

        For VolumePairList (dynamic whitelist) to work, markets must be available even when
        `exchange.pair_whitelist` is empty. For Drift, we load *perp markets* online using the
        public Drift Data API (`/contracts`).
        """

        # Drift MUST behave like ccxt exchanges here: load all markets first.
        # Pairlists (VolumePairList, etc.) are responsible for generating the runtime whitelist.
        #
        # We therefore *prefer* online discovery (all perps) and only fall back to config pairs
        # when the Data API is unavailable (403 / rate-limit / network errors).
        contracts = self._fetch_contracts(
            for_tickers=self._requires_ticker_contracts())
        drift_markets = self._contracts_to_markets(contracts)
        self._markets = drift_markets

        if self._contracts_source == "config_fallback":
            logger.warning(
                f"Loaded {len(drift_markets)} Drift markets from config fallback (Data API unavailable)"
            )
        else:
            # NOTE: _contracts_source may be data_api, memory_cache, or disk_cache.
            # Make this explicit to avoid confusion when markets are reloaded.
            logger.info(
                f"Loaded {len(drift_markets)} Drift perp markets (source={self._contracts_source})"
            )
        return drift_markets

    def _requires_ticker_contracts(self) -> bool:
        """Return True if the current config requires tickers/quoteVolume.

        This is mainly the case for VolumePairList in ticker mode (no lookback range).
        In that scenario, we must have real contracts with 24h volume fields.

        If the config does not require tickers (e.g. StaticPairList), we can still start
        using config fallback markets even if Drift Data API is blocked.
        """
        for pl in self._config.get("pairlists", []) or []:
            if pl.get("method") != "VolumePairList":
                continue
            lookback_days = int(pl.get("lookback_days", 0) or 0)
            lookback_period = int(pl.get("lookback_period", 0) or 0)
            # VolumePairList uses tickers when not using range lookback.
            if lookback_days == 0 and lookback_period == 0:
                return True
        return False

    def _fetch_contracts(self, *, for_tickers: bool) -> list[dict[str, Any]]:
        """Fetch Drift perp contract metadata.

        **Real-data-only guarantee:**
        - When `for_tickers=True`, this must return real Drift Data API contract data
          (with volume fields) from either live API, memory cache, or disk cache.
          If none are available, this raises OperationalException.

        When `for_tickers=False` (market discovery), we allow a last-resort fallback
        to config whitelist to allow StaticPairList / manual configs to start.
        """

        self._contracts_fetch_count += 1

        # 1) In-memory cache (short TTL)
        now = datetime.now(tz=UTC)
        if (
            self._contracts_cache is not None
            and self._contracts_cache_ts is not None
            and (now - self._contracts_cache_ts) < self._CONTRACTS_MEM_CACHE_TTL
        ):
            self._contracts_source = "memory_cache"
            return self._contracts_cache

        # 2) Live API (with limited retries for real rate-limit cases)
        try:
            import time
            import requests

            last_exc: Exception | None = None
            for attempt in range(3):
                resp = requests.get(
                    self._DATA_API_CONTRACTS,
                    timeout=15,
                    headers=self._PUBLIC_HTTP_HEADERS,
                )

                # Respect true rate limiting.
                if resp.status_code == 429:
                    retry_after = resp.headers.get("Retry-After")
                    sleep_s = 5
                    if retry_after:
                        try:
                            sleep_s = max(1, min(60, int(float(retry_after))))
                        except Exception:
                            sleep_s = 5
                    logger.warning(
                        f"Drift Data API rate limited (429) on /contracts. Sleeping {sleep_s}s "
                        f"(attempt {attempt + 1}/3)."
                    )
                    time.sleep(sleep_s)
                    continue

                # 403 is usually bot/WAF mitigation - do not retry spam.
                if resp.status_code == 403:
                    raise OperationalException(
                        "Drift Data API blocked (403) on /contracts")

                resp.raise_for_status()
                data = resp.json()
                contracts = data.get("contracts")
                if not isinstance(contracts, list):
                    raise OperationalException(
                        "Unexpected Drift Data API response shape. Missing 'contracts' list. "
                        f"Keys: {list(data.keys())}"
                    )
                perps = [c for c in contracts if str(
                    c.get("product_type", "")).upper() == "PERP"]

                # Update caches (real data only)
                self._contracts_cache = perps
                self._contracts_cache_ts = now
                self._save_contracts_disk_cache(perps)

                self._contracts_source = "data_api"
                return perps

        except Exception as e:
            last_exc = e

        # 3) Disk cache (real data only)
        disk = self._load_contracts_disk_cache()
        if disk:
            self._contracts_cache = disk
            self._contracts_cache_ts = now
            self._contracts_source = "disk_cache"
            return disk

        # 4) Config fallback (markets only)
        exchange_conf = self._config.get("exchange", {})
        config_pairs = exchange_conf.get("pair_whitelist", [])
        if not for_tickers and config_pairs:
            self._contracts_source = "config_fallback"
            logger.warning(
                f"Failed to fetch Drift markets from {self._DATA_API_CONTRACTS}: {last_exc}. "
                f"Falling back to {len(config_pairs)} config pairs (markets only)."
            )
            return [
                {
                    "ticker_id": p,
                    "product_type": "PERP" if "-PERP" in p else "SPOT",
                    "base_currency": p.replace("-PERP", "").split("-")[0],
                    "quote_currency": "USDC",
                }
                for p in config_pairs
            ]

        # 5) Fail fast (tickers require real data)
        raise OperationalException(
            "Drift contracts/tickers unavailable: Drift Data API /contracts failed and no cached "
            "contracts are available. VolumePairList in ticker mode requires real 24h quoteVolume. "
            "Try again when the Data API is reachable (so cache can be created), or configure "
            "VolumePairList to use candle lookback (lookback_days/lookback_period), or use StaticPairList."
        )

    def _contracts_to_markets(self, contracts: list[dict[str, Any]]) -> dict[str, Any]:
        markets: dict[str, Any] = {}
        for c in contracts:
            symbol = str(c.get("ticker_id", "") or "").strip()
            if not symbol or not symbol.endswith("-PERP"):
                continue
            base = str(c.get("base_currency", "")
                       or "").strip() or symbol.replace("-PERP", "")
            quote = str(c.get("quote_currency", "")
                        or "USDC").strip() or "USDC"
            markets[symbol] = self._build_market_dict(
                symbol, base=base, quote=quote, info=c)
        return markets

    def _build_market_dict(self, symbol: str, *, base: str, quote: str, info: dict[str, Any]) -> dict[str, Any]:
        """Build a VulcanTrader/ccxt-like market dict for Drift perps."""
        return {
            "id": symbol,
            "symbol": symbol,
            "base": base,
            "quote": quote,
            "active": True,
            "type": "swap",
            "spot": False,
            "margin": False,
            "future": False,
            "swap": True,
            "option": False,
            "contract": True,
            "linear": True,
            "inverse": False,
            "contractSize": 1.0,
            "precision": {"amount": 5, "price": 4},
            "limits": {
                "amount": {"min": 0.001, "max": 1000000},
                "price": {"min": 0.000001, "max": 10000000},
                "cost": {"min": 1, "max": 100000000},
            },
            # Fees (placeholder - Drift has tiered maker/taker; ok for pairlist)
            "taker": 0.001,
            "maker": 0.0005,
            "info": info or {},
        }

    def reload_markets(self, force: bool = False, *, load_leverage_tiers: bool = True) -> None:
        """Reload markets for Drift.

        Important:
        - Pairlist `refresh_period` controls how often the **pairlist** recomputes the whitelist.
        - Market refresh is controlled by the core exchange setting `exchange.markets_refresh_interval`
          (minutes) which is already implemented by the base `Exchange.reload_markets()`.

        Drift's markets are derived from `_fetch_contracts()`, so we implement the same throttling
        behavior as the base class to avoid repeated `/contracts` calls.
        """

        from VulcanTrader.util.datetime_helpers import dt_ts

        is_initial = self._last_markets_refresh == 0
        if (
            not force
            and self._last_markets_refresh > 0
            and (self._last_markets_refresh + self.markets_refresh_interval > dt_ts())
        ):
            return None

        markets = self.load_markets()
        self._markets = markets
        self._last_markets_refresh = dt_ts()

    def _load_async_markets(self, reload: bool = False) -> None:
        """Load markets asynchronously - simplified for Drift"""
        if reload:
            self.load_markets()

    def validate_timeframes(self, timeframe: Optional[str]) -> None:
        """Validate timeframe for Drift exchange

        Note:
        - During exchange initialization in backtesting/optimization, config['timeframe'] can be None.
          In that case, defer validation until the strategy injects/uses its timeframe.
        """
        supported_timeframes = ['1m', '3m', '5m', '15m', '30m', '1h',
                                '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M']
        if timeframe is None:
            logger.debug(
                "No timeframe provided at initialization - deferring validation.")
            return
        if timeframe not in supported_timeframes:
            raise ValueError(
                f"Timeframe {timeframe} is not supported by Drift. Supported: {supported_timeframes}")
        logger.info(f"Timeframe {timeframe} validated for Drift")

    def ohlcv_candle_limit(
        self, timeframe: str, candle_type: CandleType, since_ms: int | None = None
    ) -> int:
        """Return the maximum number of candles that can be fetched in one request"""
        return 1000

    # NOTE: Do not override `Exchange.features(...)` with an incompatible signature.
    # Drift provides `ohlcv_candle_limit` directly, so the default features are sufficient.

    def get_orderflow_data(
        self,
        pair: str,
        timeframe: str,
        since_ms: int,
        candle_type: CandleType = CandleType.SPOT,
    ) -> DataFrame:
        """
        Fetch orderflow data from Drift protocol.

        This method fetches raw trade data and lets the standard VulcanTrader
        orderflow system (populate_dataframe_with_trades) process it into orderflow format.

        :param pair: Pair to fetch data for
        :param timeframe: Timeframe to fetch
        :param since_ms: Start timestamp in milliseconds
        :param candle_type: Type of candles (SPOT or FUTURES)
        :return: DataFrame with trades data in VulcanTrader format
        """
        try:
            # Use the standard get_historic_trades method to fetch trade data
            # This ensures compatibility with the orderflow system
            last_trade_id, trades_list = self.get_historic_trades(
                pair=pair,
                since=since_ms,
                until=None  # Use current time as end
            )

            if not trades_list:
                logger.warning(f"No orderflow data available for {pair}")
                return DataFrame(columns=DEFAULT_TRADES_COLUMNS + ['date'])

            # Convert trades list to DataFrame in VulcanTrader format
            from VulcanTrader.data.converter import trades_list_to_df
            trades_df = trades_list_to_df(trades_list)

            logger.info(
                f"Fetched {len(trades_df)} trades for {pair} orderflow analysis")
            return trades_df

        except Exception as e:
            logger.error(f"Error fetching orderflow data for {pair}: {e}")
            raise TemporaryError(f"Could not fetch orderflow data: {e}") from e

    def _convert_pair_to_drift_market(self, pair: str, candle_type: CandleType) -> str:
        """
        Convert VulcanTrader pair format to Drift market format.

        :param pair: Pair in format "BTC/USDC" or "SOL-PERP"
        :param candle_type: Type of market (SPOT or FUTURES)
        :return: Drift market identifier (e.g., "BTC-PERP")
        """
        # Handle pairs already in Drift format (e.g., "SOL-PERP")
        if "-PERP" in pair:
            return pair

        # Handle pairs in standard format (e.g., "BTC/USDC")
        if "/" in pair:
            base, quote = pair.split("/")
            if candle_type in [CandleType.FUTURES, CandleType.MARK, CandleType.INDEX]:
                # For futures, use PERP suffix
                return f"{base}-PERP"
            else:
                # For spot, use the pair directly
                return f"{base}-{quote}"
        else:
            # Assume it's already in the right format
            return pair

    def _download_drift_trades(
        self,
        market: str,
        start_date: datetime,
        end_date: datetime
    ) -> pd.DataFrame:
        """
        Download raw trade data from Drift protocol using async downloader.

        :param market: Drift market identifier
        :param start_date: Start date for data
        :param end_date: End date for data
        :return: DataFrame with raw trade data
        """
        # Check if trades feather file already exists
        try:
            from VulcanTrader.data.history.datahandlers.featherdatahandler import FeatherDataHandler

            # Convert market to pair format for file checking
            if market.endswith('-PERP'):
                pair = market  # Keep as is for futures
                candle_type = CandleType.FUTURES
                trading_mode = TradingMode.FUTURES
            else:
                # Convert to standard pair format
                pair = market.replace('-', '/')
                candle_type = CandleType.SPOT
                trading_mode = TradingMode.SPOT

            # Create FeatherDataHandler instance to check for existing file
            datadir_config = self._config.get(
                'datadir', 'user_data/data/drift')
            data_dir = Path(datadir_config)
            data_handler = FeatherDataHandler(data_dir)

            # Check if trades file already exists
            existing_trades = data_handler.trades_load(pair, trading_mode)
            if not existing_trades.empty:
                logger.info(
                    f"Trades feather file already exists for {pair} with {len(existing_trades):,} trades - skipping download")
                return existing_trades

        except Exception as e:
            logger.warning(f"Could not check for existing trades file: {e}")
            # Continue with download if check fails

        # Base URL for Drift historical data
        base_url = "https://data.api.drift.trade"

        # Generate date range
        dates = []
        current_date = start_date
        while current_date <= end_date:
            dates.append(current_date)
            current_date += timedelta(days=1)

        if not dates:
            logger.warning("No dates to process")
            return pd.DataFrame()

        logger.info(
            f"Downloading Drift data for {market} from {start_date.date()} to {end_date.date()}")
        logger.info(f"Total days to download: {len(dates)}")

        # Run async download
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # If loop is running, create new task
                task = asyncio.create_task(
                    self._async_download_drift_data(base_url, market, dates))
                result_df = asyncio.run_coroutine_threadsafe(
                    task, loop).result()
            else:
                # If no loop running, run directly
                result_df = loop.run_until_complete(
                    self._async_download_drift_data(base_url, market, dates))
        except RuntimeError:
            # No event loop exists, create new one
            result_df = asyncio.run(
                self._async_download_drift_data(base_url, market, dates))

        # Save the combined trades data to file
        if not result_df.empty:
            # Determine the pair and candle type for saving
            # Convert market back to pair format for saving
            if market.endswith('-PERP'):
                pair = market  # Keep as is for futures
                candle_type = CandleType.FUTURES
            else:
                # Convert to standard pair format
                pair = market.replace('-', '/')
                candle_type = CandleType.SPOT

            logger.info(
                f"Starting save process: {len(result_df):,} trades to file for {pair}")
            self._save_trades_data(result_df, pair, candle_type)
            logger.info(f"Completed save process for {pair}")
        else:
            logger.warning("No data to save - result DataFrame is empty")

        return result_df

    def _save_trades_data(self, trades_df: pd.DataFrame, pair: str, candle_type: CandleType) -> None:
        """
        Save raw trades data using FeatherDataHandler.
        Uses either chunked processing (memory-efficient) or simple processing based on configuration.

        :param trades_df: Raw trades DataFrame from Drift
        :param pair: Trading pair
        :param candle_type: Type of candle data
        """
        try:
            # Import here to avoid circular import
            from VulcanTrader.data.history.datahandlers.featherdatahandler import FeatherDataHandler

            if trades_df.empty:
                logger.warning(f"No trades data to save for {pair}")
                return

            # Determine trading mode
            trading_mode = TradingMode.FUTURES if candle_type == CandleType.FUTURES else TradingMode.SPOT

            # Create FeatherDataHandler instance
            datadir_config = self._config.get(
                'datadir', 'user_data/data/drift')
            data_dir = Path(datadir_config)
            data_handler = FeatherDataHandler(data_dir)

            total_rows = len(trades_df)

            if self.USE_CHUNKED_PROCESSING:
                logger.info(
                    f"Using chunked processing for {total_rows:,} trades for {pair}")
                self._save_trades_data_chunked(
                    trades_df, pair, candle_type, data_handler, trading_mode, data_dir)
            else:
                logger.info(
                    f"Using simple in-memory processing for {total_rows:,} trades for {pair}")
                self._save_trades_data_simple(
                    trades_df, pair, candle_type, data_handler, trading_mode)

        except Exception as e:
            logger.error(f"Error saving trades data for {pair}: {e}")
            raise

    def _save_trades_data_simple(self, trades_df: pd.DataFrame, pair: str, candle_type: CandleType,
                                 data_handler, trading_mode) -> None:
        """
        Simple in-memory processing - faster but uses more memory.

        :param trades_df: Raw trades DataFrame from Drift
        :param pair: Trading pair
        :param candle_type: Type of candle data
        :param data_handler: FeatherDataHandler instance
        :param trading_mode: Trading mode (SPOT/FUTURES)
        """
        try:
            # Format all trades at once
            logger.info(
                f"Formatting {len(trades_df):,} trades in memory for {pair}")
            formatted_trades_df = self._format_trades_chunk(trades_df)

            if not formatted_trades_df.empty:
                logger.info(
                    f"Saving {len(formatted_trades_df):,} formatted trades for {pair}")
                self._append_to_feather_file(
                    data_handler, pair, formatted_trades_df, trading_mode)
                logger.info(
                    f"Successfully saved {len(formatted_trades_df):,} trades for {pair}")
            else:
                logger.warning(f"No valid trades after formatting for {pair}")

        except Exception as e:
            logger.error(f"Error in simple processing for {pair}: {e}")
            raise

    def _save_trades_data_chunked(self, trades_df: pd.DataFrame, pair: str, candle_type: CandleType,
                                  data_handler, trading_mode, data_dir: Path) -> None:
        """
        Memory-efficient chunked processing using temporary files.

        :param trades_df: Raw trades DataFrame from Drift
        :param pair: Trading pair
        :param candle_type: Type of candle data
        :param data_handler: FeatherDataHandler instance
        :param trading_mode: Trading mode (SPOT/FUTURES)
        :param data_dir: Data directory path
        """
        try:
            # Create processing directory for intermediate operations
            processing_dir = data_dir / "temp" / "processing"
            processing_dir.mkdir(parents=True, exist_ok=True)

            # Process data in chunks to avoid memory issues
            chunk_size = 5000  # Reduced from 10k to 5k trades at a time for better memory management
            total_rows = len(trades_df)

            if total_rows <= chunk_size:
                # Small dataset - process normally even in chunked mode
                formatted_trades_df = self._format_trades_chunk(trades_df)
                if not formatted_trades_df.empty:
                    self._append_to_feather_file(
                        data_handler, pair, formatted_trades_df, trading_mode)
                    logger.info(
                        f"Saved {len(formatted_trades_df)} trades for {pair} to feather file")
            else:
                # Large dataset - process in chunks using processing directory
                logger.info(
                    f"Processing {total_rows} trades for {pair} in chunks of {chunk_size} using processing directory")

                total_saved = 0
                chunk_files = []  # Track temporary chunk files

                # Process and save chunks to processing directory first
                for start_idx in range(0, total_rows, chunk_size):
                    end_idx = min(start_idx + chunk_size, total_rows)
                    chunk_df = trades_df.iloc[start_idx:end_idx].copy()

                    # Format chunk
                    formatted_chunk = self._format_trades_chunk(chunk_df)

                    if not formatted_chunk.empty:
                        # Save chunk to processing directory as intermediate file
                        chunk_filename = f"{pair.replace('/', '_')}_{start_idx}_{end_idx}.feather"
                        chunk_path = processing_dir / chunk_filename

                        try:
                            formatted_chunk.to_feather(chunk_path)
                            chunk_files.append(chunk_path)
                            total_saved += len(formatted_chunk)

                            # Log progress
                            progress = (end_idx / total_rows) * 100
                            logger.info(f"Processed chunk {start_idx:,}-{end_idx:,} ({progress:.1f}%) - "
                                        f"{len(formatted_chunk)} trades saved to processing")
                        except Exception as chunk_error:
                            logger.warning(
                                f"Failed to save chunk {start_idx}-{end_idx}: {chunk_error}")

                    # Clear chunk from memory immediately
                    del chunk_df, formatted_chunk
                    gc.collect()  # Force garbage collection after each chunk

                # Now combine all chunk files and save to final feather file
                if chunk_files:
                    logger.info(
                        f"Combining {len(chunk_files)} chunk files into final feather file...")

                    # Process chunks in smaller batches to avoid memory exhaustion
                    chunk_batch_size = 10  # Process only 10 chunk files at a time
                    total_chunk_batches = (
                        len(chunk_files) + chunk_batch_size - 1) // chunk_batch_size

                    # First, save data directly to feather file without keeping in memory
                    first_batch = True
                    total_trades_processed = 0

                    for batch_idx in range(0, len(chunk_files), chunk_batch_size):
                        batch_end = min(
                            batch_idx + chunk_batch_size, len(chunk_files))
                        batch_chunk_files = chunk_files[batch_idx:batch_end]

                        logger.info(
                            f"Processing chunk batch {batch_idx//chunk_batch_size + 1}/{total_chunk_batches} ({len(batch_chunk_files)} files)...")

                        # Read and combine this batch of chunks
                        batch_combined_chunks = []
                        for chunk_path in batch_chunk_files:
                            try:
                                if not chunk_path.exists():
                                    logger.warning(
                                        f"Chunk file does not exist: {chunk_path}")
                                    continue

                                chunk_data = pd.read_feather(chunk_path)
                                if chunk_data.empty:
                                    logger.debug(
                                        f"Empty chunk file: {chunk_path}")
                                    del chunk_data
                                    continue

                                logger.debug(
                                    f"Read chunk {chunk_path}: {len(chunk_data)} rows")
                                batch_combined_chunks.append(chunk_data)
                                del chunk_data
                            except Exception as read_error:
                                logger.warning(
                                    f"Failed to read chunk {chunk_path}: {read_error}")
                                # Try to remove corrupted chunk file
                                try:
                                    chunk_path.unlink(missing_ok=True)
                                except Exception:
                                    pass

                        if batch_combined_chunks:
                            # Combine this batch
                            try:
                                batch_df = pd.concat(
                                    batch_combined_chunks, ignore_index=True)
                                del batch_combined_chunks
                                gc.collect()

                                # Validate the batch DataFrame
                                if batch_df.empty:
                                    logger.warning(
                                        f"Batch DataFrame is empty for batch {batch_idx//chunk_batch_size + 1}")
                                    del batch_df
                                    continue

                                batch_rows = len(batch_df)
                                total_trades_processed += batch_rows
                                logger.info(
                                    f"Batch {batch_idx//chunk_batch_size + 1} combined: {batch_rows} trades")

                                # Save this batch directly to feather file
                                if first_batch:
                                    # For first batch, create new file
                                    self._append_to_feather_file(
                                        data_handler, pair, batch_df, trading_mode)
                                    first_batch = False
                                else:
                                    # For subsequent batches, append to existing file
                                    self._append_to_feather_file(
                                        data_handler, pair, batch_df, trading_mode)

                                del batch_df
                                gc.collect()

                            except Exception as batch_error:
                                logger.error(
                                    f"Failed to process chunk batch {batch_idx//chunk_batch_size + 1}: {batch_error}")
                                if 'batch_combined_chunks' in locals():
                                    del batch_combined_chunks
                                gc.collect()
                                continue

                    if total_trades_processed > 0:
                        logger.info(
                            f"Successfully saved {total_trades_processed} trades for {pair} to feather file")
                    else:
                        logger.warning(
                            f"No trades were successfully processed for {pair}")

                    # Clean up temporary chunk files from processing directory
                    for chunk_path in chunk_files:
                        try:
                            chunk_path.unlink(missing_ok=True)
                        except Exception:
                            pass

                # Final cleanup: remove any remaining files in processing directory for this pair
                try:
                    pair_prefix = pair.replace('/', '_')
                    for leftover_file in processing_dir.glob(f"{pair_prefix}_*.feather"):
                        try:
                            leftover_file.unlink()
                            logger.debug(
                                f"Cleaned up leftover processing file: {leftover_file}")
                        except Exception:
                            pass
                except Exception:
                    pass

                logger.info(
                    f"Completed saving {total_saved:,} trades for {pair} to feather file")

        except Exception as e:
            logger.error(f"Error in chunked processing for {pair}: {e}")
            raise

    def _format_trades_chunk(self, trades_chunk: pd.DataFrame) -> pd.DataFrame:
        """
        Format a chunk of Drift trades data into VulcanTrader format efficiently.

        :param trades_chunk: Chunk of raw Drift trades data
        :return: Formatted DataFrame ready for FeatherDataHandler
        """
        try:
            if trades_chunk.empty:
                logger.debug("Empty trades chunk received")
                return pd.DataFrame()

            logger.debug(f"Formatting {len(trades_chunk)} trades")
            logger.debug(f"Available columns: {list(trades_chunk.columns)}")

            # DEBUG: Check what's in the timestamp columns
            if 'ts' in trades_chunk.columns:
                logger.debug(
                    f"TS column sample values: {trades_chunk['ts'].head().tolist()}")
            if 'timestamp' in trades_chunk.columns:
                logger.debug(
                    f"Timestamp column sample values: {trades_chunk['timestamp'].head().tolist()}")
            if 'blockTime' in trades_chunk.columns:
                logger.debug(
                    f"BlockTime column sample values: {trades_chunk['blockTime'].head().tolist()}")
            if 'date' in trades_chunk.columns:
                logger.debug(
                    f"Date column sample values: {trades_chunk['date'].head().tolist()}")

            # Check a sample trade
            if len(trades_chunk) > 0:
                sample_row = trades_chunk.iloc[0]
                logger.debug(
                    f"Sample raw data - oraclePrice: {sample_row.get('oraclePrice')}, baseAssetAmountFilled: {sample_row.get('baseAssetAmountFilled')}, takerOrderDirection: {sample_row.get('takerOrderDirection')}")

            # Apply Drift precision conversion
            # Convert oraclePrice from Drift precision to standard price
            oracle_price_raw = trades_chunk.get('oraclePrice', 0).astype(float)

            # Check if oraclePrice is already in realistic format (like real BTC prices)
            if len(oracle_price_raw) > 0:
                sample_price = oracle_price_raw.iloc[0]
                if sample_price > 1000:  # Already realistic pricing (> $1000)
                    print(
                        f"DRIFT DEBUG: _format_trades_chunk - oraclePrice already realistic (${sample_price:,.2f}), using as-is")
                    price_column = oracle_price_raw
                else:
                    print(
                        f"DRIFT DEBUG: _format_trades_chunk - applying precision conversion to oraclePrice ({sample_price})")
                    price_column = oracle_price_raw / self.PRICE_PRECISION
                    print(
                        f"DRIFT DEBUG: _format_trades_chunk - converted price: {price_column.iloc[0] if len(price_column) > 0 else 'N/A'}")
            else:
                price_column = oracle_price_raw / self.PRICE_PRECISION

            # The baseAssetAmountFilled and quoteAssetAmountFilled appear to already be in normal units
            # Don't divide by precision constants since they're already converted
            amount_column = trades_chunk.get(
                'baseAssetAmountFilled', 0).astype(float)
            cost_column = trades_chunk.get(
                'quoteAssetAmountFilled', 0).astype(float)

            logger.debug(
                f"Sample converted values - price: {price_column.iloc[0] if len(price_column) > 0 else 'N/A'}, amount: {amount_column.iloc[0] if len(amount_column) > 0 else 'N/A'}")

            # Vectorized conversion for better performance
            # FIXED: Handle timestamp properly - use actual timestamp columns or create proper datetime index
            timestamp_source = None
            if 'blockTime' in trades_chunk.columns and trades_chunk['blockTime'].notna().any():
                # blockTime is usually the most reliable timestamp in Drift data
                timestamp_source = pd.to_datetime(
                    trades_chunk['blockTime']).astype(int) // 10**9
                logger.debug("Using blockTime as timestamp source")
            elif 'timestamp' in trades_chunk.columns and trades_chunk['timestamp'].notna().any():
                # Use timestamp column if available
                timestamp_source = pd.to_datetime(
                    trades_chunk['timestamp']).astype(int) // 10**9
                logger.debug("Using timestamp column as timestamp source")
            elif 'ts' in trades_chunk.columns and trades_chunk['ts'].notna().any():
                # Check if ts contains actual timestamps
                ts_values = trades_chunk['ts'].astype(float)
                if ts_values.max() > 1e12:  # Milliseconds (> year 2001)
                    # Convert ms to seconds
                    timestamp_source = (ts_values / 1000).astype(int)
                    logger.debug(
                        "Using ts column as timestamp source (converted from ms to seconds)")
                elif ts_values.max() > 1e9:  # Seconds (> year 2001)
                    timestamp_source = ts_values.astype(
                        int)  # Use as-is (seconds)
                    logger.debug(
                        "Using ts column as timestamp source (already in seconds)")
                else:
                    logger.warning(
                        f"TS column contains sequential numbers, not timestamps: {ts_values.head().tolist()}")
                    # Create synthetic timestamps based on row position and current time
                    base_timestamp = int(
                        datetime.now().timestamp()) - len(trades_chunk)
                    timestamp_source = pd.Series(
                        range(base_timestamp, base_timestamp + len(trades_chunk)))
                    logger.debug(
                        "Created synthetic timestamps for sequential data")
            else:
                logger.warning(
                    "No valid timestamp column found, creating synthetic timestamps")
                # Create synthetic timestamps
                base_timestamp = int(
                    datetime.now().timestamp()) - len(trades_chunk)
                timestamp_source = pd.Series(
                    range(base_timestamp, base_timestamp + len(trades_chunk)))

            formatted_data = {
                'timestamp': timestamp_source,
                'id': trades_chunk.get('fillRecordId', '').astype(str),
                'type': 'limit',  # Drift trades are typically limit orders
                'side': trades_chunk.get('takerOrderDirection', 'long').map({
                    'long': 'buy',
                    'short': 'sell'
                }).fillna('buy'),
                'price': price_column,
                'amount': amount_column,
                'cost': cost_column,
                'date': pd.to_datetime(timestamp_source, unit='s', utc=True) if timestamp_source is not None else None
            }

            # Create DataFrame efficiently
            formatted_df = pd.DataFrame(formatted_data)
            logger.debug(
                f"Created formatted DataFrame with {len(formatted_df)} rows")

            # Remove any rows with invalid data
            formatted_df = formatted_df.dropna(
                subset=['timestamp', 'price', 'amount'])
            logger.debug(f"After dropna: {len(formatted_df)} rows")

            formatted_df = formatted_df[formatted_df['timestamp'] > 0]
            logger.debug(f"After timestamp filter: {len(formatted_df)} rows")

            formatted_df = formatted_df[formatted_df['price'] > 0]
            logger.debug(f"After price filter: {len(formatted_df)} rows")

            formatted_df = formatted_df[formatted_df['amount'] > 0]
            logger.debug(f"After amount filter: {len(formatted_df)} rows")

            logger.debug(
                f"Formatted {len(formatted_df)} valid trades from {len(trades_chunk)} raw trades")
            return formatted_df

        except Exception as e:
            logger.error(f"Error formatting trades chunk: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    def _append_to_feather_file(self, data_handler, pair: str, trades_df: pd.DataFrame, trading_mode) -> None:
        """
        Append trades data to existing feather file or create new one.
        Handles corrupted files by recreating them.

        :param data_handler: FeatherDataHandler instance
        :param pair: Trading pair
        :param trades_df: Formatted trades DataFrame
        :param trading_mode: Trading mode (SPOT/FUTURES)
        """
        try:
            # Try to load existing data first
            existing_df = None
            try:
                existing_df = data_handler.trades_load(pair, trading_mode)
                if existing_df is not None and not existing_df.empty:
                    logger.debug(
                        f"Found existing trades data: {len(existing_df)} trades for {pair}")
                else:
                    logger.debug(f"No existing trades data found for {pair}")
                    existing_df = None
            except Exception as load_error:
                logger.warning(
                    f"Failed to load existing trades data for {pair}: {load_error}")
                logger.info(f"Will create new trades file for {pair}")
                existing_df = None

                # Try to remove corrupted file
                try:
                    import os
                    datadir_config = self._config.get(
                        'datadir', 'user_data/data/drift')
                    data_dir = Path(datadir_config)
                    trading_mode_str = trading_mode.value if hasattr(
                        trading_mode, 'value') else str(trading_mode).lower()
                    corrupted_file = data_dir / \
                        trading_mode_str / f"{pair}-trades.feather"
                    if corrupted_file.exists():
                        logger.warning(
                            f"Removing corrupted feather file: {corrupted_file}")
                        corrupted_file.unlink()
                except Exception as remove_error:
                    logger.warning(
                        f"Failed to remove corrupted file: {remove_error}")

            if existing_df is not None and not existing_df.empty:
                # Combine with existing data, removing duplicates
                logger.debug(
                    f"Combining {len(trades_df)} new trades with {len(existing_df)} existing trades")
                combined_df = pd.concat(
                    [existing_df, trades_df], ignore_index=True)

                # Remove duplicates based on timestamp and id
                initial_len = len(combined_df)
                if 'id' in combined_df.columns:
                    combined_df = combined_df.drop_duplicates(
                        subset=['timestamp', 'id'], keep='last')
                else:
                    combined_df = combined_df.drop_duplicates(
                        subset=['timestamp'], keep='last')

                duplicates_removed = initial_len - len(combined_df)
                if duplicates_removed > 0:
                    logger.debug(
                        f"Removed {duplicates_removed} duplicate trades")

                # Sort by timestamp
                combined_df = combined_df.sort_values(
                    'timestamp').reset_index(drop=True)

                # Save the combined data
                logger.info(
                    f"Saving combined data: {len(combined_df)} trades to feather file for {pair}")
                data_handler._trades_store(pair, combined_df, trading_mode)
                logger.info(f"Successfully saved combined data for {pair}")
                logger.debug(
                    f"Appended {len(trades_df)} trades to existing {len(existing_df)} trades for {pair}")
            else:
                # No existing data or failed to load, save new data
                logger.info(
                    f"Creating new trades file with {len(trades_df)} trades for {pair}")
                data_handler._trades_store(pair, trades_df, trading_mode)
                logger.info(f"Successfully created new trades file for {pair}")

        except Exception as e:
            logger.error(f"Error appending to feather file for {pair}: {e}")
            # Try to save as new file ignoring existing data
            try:
                logger.warning(
                    f"Attempting to save {pair} trades as new file, ignoring existing data")
                data_handler._trades_store(pair, trades_df, trading_mode)
                logger.info(f"Successfully saved {pair} trades as new file")
            except Exception as fallback_error:
                logger.error(
                    f"Failed to save trades even as new file for {pair}: {fallback_error}")
                raise

    async def _async_download_drift_data(
        self,
        base_url: str,
        market: str,
        dates: List[datetime]
    ) -> pd.DataFrame:
        """
        Memory-efficient async function to download Drift data using chunked processing.

        :param base_url: Base URL for Drift API
        :param market: Market identifier
        :param dates: List of dates to download
        :return: Combined DataFrame
        """
        # Use very conservative settings for Drift API to minimize memory usage
        # Limit to 1 concurrent request to minimize memory pressure
        max_concurrent = 1  # Further reduced from 2 to 1

        # Get data directory from config or use default
        datadir_config = self._config.get('datadir', 'user_data/data/drift')
        data_dir = Path(datadir_config)

        async with AsyncHTTPDownloader(
            max_concurrent=max_concurrent,
            retry_delay=20,
            max_retries=3,
            request_timeout=30,
            data_dir=str(data_dir)
        ) as downloader:
            temp_files, successful_days, failed_days, empty_days = await downloader.download_date_range(
                base_url, market, dates
            )

            logger.info(
                f"Download complete: {successful_days} successful, {empty_days} empty, {failed_days} failed")

            if not temp_files:
                logger.warning("No data was successfully downloaded")
                return pd.DataFrame()

            logger.info(
                f"Processing {len(temp_files)} temporary data files...")

            # Choose processing method based on configuration
            if self.USE_CHUNKED_PROCESSING:
                logger.info("Using chunked temp file processing")
                return await self._process_temp_files_chunked(temp_files)
            else:
                logger.info("Using simple temp file processing")
                return await self._process_temp_files_simple(temp_files)

    async def _process_temp_files_chunked(self, temp_files: list) -> pd.DataFrame:
        """
        Process temporary files in truly memory-efficient chunks using file-based streaming.
        Never holds more than one day's worth of data in memory at a time.
        Uses pandas only for consistency and simplicity.

        :param temp_files: List of temporary file paths
        :return: Combined DataFrame
        """
        try:
            import os

            if not temp_files:
                logger.warning("No temporary files to process")
                return pd.DataFrame()

            # Create processing directory and use it for temporary files
            datadir_config = self._config.get(
                'datadir', 'user_data/data/drift')
            processing_dir = Path(datadir_config) / "temp" / "processing"
            processing_dir.mkdir(parents=True, exist_ok=True)

            total_files = len(temp_files)
            total_rows_processed = 0
            files_processed = 0

            logger.info(
                f"Processing {total_files} temporary files with pandas-based streaming...")

            # Stream through files one at a time, never accumulating in memory
            combined_parts = []  # Store file paths instead of data

            for i, temp_file in enumerate(temp_files):
                try:
                    if not os.path.exists(temp_file):
                        logger.warning(
                            f"Temp file {temp_file} doesn't exist, skipping")
                        continue

                    # Read one file at a time with pandas
                    df = pd.read_parquet(temp_file)

                    if len(df) == 0:
                        logger.debug(f"Empty file {temp_file}, skipping")
                        del df
                        continue

                    file_rows = len(df)
                    total_rows_processed += file_rows

                    # For large individual files, split them first
                    if file_rows > 10000:  # Reduced threshold
                        logger.debug(
                            f"Large file detected ({file_rows} rows), splitting into chunks")

                        chunk_size = 5000  # Smaller chunks to reduce memory pressure
                        for j in range(0, file_rows, chunk_size):
                            chunk_end = min(j + chunk_size, file_rows)
                            chunk = df.iloc[j:chunk_end].copy()

                            # Write chunk to processing directory
                            chunk_filename = f'drift_chunk_{i}_{j//chunk_size}_{uuid.uuid4().hex[:8]}.parquet'
                            chunk_path = processing_dir / chunk_filename

                            chunk.to_parquet(chunk_path, index=False)
                            combined_parts.append(chunk_path)

                            del chunk
                            gc.collect()
                    else:
                        # Small file - add to parts list directly
                        combined_parts.append(temp_file)

                    # Clean up current file data from memory immediately
                    del df
                    gc.collect()

                    files_processed += 1
                    progress = (files_processed / total_files) * 100
                    logger.info(f"Streamed file {files_processed}/{total_files} ({progress:.1f}%) - "
                                f"{file_rows:,} rows, {total_rows_processed:,} total")

                except Exception as e:
                    logger.warning(
                        f"Failed to process temp file {temp_file}: {e}")
                    continue

            if not combined_parts:
                logger.warning("No valid data found in temporary files")
                return pd.DataFrame()

            # Use batch-wise combination to avoid memory exhaustion with many files
            batch_size = 50  # Combine files in batches of 50
            total_batches = (len(combined_parts) +
                             batch_size - 1) // batch_size

            logger.info(
                f"Combining {len(combined_parts)} data parts in {total_batches} batches of {batch_size}...")

            batch_results = []

            try:
                # Process files in batches to avoid memory issues
                for batch_idx in range(0, len(combined_parts), batch_size):
                    batch_end = min(batch_idx + batch_size,
                                    len(combined_parts))
                    batch_parts = combined_parts[batch_idx:batch_end]

                    logger.info(
                        f"Processing batch {batch_idx//batch_size + 1}/{total_batches} ({len(batch_parts)} files)...")

                    try:
                        # Combine this batch using pandas
                        batch_frames = []
                        for part_path in batch_parts:
                            try:
                                df = pd.read_parquet(part_path)
                                if len(df) > 0:
                                    batch_frames.append(df)
                                del df
                            except Exception as e:
                                logger.warning(
                                    f"Failed to read part {part_path}: {e}")
                                continue

                        if batch_frames:
                            # Combine this batch
                            batch_combined = pd.concat(
                                batch_frames, ignore_index=True)

                            # Write batch result to processing directory
                            batch_filename = f'drift_batch_{batch_idx//batch_size}_{uuid.uuid4().hex[:8]}.parquet'
                            batch_temp_path = processing_dir / batch_filename

                            batch_combined.to_parquet(
                                batch_temp_path, index=False)
                            batch_results.append(batch_temp_path)

                            logger.info(
                                f"Batch {batch_idx//batch_size + 1} combined: {len(batch_combined)} rows")

                            # Clean up batch frames from memory
                            del batch_frames, batch_combined
                            gc.collect()

                    except Exception as batch_error:
                        logger.error(
                            f"Failed to process batch {batch_idx//batch_size + 1}: {batch_error}")
                        continue

                # Now combine all batch results
                if not batch_results:
                    logger.error("No batches were successfully processed")
                    return pd.DataFrame()

                logger.info(
                    f"Combining {len(batch_results)} batch results into final DataFrame...")

                # Combine batch results (much fewer files now)
                if len(batch_results) == 1:
                    # Only one batch, read it directly
                    final_df = pd.read_parquet(batch_results[0])
                else:
                    # Multiple batches, combine them in smaller groups to avoid memory issues
                    final_batch_frames = []
                    max_batches_in_memory = 5  # Only keep 5 batches in memory at once

                    for i in range(0, len(batch_results), max_batches_in_memory):
                        batch_group_end = min(
                            i + max_batches_in_memory, len(batch_results))
                        batch_group = batch_results[i:batch_group_end]

                        group_frames = []
                        for batch_path in batch_group:
                            try:
                                batch_df = pd.read_parquet(batch_path)
                                group_frames.append(batch_df)
                                del batch_df
                            except Exception as e:
                                logger.warning(
                                    f"Failed to read batch result {batch_path}: {e}")
                                continue

                        if group_frames:
                            group_combined = pd.concat(
                                group_frames, ignore_index=True)
                            final_batch_frames.append(group_combined)
                            del group_frames, group_combined
                            gc.collect()

                    if final_batch_frames:
                        final_df = pd.concat(
                            final_batch_frames, ignore_index=True)
                        del final_batch_frames
                        gc.collect()
                    else:
                        logger.error("Failed to read any batch results")
                        return pd.DataFrame()

                logger.info(
                    f"Successfully combined {len(final_df)} rows from {len(combined_parts)} parts")

                # Clean up batch result files
                for batch_path in batch_results:
                    try:
                        # Remove temporary batch files
                        batch_path.unlink(missing_ok=True)
                    except:
                        pass

            except Exception as e:
                logger.error(f"Failed batch combination: {e}")
                # Final fallback: sequential file-by-file combination
                logger.info(
                    "Falling back to sequential file-by-file combination...")

                final_df = None
                files_combined = 0

                for i, part_path in enumerate(combined_parts):
                    try:
                        part_df = pd.read_parquet(part_path)

                        if len(part_df) == 0:
                            del part_df
                            continue

                        if final_df is None:
                            final_df = part_df.copy()
                        else:
                            final_df = pd.concat(
                                [final_df, part_df], ignore_index=True)

                        files_combined += 1
                        del part_df
                        gc.collect()

                        # Log progress every 50 files
                        if (i + 1) % 50 == 0:
                            progress_pct = (
                                (i + 1) / len(combined_parts)) * 100
                            logger.info(
                                f"Sequential progress: {progress_pct:.1f}% ({files_combined} files combined)")

                    except Exception as part_error:
                        logger.warning(
                            f"Failed to process part {part_path}: {part_error}")
                        continue

                if final_df is None:
                    logger.error(
                        "Failed to combine any parts in fallback mode")
                    return pd.DataFrame()

                logger.info(
                    f"Sequential fallback completed: {files_combined} files combined")

            # Clean up temporary chunk files (but preserve original temp files)
            for part_path in combined_parts:
                if part_path not in temp_files:  # Only delete our created chunks
                    try:
                        part_path.unlink(missing_ok=True)
                    except:
                        pass

            # Final DataFrame is already in pandas format
            logger.info(
                f"Final pandas DataFrame ready: {len(final_df):,} rows with {len(final_df.columns)} columns")
            return final_df

        except Exception as e:
            logger.error(f"Error in chunked file processing: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return pd.DataFrame()

    async def _process_temp_files_simple(self, temp_files: list) -> pd.DataFrame:
        """
        Simple in-memory processing of temporary files.
        Faster than chunked processing but uses more memory.

        :param temp_files: List of temporary file paths
        :return: Combined DataFrame
        """
        try:
            import os

            if not temp_files:
                logger.warning("No temporary files to process")
                return pd.DataFrame()

            logger.info(
                f"Processing {len(temp_files)} temporary files with simple in-memory processing...")

            # Read all files into memory and combine
            all_dataframes = []
            total_rows_processed = 0

            for i, temp_file in enumerate(temp_files):
                try:
                    if not os.path.exists(temp_file):
                        logger.warning(
                            f"Temp file {temp_file} doesn't exist, skipping")
                        continue

                    # Read file directly into memory
                    df = pd.read_parquet(temp_file)

                    if len(df) == 0:
                        logger.debug(f"Empty file {temp_file}, skipping")
                        del df
                        continue

                    file_rows = len(df)
                    total_rows_processed += file_rows
                    all_dataframes.append(df)

                    progress = ((i + 1) / len(temp_files)) * 100
                    logger.info(f"Loaded file {i + 1}/{len(temp_files)} ({progress:.1f}%) - "
                                f"{file_rows:,} rows, {total_rows_processed:,} total")

                except Exception as e:
                    logger.warning(
                        f"Failed to process temp file {temp_file}: {e}")
                    continue

            if not all_dataframes:
                logger.warning("No valid data found in temporary files")
                return pd.DataFrame()

            # Combine all DataFrames at once
            logger.info(
                f"Combining {len(all_dataframes)} DataFrames with {total_rows_processed:,} total rows...")
            final_df = pd.concat(all_dataframes, ignore_index=True)

            # Clean up individual DataFrames from memory
            del all_dataframes
            gc.collect()

            logger.info(
                f"Simple processing complete: {len(final_df):,} rows with {len(final_df.columns)} columns")
            return final_df

        except Exception as e:
            logger.error(f"Error in simple file processing: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return pd.DataFrame()

    def _process_drift_orderflow(self, trades_df: pd.DataFrame, timeframe: str) -> DataFrame:
        """
        Process raw Drift trades into orderflow format compatible with VulcanTrader.
        Based on the implementation from DRIFT.txt.
        Uses pandas only for consistency and simplicity.

        :param trades_df: Raw trades data
        :param timeframe: Target timeframe
        :return: Processed orderflow DataFrame
        """
        if trades_df.empty:
            return DataFrame()

        # Work with pandas directly
        df = trades_df.copy()

        # Convert numeric columns from strings if needed
        numeric_columns = [
            'ts', 'slot', 'fillerReward', 'baseAssetAmountFilled', 'quoteAssetAmountFilled',
            'takerFee', 'makerRebate', 'referrerReward', 'quoteAssetAmountSurplus',
            'takerOrderBaseAssetAmount', 'takerOrderCumulativeBaseAssetAmountFilled',
            'takerOrderCumulativeQuoteAssetAmountFilled', 'makerOrderBaseAssetAmount',
            'makerOrderCumulativeBaseAssetAmountFilled', 'makerOrderCumulativeQuoteAssetAmountFilled',
            'oraclePrice', 'makerFee', 'marketIndex', 'spotFulfillmentMethodFee'
        ]

        for col in numeric_columns:
            if col in df.columns:
                try:
                    if col in ['ts', 'slot']:
                        df[col] = pd.to_numeric(
                            df[col], errors='coerce').astype('Int64')
                    else:
                        df[col] = pd.to_numeric(
                            df[col], errors='coerce').astype('float64')
                except Exception as e:
                    logger.warning(f"Could not convert column {col}: {e}")

        # Handle timestamp conversion
        timestamp_col = None
        possible_ts_cols = ['timestamp_minute', 'ts',
                            'timestamp', 'time', 'blockTime', 'txTime']

        for col in possible_ts_cols:
            if col in df.columns:
                timestamp_col = col
                break

        if timestamp_col is None:
            logger.error("No timestamp column found in Drift data")
            return DataFrame()

        # Convert timestamp to datetime
        if timestamp_col == 'ts':
            try:
                df['timestamp'] = pd.to_datetime(df['ts'], unit='s', utc=True)
            except:
                try:
                    df['timestamp'] = pd.to_datetime(
                        df['ts'], unit='ms', utc=True)
                except:
                    df['timestamp'] = pd.to_datetime(df['ts'], utc=True)
        elif timestamp_col != 'timestamp':
            df['timestamp'] = df[timestamp_col]

        # Create minute floor column for aggregation
        df['timestamp_minute'] = df['timestamp'].dt.floor(timeframe)

        # Determine trade direction
        direction_col = None
        for col in ['takerOrderDirection', 'direction', 'side', 'takerSide']:
            if col in df.columns:
                direction_col = col
                break

        if direction_col:
            if direction_col == 'takerOrderDirection':
                df['side'] = df[direction_col].map(
                    lambda x: 'buy' if x == 'long' else 'sell' if x == 'short' else 'unknown'
                )
            else:
                mapping_dict = {'buy': 'buy', 'sell': 'sell',
                                'long': 'buy', 'short': 'sell'}
                df['side'] = df[direction_col].map(
                    lambda x: mapping_dict.get(x, 'unknown')
                )

            # Filter out unknown directions
            df = df[df['side'].isin(['buy', 'sell'])]
        else:
            df['side'] = 'buy'

        # Handle volume columns
        volume_col = None
        for col in ['baseAssetAmountFilled', 'volume', 'size', 'amount', 'baseAmount']:
            if col in df.columns:
                volume_col = col
                break

        if volume_col:
            df['volume'] = df[volume_col].abs()
        else:
            logger.warning("No volume column found in Drift data")
            df['volume'] = 0

        # Handle quote volume
        quote_volume_col = None
        for col in ['quoteAssetAmountFilled', 'quoteVolume', 'notional', 'quoteAmount']:
            if col in df.columns:
                quote_volume_col = col
                break

        if quote_volume_col:
            df['quote_volume'] = df[quote_volume_col].abs()
        else:
            df['quote_volume'] = 0

        # Handle price columns
        price_col = None
        for col in ['price', 'oraclePrice', 'fillPrice', 'executionPrice']:
            if col in df.columns:
                price_col = col
                break

        if price_col:
            df['oracle_price'] = df[price_col]
        else:
            df['oracle_price'] = 0

        # Calculate trade price
        df['price'] = df.apply(
            lambda row: row['quote_volume'] /
            row['volume'] if row['volume'] > 0 else row['oracle_price'],
            axis=1
        )

        # Group by minute and side
        grouped = df.groupby(['timestamp_minute', 'side']).agg({
            'volume': 'sum',
            'price': 'mean',
            'ts': 'count'  # count trades
        }).rename(columns={'ts': 'trade_count'})

        # Reset index to work with columns
        grouped = grouped.reset_index()

        # Pivot to get buy/sell columns
        pivot = grouped.pivot(index='timestamp_minute', columns='side', values=[
                              'volume', 'price', 'trade_count'])

        # Flatten column names
        pivot.columns = [f'{val}_{col}' for val, col in pivot.columns]
        pivot = pivot.reset_index()

        # Rename columns to match VulcanTrader conventions
        column_mapping = {
            'volume_buy': 'buy_volume',
            'volume_sell': 'sell_volume',
            'price_buy': 'buy_avg_price',
            'price_sell': 'sell_avg_price',
            'trade_count_buy': 'buy_count',
            'trade_count_sell': 'sell_count'
        }

        pivot = pivot.rename(columns=column_mapping)

        # Ensure required columns exist
        for col in ['buy_volume', 'sell_volume', 'buy_count', 'sell_count']:
            if col not in pivot.columns:
                pivot[col] = 0

        for col in ['buy_avg_price', 'sell_avg_price']:
            if col not in pivot.columns:
                pivot[col] = None

        # Rename timestamp column
        pivot = pivot.rename(columns={'timestamp_minute': 'date'})

        # Add orderflow metrics
        if 'buy_volume' in pivot.columns and 'sell_volume' in pivot.columns:
            pivot['delta'] = pivot['buy_volume'] - pivot['sell_volume']
            pivot['cumulative_delta'] = pivot['delta'].cumsum()
            pivot['total_volume'] = pivot['buy_volume'] + pivot['sell_volume']

            # Calculate buy/sell ratio safely
            pivot['buy_sell_ratio'] = pivot.apply(
                lambda row: row['buy_volume'] / row['sell_volume']
                if row['sell_volume'] > 0 else 0,
                axis=1
            )

        # Sort by date
        pivot = pivot.sort_values('date')

        logger.info(f"Processed {len(pivot)} candles with orderflow data")

        return pivot

    def get_historic_trades(
        self,
        pair: str,
        since: int | None = None,
        until: int | None = None,
        from_id: str | None = None,
    ) -> tuple[str | None, list]:
        """
        Fetch historic trades from Drift.

        :param pair: Pair to fetch trades for
        :param since: Timestamp in milliseconds to fetch from
        :param until: Timestamp in milliseconds to fetch until
        :param from_id: Trade ID to fetch from
        :return: Tuple of (trade_id, trades_list)
        """
        try:
            # Convert pair to Drift market format
            market = self._convert_pair_to_drift_market(
                pair, CandleType.FUTURES)

            # Convert timestamps to datetime objects for Drift API
            start_date = datetime.fromtimestamp(
                since / 1000, tz=UTC) if since else None
            end_date = datetime.fromtimestamp(
                until / 1000, tz=UTC) if until else None

            # Fetch trades data from Drift
            trades_df = self._download_drift_trades(
                market=market,
                start_date=start_date or datetime.now(
                    tz=UTC) - timedelta(days=1),
                end_date=end_date or datetime.now(tz=UTC)
            )

            if trades_df.empty:
                logger.warning(f"No trades data available for {pair}")
                return None, []

            # Convert DataFrame to the format expected by VulcanTrader
            # DEFAULT_TRADES_COLUMNS = ["timestamp", "id", "type", "side", "price", "amount", "cost"]
            formatted_trades = []

            for _, trade in trades_df.iterrows():
                # Convert Drift trade data to VulcanTrader format
                ts = float(trade.get('ts', 0))
                timestamp = int(ts * 1000) if ts < 1e12 else int(ts)
                trade_id = str(trade.get('fillRecordId', ''))
                trade_type = 'limit'  # Drift trades are typically limit orders

                # Determine side based on taker order direction
                side = 'buy' if trade.get(
                    'takerOrderDirection') == 'Long' else 'sell'

                # Convert prices and amounts from Drift precision
                price = float(trade.get('oraclePrice', 0)) / \
                    self.PRICE_PRECISION
                amount = float(
                    trade.get('baseAssetAmountFilled', 0)) / self.BASE_PRECISION
                cost = float(trade.get('quoteAssetAmountFilled', 0)
                             ) / self.QUOTE_PRECISION

                # Create trade record in VulcanTrader format (list/tuple format)
                trade_record = [
                    timestamp,  # timestamp
                    trade_id,   # id
                    trade_type,  # type
                    side,       # side
                    price,      # price
                    amount,     # amount
                    cost        # cost
                ]
                formatted_trades.append(trade_record)

            # Return the last trade ID and the list of trades
            # id is at index 1
            last_trade_id = formatted_trades[-1][1] if formatted_trades else None
            return last_trade_id, formatted_trades

        except Exception as e:
            logger.error(f"Error fetching historic trades for {pair}: {e}")
            raise TemporaryError(f"Could not fetch trades: {e}") from e

    def _load_existing_trades_data(self, pair: str, candle_type: CandleType, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """
        Load existing trades data from feather file with memory-efficient filtering.

        :param pair: Trading pair
        :param candle_type: Candle type (SPOT/FUTURES)
        :param start_date: Required start date for data
        :param end_date: Required end date for data
        :return: DataFrame with trades data or empty DataFrame if not available
        """
        try:
            from VulcanTrader.data.history.datahandlers.featherdatahandler import FeatherDataHandler

            # Get data directory
            datadir_config = self._config.get(
                'datadir', 'user_data/data/drift')
            data_dir = Path(datadir_config)
            data_handler = FeatherDataHandler(data_dir)

            # Determine trading mode
            trading_mode = TradingMode.FUTURES if candle_type == CandleType.FUTURES else TradingMode.SPOT

            # Try to load existing trades data
            trades_df = data_handler.trades_load(pair, trading_mode)

            if trades_df.empty:
                logger.debug(f"No existing trades data found for {pair}")
                return pd.DataFrame()

            logger.info(
                f"Loaded {len(trades_df)} existing trades for {pair} - checking date coverage...")

            # Convert timestamp column to datetime for filtering
            if 'timestamp' in trades_df.columns:
                # Convert timestamp efficiently
                trades_df['datetime'] = pd.to_datetime(
                    trades_df['timestamp'], unit='ms', utc=True)
            elif 'date' in trades_df.columns:
                trades_df['datetime'] = pd.to_datetime(
                    trades_df['date'], utc=True)
            else:
                logger.warning(
                    f"No timestamp column found in existing trades data for {pair}")
                return pd.DataFrame()

            # Get data coverage info efficiently
            data_start = trades_df['datetime'].min()
            data_end = trades_df['datetime'].max()

            # Ensure timezone consistency
            if start_date.tzinfo is None:
                start_ts = start_date.replace(tzinfo=UTC)
            else:
                start_ts = start_date.astimezone(UTC)

            if end_date.tzinfo is None:
                end_ts = end_date.replace(tzinfo=UTC)
            else:
                end_ts = end_date.astimezone(UTC)

            # Debug logging
            logger.info(f"Date comparison for {pair}:")
            logger.info(f"  Requested start: {start_ts} ({start_ts.date()})")
            logger.info(f"  Requested end: {end_ts} ({end_ts.date()})")
            logger.info(
                f"  Available start: {data_start} ({data_start.date()})")
            logger.info(f"  Available end: {data_end} ({data_end.date()})")

            # Use flexible date range checking
            # Allow up to 1 hour difference at start
            start_tolerance = timedelta(hours=1)
            # Allow up to 1 day difference at end
            end_tolerance = timedelta(days=1)

            start_ok = data_start <= (start_ts + start_tolerance)
            end_ok = data_end >= (end_ts - end_tolerance)

            if start_ok and end_ok:
                # Filter to requested range efficiently using boolean indexing
                mask = (trades_df['datetime'] >= start_ts) & (
                    trades_df['datetime'] <= end_ts)

                # For large datasets, use chunked filtering to avoid memory issues
                if len(trades_df) > 100000:  # Large dataset threshold
                    logger.info(
                        f"Large dataset detected ({len(trades_df)} trades), using chunked filtering...")

                    chunk_size = 50000
                    filtered_chunks = []

                    for i in range(0, len(trades_df), chunk_size):
                        chunk_end = min(i + chunk_size, len(trades_df))
                        chunk = trades_df.iloc[i:chunk_end]
                        chunk_mask = (chunk['datetime'] >= start_ts) & (
                            chunk['datetime'] <= end_ts)
                        filtered_chunk = chunk[chunk_mask]

                        if not filtered_chunk.empty:
                            filtered_chunks.append(filtered_chunk)

                        logger.debug(
                            f"Processed chunk {i//chunk_size + 1} - {len(filtered_chunk)} trades match date range")

                    # Combine filtered chunks
                    if filtered_chunks:
                        filtered_trades = pd.concat(
                            filtered_chunks, ignore_index=True)
                    else:
                        filtered_trades = pd.DataFrame()

                else:
                    # Small dataset - filter normally
                    filtered_trades = trades_df[mask].copy()

                # Clean up datetime column if it was temporary
                if 'datetime' in filtered_trades.columns and 'datetime' not in trades_df.columns:
                    filtered_trades = filtered_trades.drop(
                        columns=['datetime'])

                logger.info(
                    f"Found existing trades data for {pair}: {len(filtered_trades)} trades covering requested range")
                return filtered_trades
            else:
                logger.info(
                    f"Existing trades data for {pair} doesn't cover full requested range:")
                logger.info(
                    f"  Requested: {start_date.date()} to {end_date.date()}")
                logger.info(
                    f"  Available: {data_start.date()} to {data_end.date()}")
                logger.info(
                    f"  Start coverage: {'ok' if start_ok else 'missing'}, End coverage: {'ok' if end_ok else 'missing'}")
                return pd.DataFrame()

        except Exception as e:
            logger.debug(
                f"Could not load existing trades data for {pair}: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return pd.DataFrame()

    def _get_special_candle_data(self, pair: str, timeframe: str, candle_type: CandleType, since_ms: int | None = None, until_ms: int | None = None) -> DataFrame:
        """
        Handle special candle types that don't require trades data downloads.
        This includes funding_rate, mark price, index price, etc.

        :param pair: Trading pair
        :param timeframe: Timeframe (e.g., '1h', '8h')
        :param candle_type: Type of candle data requested
        :param since_ms: Start timestamp in milliseconds
        :param until_ms: End timestamp in milliseconds
        :return: DataFrame with special candle data or empty DataFrame
        """
        try:
            candle_type_str = str(candle_type).lower()

            if 'funding_rate' in candle_type_str:
                logger.info(
                    f"Funding rate data requested for {pair} - fetching from Drift funding rates API")
                return self._get_funding_rate_ohlcv(pair, timeframe, since_ms, until_ms)
            elif 'mark' in candle_type_str:
                logger.info(
                    f"Mark price data requested for {pair} - fetching from Drift mark prices API")
                return self._get_mark_price_ohlcv(pair, timeframe, since_ms, until_ms)
            elif 'index' in candle_type_str:
                logger.info(
                    f"Index price data requested for {pair} - Drift index prices not available")
                logger.info(
                    f"Returning empty index price data for {pair}")
            else:
                logger.info(
                    f"Special candle type {candle_type} requested for {pair} - not available")

            # Create empty DataFrame with expected structure
            # VulcanTrader expects these columns for all OHLCV data
            empty_df = DataFrame(
                columns=['date', 'open', 'high', 'low', 'close', 'volume'])

            return empty_df

        except Exception as e:
            logger.error(
                f"Error handling special candle type {candle_type} request for {pair}: {e}")
            return DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'volume'])

    def _get_mark_price_ohlcv(self, pair: str, timeframe: str, since_ms: int | None = None, until_ms: int | None = None) -> DataFrame:
        """
        Convert mark price data to OHLCV format for compatibility with VulcanTrader.
        Mark prices are derived from funding rate data on Drift.

        :param pair: Trading pair
        :param timeframe: Timeframe (e.g., '1h', '8h')
        :param since_ms: Start timestamp in milliseconds
        :param until_ms: End timestamp in milliseconds
        :return: DataFrame with mark price data in OHLCV format
        """
        try:
            # Fetch funding rate history which contains mark prices
            funding_rates = self.fetch_funding_rate_history(
                pair=pair,
                since=since_ms,
                limit=None  # Get all available data in range
            )

            if not funding_rates:
                logger.warning(f"No mark price data available for {pair}")
                return DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'volume'])

            # Convert funding rate records to mark price OHLCV DataFrame
            ohlcv_data = []

            for rate_record in funding_rates:
                try:
                    timestamp_ms = rate_record.get('timestamp', 0)
                    mark_price = rate_record.get('markPriceTwap', 0)

                    if mark_price <= 0:
                        continue

                    # Convert timestamp to datetime
                    date = pd.to_datetime(timestamp_ms, unit='ms', utc=True)

                    # For mark prices, all OHLCV values are the same (the mark price)
                    # Volume is set to 0 as it's not applicable for mark prices
                    ohlcv_record = {
                        'date': date,
                        'open': float(mark_price),
                        'high': float(mark_price),
                        'low': float(mark_price),
                        'close': float(mark_price),
                        'volume': 0
                    }

                    ohlcv_data.append(ohlcv_record)

                except Exception as e:
                    logger.warning(
                        f"Error processing mark price record: {rate_record}, error: {e}")
                    continue

            if not ohlcv_data:
                logger.warning(
                    f"No valid mark price data processed for {pair}")
                return DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'volume'])

            # Create DataFrame
            df = DataFrame(ohlcv_data)

            # Sort by date
            df = df.sort_values('date').reset_index(drop=True)

            # Filter by until_ms if provided
            if until_ms:
                until_date = pd.to_datetime(until_ms, unit='ms', utc=True)
                df = df[df['date'] <= until_date]

            # Resample to requested timeframe into OHLCV using close as source
            try:
                rule = (
                    timeframe.replace('m', 'T')
                    .replace('h', 'h')
                    .replace('d', 'D')
                    .replace('w', 'W')
                )
                ohlc = (
                    df.set_index('date')['close']
                    .resample(rule)
                    .agg(['first', 'max', 'min', 'last'])
                    .rename(columns={'first': 'open', 'max': 'high', 'min': 'low', 'last': 'close'})
                )
                # Reindex to full grid and forward-fill to ensure contiguous OHLCV like ccxt-based sources
                full_index = pd.date_range(
                    start=pd.to_datetime(
                        since_ms, unit='ms', utc=True) if since_ms else ohlc.index.min(),
                    end=pd.to_datetime(
                        until_ms, unit='ms', utc=True) if until_ms else ohlc.index.max(),
                    freq=rule
                )
                ohlc = ohlc.reindex(full_index)
                # Forward-fill close and set O/H/L to close for empty buckets
                ohlc['close'] = ohlc['close'].ffill()
                for c in ['open', 'high', 'low']:
                    ohlc[c] = ohlc[c].fillna(ohlc['close'])
                ohlc['volume'] = 0.0
                ohlc = ohlc.reset_index()
                # Ensure index column is named 'date' after reset_index
                if 'index' in ohlc.columns:
                    ohlc.rename(columns={'index': 'date'}, inplace=True)
                logger.info(
                    f"Converted {len(df)} mark price records to {timeframe} OHLCV for {pair}")
                return ohlc[['date', 'open', 'high', 'low', 'close', 'volume']]
            except Exception as e:
                logger.warning(
                    f"Resample mark price to {timeframe} failed: {e}. Returning un-resampled data.")
                return df

        except Exception as e:
            logger.error(
                f"Error converting mark price data to OHLCV for {pair}: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'volume'])

    def _get_funding_rate_ohlcv(self, pair: str, timeframe: str, since_ms: int | None = None, until_ms: int | None = None) -> DataFrame:
        """
        Convert funding rate data to OHLCV format for compatibility with VulcanTrader.

        :param pair: Trading pair
        :param timeframe: Timeframe (e.g., '1h', '8h')
        :param since_ms: Start timestamp in milliseconds
        :param until_ms: End timestamp in milliseconds
        :return: DataFrame with funding rate data in OHLCV format
        """
        try:
            # Fetch funding rate history using existing method
            funding_rates = self.fetch_funding_rate_history(
                pair=pair,
                since=since_ms,
                limit=None  # Get all available data in range
            )

            if not funding_rates:
                logger.warning(f"No funding rate data available for {pair}")
                return DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'volume'])

            # Convert funding rate records to OHLCV DataFrame
            ohlcv_data = []

            for rate_record in funding_rates:
                try:
                    timestamp_ms = rate_record.get('timestamp', 0)
                    funding_rate = rate_record.get('fundingRate', 0)

                    # Convert timestamp to datetime
                    date = pd.to_datetime(timestamp_ms, unit='ms', utc=True)

                    # For funding rates, all OHLCV values are the same (the funding rate)
                    # Volume is set to 0 as it's not applicable for funding rates
                    ohlcv_record = {
                        'date': date,
                        'open': funding_rate,
                        'high': funding_rate,
                        'low': funding_rate,
                        'close': funding_rate,
                        'volume': 0
                    }

                    ohlcv_data.append(ohlcv_record)

                except Exception as e:
                    logger.warning(
                        f"Error processing funding rate record: {rate_record}, error: {e}")
                    continue

            if not ohlcv_data:
                logger.warning(
                    f"No valid funding rate data processed for {pair}")
                return DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'volume'])

            # Create DataFrame
            df = DataFrame(ohlcv_data)

            # Sort by date
            df = df.sort_values('date').reset_index(drop=True)

            # Filter by until_ms if provided
            if until_ms:
                until_date = pd.to_datetime(until_ms, unit='ms', utc=True)
                df = df[df['date'] <= until_date]

            # Resample to requested timeframe into OHLCV using close as source (funding)
            try:
                rule = (
                    timeframe.replace('m', 'T')
                    .replace('h', 'h')
                    .replace('d', 'D')
                    .replace('w', 'W')
                )
                ohlc = (
                    df.set_index('date')['close']
                    .resample(rule)
                    .agg(['first', 'max', 'min', 'last'])
                    .rename(columns={'first': 'open', 'max': 'high', 'min': 'low', 'last': 'close'})
                )
                # Reindex to full grid and forward-fill to ensure contiguous OHLCV like ccxt-based sources
                full_index = pd.date_range(
                    start=pd.to_datetime(
                        since_ms, unit='ms', utc=True) if since_ms else ohlc.index.min(),
                    end=pd.to_datetime(
                        until_ms, unit='ms', utc=True) if until_ms else ohlc.index.max(),
                    freq=rule
                )
                ohlc = ohlc.reindex(full_index)
                # Forward-fill close and set O/H/L to close for empty buckets
                ohlc['close'] = ohlc['close'].ffill()
                for c in ['open', 'high', 'low']:
                    ohlc[c] = ohlc[c].fillna(ohlc['close'])
                ohlc['volume'] = 0.0
                ohlc = ohlc.reset_index()
                # Ensure index column is named 'date' after reset_index
                if 'index' in ohlc.columns:
                    ohlc.rename(columns={'index': 'date'}, inplace=True)
                logger.info(
                    f"Converted {len(df)} funding rate records to {timeframe} OHLCV for {pair}")
                return ohlc[['date', 'open', 'high', 'low', 'close', 'volume']]
            except Exception as e:
                logger.warning(
                    f"Resample funding rate to {timeframe} failed: {e}. Returning un-resampled data.")
                return df

        except Exception as e:
            logger.error(
                f"Error converting funding rate data to OHLCV for {pair}: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'volume'])

    def get_historic_ohlcv(
        self,
        pair: str,
        timeframe: str,
        since_ms: int | None = None,
        is_new_pair: bool = False,
        raise_: bool = False,
        candle_type: CandleType = CandleType.SPOT,
        until_ms: int | None = None,
    ) -> DataFrame:
        """
        Download historic OHLCV data directly from Drift Protocol candles API.

        :param pair: Pair to download data for
        :param timeframe: Timeframe to download (e.g., '1m', '5m', '1h')
        :param since_ms: Timestamp in milliseconds to start download from
        :param is_new_pair: Whether this is a new pair
        :param raise_: Whether to raise exceptions
        :param candle_type: Type of candle data
        :param until_ms: Timestamp in milliseconds to download until
        :return: DataFrame with OHLCV data
        """
        try:
            # Handle special candle types that don't use candles API
            if (candle_type == CandleType.FUNDING_RATE or
                candle_type == CandleType.MARK or
                    candle_type == CandleType.INDEX):
                logger.info(
                    f"Special candle type {candle_type} requested for {pair} - using dedicated method")
                return self._get_special_candle_data(pair, timeframe, candle_type, since_ms, until_ms)

            # Convert pair to Drift market format
            market = self._convert_pair_to_drift_market(pair, candle_type)

            # Convert timestamps to Unix UTC timestamps (seconds)
            logger.debug(f"since_ms={since_ms}, until_ms={until_ms}")

            # Check for valid timestamps (must be after year 2000)
            min_valid_timestamp = datetime(
                2000, 1, 1, tzinfo=UTC).timestamp() * 1000

            start_ts = None
            if since_ms and since_ms > min_valid_timestamp:
                start_ts = int(since_ms / 1000)  # Convert to seconds

            end_ts = None
            if until_ms and until_ms > min_valid_timestamp:
                end_ts = int(until_ms / 1000)  # Convert to seconds

            # Default to reasonable date range if no valid start timestamp provided
            if not start_ts:
                days_back = 30 if is_new_pair else 1
                start_ts = int(
                    (datetime.now(tz=UTC) - timedelta(days=days_back)).timestamp())
                logger.info(
                    f"No valid start timestamp provided (since_ms={since_ms}), using {days_back} days back")

            if not end_ts:
                end_ts = int(datetime.now(tz=UTC).timestamp())

            # Download OHLCV data directly from Drift candles API
            ohlcv_df = self._download_drift_candles(
                market=market,
                timeframe=timeframe,
                start_ts=start_ts,
                end_ts=end_ts
            )

            if ohlcv_df.empty:
                logger.warning(
                    f"No OHLCV data available for {pair} {timeframe}")
                return DataFrame(columns=DEFAULT_DATAFRAME_COLUMNS)

            logger.info(
                f"Downloaded {len(ohlcv_df)} candles for {pair} {timeframe}")
            return ohlcv_df

        except Exception as e:
            logger.error(f"Error downloading OHLCV data for {pair}: {e}")
            if raise_:
                raise TemporaryError(
                    f"Could not download OHLCV data: {e}") from e
            return DataFrame(columns=DEFAULT_DATAFRAME_COLUMNS)

    def _download_drift_candles(
        self,
        market: str,
        timeframe: str,
        start_ts: int,
        end_ts: int
    ) -> DataFrame:
        """
        Download OHLCV candle data from Drift Protocol candles API with pagination.

        Since Drift does not provide aggregated candle data, we fetch trades data
        and aggregate it into OHLCV format as a fallback.

        :param market: Drift market identifier (e.g., "BTC-PERP")
        :param timeframe: Timeframe (e.g., '1m', '5m', '1h')
        :param start_ts: Start timestamp in Unix UTC seconds
        :param end_ts: End timestamp in Unix UTC seconds
        :return: DataFrame with OHLCV data
        """
        try:
            logger.info(
                f"Drift candles API not available, falling back to OHLCV generation from trades data")

            # Convert timestamps to datetime objects
            start_date = datetime.fromtimestamp(start_ts, tz=UTC)
            end_date = datetime.fromtimestamp(end_ts, tz=UTC)

            # Fetch trades data for the timeframe
            trades_df = self._download_drift_trades(
                market=market,
                start_date=start_date,
                end_date=end_date
            )

            if trades_df.empty:
                logger.warning(
                    f"No trades data available to generate OHLCV for {market}")
                return DataFrame(columns=DEFAULT_DATAFRAME_COLUMNS)

            logger.info(
                f"Converting {len(trades_df)} trades to {timeframe} OHLCV for {market}")

            # Convert trades data to OHLCV format
            ohlcv_df = self._convert_trades_to_ohlcv(
                trades_df, timeframe, start_dt=start_date, end_dt=end_date
            )

            if ohlcv_df.empty:
                logger.warning(
                    f"Failed to generate OHLCV data from trades for {market}")
                return DataFrame(columns=DEFAULT_DATAFRAME_COLUMNS)

            logger.info(
                f"Successfully generated {len(ohlcv_df)} {timeframe} candles from trades data for {market}")
            return ohlcv_df

        except Exception as e:
            logger.error(
                f"Error generating OHLCV from trades for {market}: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return DataFrame(columns=DEFAULT_DATAFRAME_COLUMNS)

    def _convert_timeframe_to_drift_interval(self, timeframe: str) -> str:
        """
        Convert VulcanTrader timeframe to Drift candles API interval format.

        :param timeframe: VulcanTrader timeframe (e.g., '1m', '5m', '1h', '1d')
        :return: Drift interval string
        """
        # Mapping from VulcanTrader timeframes to Drift intervals
        timeframe_mapping = {
            '1m': '1',      # 1 minute
            '5m': '5',      # 5 minutes
            '15m': '15',    # 15 minutes
            '1h': '60',     # 60 minutes (1 hour)
            '4h': '240',    # 240 minutes (4 hours)
            '1d': 'D',      # 1 day
            '1w': 'W',      # 1 week
            '1M': 'M'       # 1 month
        }

        if timeframe in timeframe_mapping:
            return timeframe_mapping[timeframe]
        else:
            # Try to parse custom timeframes
            if timeframe.endswith('m'):
                minutes = int(timeframe[:-1])
                if minutes in [1, 5, 15, 60, 240]:
                    return str(minutes)
            elif timeframe.endswith('h'):
                hours = int(timeframe[:-1])
                minutes = hours * 60
                if minutes in [60, 240]:  # 1h, 4h
                    return str(minutes)

            # Default fallback
            logger.warning(
                f"Unsupported timeframe {timeframe}, defaulting to 5m")
            return '5'

    def _process_drift_candles(self, candles_data: list) -> DataFrame:
        """
        Process raw Drift candles data into VulcanTrader OHLCV format.

        :param candles_data: List of candle records from Drift API
        :return: DataFrame with OHLCV data
        """
        try:
            if not candles_data:
                return DataFrame(columns=DEFAULT_DATAFRAME_COLUMNS)

            processed_candles = []

            for candle in candles_data:
                try:
                    # Extract timestamp and convert to datetime
                    timestamp = candle.get('ts', 0)
                    date = pd.to_datetime(timestamp, unit='s', utc=True)

                    # Use fill prices (actual traded prices) as primary, oracle as fallback
                    # Drift provides both fill prices (actual trades) and oracle prices
                    open_price = candle.get(
                        'fillOpen', candle.get('oracleOpen', 0))
                    high_price = candle.get(
                        'fillHigh', candle.get('oracleHigh', 0))
                    low_price = candle.get(
                        'fillLow', candle.get('oracleLow', 0))
                    close_price = candle.get(
                        'fillClose', candle.get('oracleClose', 0))

                    # Use base volume (in base asset units)
                    volume = candle.get('baseVolume', 0)

                    # Validate data
                    if all(price > 0 for price in [open_price, high_price, low_price, close_price]):
                        processed_candles.append({
                            'date': date,
                            'open': float(open_price),
                            'high': float(high_price),
                            'low': float(low_price),
                            'close': float(close_price),
                            'volume': float(volume)
                        })
                    else:
                        logger.debug(f"Skipping invalid candle data: {candle}")

                except Exception as e:
                    logger.warning(
                        f"Error processing candle record: {candle}, error: {e}")
                    continue

            if not processed_candles:
                logger.warning("No valid candles processed from Drift data")
                return DataFrame(columns=DEFAULT_DATAFRAME_COLUMNS)

            # Create DataFrame
            df = DataFrame(processed_candles)

            # Sort by date (Drift returns in descending order, we want ascending)
            df = df.sort_values('date').reset_index(drop=True)

            # Ensure all numeric columns are float type
            numeric_cols = ['open', 'high', 'low', 'close', 'volume']
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)

            # Remove any rows with NaN values
            df = df.dropna()

            logger.info(f"Processed {len(df)} valid candles from Drift data")
            return df

        except Exception as e:
            logger.error(f"Error processing Drift candles data: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return DataFrame(columns=DEFAULT_DATAFRAME_COLUMNS)

    def generate_ohlcv_from_trades(self, pair: str, timeframes: list[str], candle_type: CandleType = CandleType.FUTURES) -> dict[str, DataFrame]:
        """
        Generate OHLCV data for multiple timeframes from existing trades data.
        This method is primarily for orderflow analysis or when you need OHLCV derived from trades.
        For regular OHLCV data, use get_historic_ohlcv() which downloads directly from Drift's candles API.

        :param pair: Trading pair
        :param timeframes: List of timeframes to generate (e.g., ['5m', '15m', '1h'])
        :param candle_type: Candle type (SPOT/FUTURES)
        :return: Dictionary mapping timeframe to OHLCV DataFrame
        """
        try:
            from VulcanTrader.data.history.datahandlers.featherdatahandler import FeatherDataHandler

            # Get data directory
            datadir_config = self._config.get(
                'datadir', 'user_data/data/drift')
            data_dir = Path(datadir_config)
            data_handler = FeatherDataHandler(data_dir)

            # Determine trading mode
            trading_mode = TradingMode.FUTURES if candle_type == CandleType.FUTURES else TradingMode.SPOT

            # Load existing trades data
            trades_df = data_handler.trades_load(pair, trading_mode)

            if trades_df.empty:
                logger.warning(
                    f"No trades data found for {pair}. Run trade download first.")
                logger.info(
                    f"For regular OHLCV data, use get_historic_ohlcv() instead.")
                return {}

            logger.info(
                f"Generating OHLCV for {pair} from {len(trades_df)} existing trades")

            # Generate OHLCV for each timeframe
            ohlcv_results = {}

            for timeframe in timeframes:
                try:
                    logger.info(
                        f"Converting trades to {timeframe} OHLCV for {pair}")

                    # Convert trades to OHLCV for this timeframe
                    ohlcv_df = self._convert_trades_to_ohlcv(
                        trades_df, timeframe)

                    if not ohlcv_df.empty:
                        # Save OHLCV data to feather file with special suffix to distinguish from API data
                        try:
                            # Use a special filename suffix to distinguish trades-derived OHLCV
                            modified_pair = f"{pair}_from_trades"
                            data_handler.ohlcv_store(
                                modified_pair, timeframe, data=ohlcv_df, candle_type=candle_type)
                            logger.info(
                                f"Saved {len(ohlcv_df)} {timeframe} candles for {modified_pair}")
                            ohlcv_results[timeframe] = ohlcv_df
                        except Exception as e:
                            logger.error(
                                f"Failed to save {timeframe} OHLCV data for {pair}: {e}")
                    else:
                        logger.warning(
                            f"Could not generate {timeframe} OHLCV for {pair}")

                except Exception as e:
                    logger.error(
                        f"Error generating {timeframe} OHLCV for {pair}: {e}")

            if ohlcv_results:
                logger.info(
                    f"Successfully generated OHLCV from trades for {pair}: {list(ohlcv_results.keys())}")
            else:
                logger.warning(
                    f"No OHLCV data generated from trades for {pair}")

            return ohlcv_results

        except Exception as e:
            logger.error(f"Error generating OHLCV from trades for {pair}: {e}")
            return {}

    def _convert_trades_to_ohlcv(
        self,
        trades_df: DataFrame,
        timeframe: str,
        start_dt: datetime | None = None,
        end_dt: datetime | None = None,
    ) -> DataFrame:
        """
        Convert trades DataFrame to OHLCV format.

        :param trades_df: DataFrame with trades data
        :param timeframe: Target timeframe (e.g., '1m', '5m', '1h')
        :return: DataFrame with OHLCV data
        """
        try:
            if trades_df.empty:
                return DataFrame(columns=DEFAULT_DATAFRAME_COLUMNS)

            # Convert timeframe to pandas offset
            timeframe_ms = timeframe_to_msecs(timeframe)
            freq = f"{timeframe_ms // 1000}s"  # Convert to seconds for pandas

            # Flexible timestamp column detection
            timestamp_col = None
            datetime_col = None

            # Check available columns for debugging
            logger.info(
                f"Available columns in trades data: {list(trades_df.columns)}")

            # DEBUG: Check sample values in key columns
            if len(trades_df) > 0:
                logger.debug(f"Sample raw trade data:")
                for col in ['ts', 'timestamp', 'oraclePrice', 'baseAssetAmountFilled']:
                    if col in trades_df.columns:
                        sample_val = trades_df[col].iloc[0]
                        logger.debug(
                            f"  {col}: {sample_val} (type: {type(sample_val)})")

            # Check if we have sequential timestamps indicating an index was used instead of real timestamps
            if 'timestamp' in trades_df.columns:
                ts_sample = trades_df['timestamp'].head(5).tolist()
                logger.debug(f"First 5 timestamp values: {ts_sample}")
                if all(isinstance(x, int) and x < 2000000 for x in ts_sample if pd.notna(x)):
                    logger.warning(
                        "DETECTED SEQUENTIAL TIMESTAMPS - this indicates index was used instead of real timestamp data!")
                    logger.warning(
                        "This is the root cause of the timestamp issue - investigating source...")

            # Look for timestamp column (prefer real timestamps, avoid daily label columns)
            for col in ['ts', 'timestamp']:
                if col in trades_df.columns:
                    timestamp_col = col
                    break

            # Look for datetime column (explicit only)
            for col in ['datetime']:
                if col in trades_df.columns and pd.api.types.is_datetime64_any_dtype(trades_df[col]):
                    datetime_col = col
                    break

            # Convert timestamp to datetime with consistent timezone handling
            if datetime_col:
                # Already have datetime column, ensure it's timezone-aware
                trades_df['datetime'] = pd.to_datetime(
                    trades_df[datetime_col], utc=True)
                logger.info(f"Using existing datetime column: {datetime_col}")
            elif timestamp_col:
                # Convert timestamp to datetime
                if trades_df[timestamp_col].dtype == 'object':  # String
                    trades_df[timestamp_col] = pd.to_numeric(
                        trades_df[timestamp_col], errors='coerce')

                # Determine if timestamps are in seconds or milliseconds
                sample_ts = trades_df[timestamp_col].iloc[0] if len(
                    trades_df) > 0 else 0
                # Milliseconds (larger than year 2001 in seconds)
                if sample_ts > 1e12:
                    trades_df['datetime'] = pd.to_datetime(
                        trades_df[timestamp_col], unit='ms', utc=True)
                else:  # Seconds
                    trades_df['datetime'] = pd.to_datetime(
                        trades_df[timestamp_col], unit='s', utc=True)
                logger.info(
                    f"Converted timestamp column {timestamp_col} to datetime")
            else:
                logger.error(
                    f"Missing timestamp column in trades data. Available columns: {list(trades_df.columns)}")
                return DataFrame(columns=DEFAULT_DATAFRAME_COLUMNS)

            # Flexible price column detection
            price_col = None
            for col in ['oraclePrice', 'price', 'cost']:
                if col in trades_df.columns:
                    price_col = col
                    break

            # Flexible amount/volume column detection
            amount_col = None
            for col in ['baseAssetAmountFilled', 'amount', 'volume']:
                if col in trades_df.columns:
                    amount_col = col
                    break

            if not price_col:
                logger.error(
                    f"Missing price column in trades data. Available columns: {list(trades_df.columns)}")
                return DataFrame(columns=DEFAULT_DATAFRAME_COLUMNS)

            # Convert columns to numeric first, then apply Drift precision conversion
            trades_df[price_col] = pd.to_numeric(
                trades_df[price_col], errors='coerce')

            # Apply Drift precision conversion to oraclePrice
            if price_col == 'oraclePrice':
                # Check if oraclePrice is already in normal price format (realistic BTC prices)
                sample_price = trades_df[price_col].iloc[0] if len(
                    trades_df) > 0 else 0
                # Already looks like realistic pricing (> $1000)
                if sample_price > 1000:
                    print(
                        f"DRIFT DEBUG: oraclePrice already in realistic format (${sample_price:,.2f}), using as-is")
                    trades_df['price'] = trades_df[price_col]
                    print(
                        f"DRIFT DEBUG: After assignment, final price column sample: {trades_df['price'].iloc[0] if len(trades_df) > 0 else 'N/A'}")
                else:
                    print(
                        f"DRIFT DEBUG: Applying precision conversion to oraclePrice (sample before: {sample_price})")
                    trades_df['price'] = trades_df[price_col] / \
                        self.PRICE_PRECISION
                    print(
                        f"DRIFT DEBUG: Sample after precision conversion: ${trades_df['price'].iloc[0] if len(trades_df) > 0 else 'N/A'}")
            else:
                print(
                    f"DRIFT DEBUG: Price column is {price_col}, not oraclePrice - no precision conversion applied")
                trades_df['price'] = trades_df[price_col]

            if amount_col:
                trades_df[amount_col] = pd.to_numeric(
                    trades_df[amount_col], errors='coerce')

                # Apply Drift precision conversion to baseAssetAmountFilled if needed
                if amount_col == 'baseAssetAmountFilled':
                    logger.debug(
                        f"Base asset amount (raw): {trades_df[amount_col].iloc[0] if len(trades_df) > 0 else 'N/A'}")
                    # baseAssetAmountFilled appears to already be in normal units, don't divide by precision
                    trades_df['volume'] = trades_df[amount_col]
                else:
                    trades_df['volume'] = trades_df[amount_col]
            else:
                # Try to calculate volume from cost/price if available
                if 'cost' in trades_df.columns:
                    trades_df['cost'] = pd.to_numeric(
                        trades_df['cost'], errors='coerce')
                    trades_df['volume'] = trades_df['cost'] / \
                        trades_df['price']
                else:
                    logger.warning(
                        "No volume/amount column found, using default volume of 1.0")
                    trades_df['volume'] = 1.0

            # Set datetime as index for resampling
            trades_df = trades_df.set_index('datetime')
            print(
                f"DRIFT DEBUG: Set datetime index, shape: {trades_df.shape}")

            # Resample to create OHLCV candles
            ohlcv = trades_df.groupby(pd.Grouper(freq=freq)).agg({
                'price': ['first', 'max', 'min', 'last'],  # OHLC
                'volume': 'sum'  # Volume
            })
            print(
                f"DRIFT DEBUG: After groupby aggregation, shape: {ohlcv.shape}")
            print(f"DRIFT DEBUG: Raw OHLCV columns: {list(ohlcv.columns)}")
            print(
                f"DRIFT DEBUG: OHLCV sample after aggregation: {ohlcv.head(2).to_string() if len(ohlcv) > 0 else 'EMPTY'}")
            # Ensure a complete time grid and fill missing candles to match ccxt-like continuity
            print(f"DRIFT DEBUG: start_dt: {start_dt}, end_dt: {end_dt}")
            print(
                f"DRIFT DEBUG: ohlcv.index.min(): {ohlcv.index.min()}, ohlcv.index.max(): {ohlcv.index.max()}")

            # Use actual data range if start_dt == end_dt (problematic case)
            range_start = start_dt if start_dt is not None and start_dt != end_dt else ohlcv.index.min()
            range_end = end_dt if end_dt is not None and start_dt != end_dt else ohlcv.index.max()

            full_index = pd.date_range(
                start=range_start,
                end=range_end,
                freq=freq
            )
            print(
                f"DRIFT DEBUG: Using range_start: {range_start}, range_end: {range_end}")
            print(
                f"DRIFT DEBUG: Created full_index range: {full_index[0]} to {full_index[-1]}, length: {len(full_index)}")
            ohlcv = ohlcv.reindex(full_index)
            print(f"DRIFT DEBUG: After reindex, shape: {ohlcv.shape}")

            # Flatten column names
            ohlcv.columns = ['open', 'high', 'low', 'close', 'volume']
            print(
                f"DRIFT DEBUG: After flattening columns, shape: {ohlcv.shape}")

            # Forward fill close, set O/H/L from close for empty buckets, and set missing volume to 0
            ohlcv['close'] = ohlcv['close'].ffill()
            for c in ['open', 'high', 'low']:
                ohlcv[c] = ohlcv[c].fillna(ohlcv['close'])
            ohlcv['volume'] = ohlcv['volume'].fillna(0.0)
            print(f"DRIFT DEBUG: After forward fill, shape: {ohlcv.shape}")

            # Reset index to get datetime as a column
            ohlcv = ohlcv.reset_index()
            print(f"DRIFT DEBUG: After reset_index, shape: {ohlcv.shape}")
            print(
                f"DRIFT DEBUG: Columns after reset_index: {list(ohlcv.columns)}")

            # Rename the datetime column to 'date'
            if 'index' in ohlcv.columns:
                ohlcv.rename(columns={'index': 'date'}, inplace=True)
            elif 'datetime' in ohlcv.columns:
                ohlcv.rename(columns={'datetime': 'date'}, inplace=True)

            print(f"DRIFT DEBUG: After rename columns, shape: {ohlcv.shape}")
            print(
                f"DRIFT DEBUG: Columns after rename: {list(ohlcv.columns)}")

            # Ensure date column is timezone-aware and in datetime format
            if not pd.api.types.is_datetime64_any_dtype(ohlcv['date']):
                ohlcv['date'] = pd.to_datetime(ohlcv['date'], utc=True)
            elif ohlcv['date'].dt.tz is None:
                # Make timezone-naive timestamps timezone-aware
                ohlcv['date'] = ohlcv['date'].dt.tz_localize('UTC')
            print(
                f"DRIFT DEBUG: After timezone handling, shape: {ohlcv.shape}")

            # Ensure no missing values
            ohlcv = ohlcv.ffill().dropna()
            print(
                f"DRIFT DEBUG: After ffill().dropna(), shape: {ohlcv.shape}")

            # VulcanTrader expects OHLCV data with a 'date' column for saving, but DatetimeIndex for processing
            # We'll provide the 'date' column format since that's what the data saving infrastructure expects
            print(
                f"DRIFT DEBUG: Final OHLCV shape before return: {ohlcv.shape}")
            print(f"DRIFT DEBUG: Final OHLCV columns: {list(ohlcv.columns)}")
            print(
                f"DRIFT DEBUG: Final OHLCV sample: {ohlcv.head(2).to_string() if len(ohlcv) > 0 else 'EMPTY DataFrame'}")
            print(
                f"DRIFT DEBUG: Date column sample: {ohlcv['date'].head(3).tolist() if len(ohlcv) > 0 else 'EMPTY'}")

            return ohlcv

        except Exception as e:
            logger.error(f"Error converting trades to OHLCV: {e}")
            print(f"DRIFT DEBUG: OHLCV conversion error: {e}")
            print(
                f"DRIFT DEBUG: Trades data shape: {trades_df.shape if 'trades_df' in locals() else 'not available'}")
            print(
                f"DRIFT DEBUG: Available columns: {list(trades_df.columns) if 'trades_df' in locals() else 'not available'}")
            import traceback
            print(f"DRIFT DEBUG: Full traceback: {traceback.format_exc()}")
            return DataFrame(columns=DEFAULT_DATAFRAME_COLUMNS)

    # TODO
    def get_order_book(self, pair: str, limit: int = 100) -> OrderBook:
        """
        Fetch order book from Drift.

        :param pair: Pair to fetch order book for
        :param limit: Number of levels to fetch
        :return: Order book dict
        """
        try:
            # In production, fetch actual order book from Drift
            # This is a placeholder implementation
            return {
                'symbol': pair,
                'bids': [],
                'asks': [],
                'timestamp': None,
                'datetime': None,
                'nonce': None,
            }
        except Exception as e:
            logger.error(f"Error fetching order book for {pair}: {e}")
            raise TemporaryError(f"Could not fetch order book: {e}") from e

    def create_order(
        self,
        *,
        pair: str,
        ordertype: str,
        side: str,
        amount: float,
        rate: float | None = None,
        leverage: float | None = None,
        reduceOnly: bool = False,
        time_in_force: str = "GTC",
        **kwargs
    ) -> dict[str, Any]:
        """
        Place an order on Drift protocol.

        :param pair: Pair to trade
        :param ordertype: Order type (limit, market, etc.)
        :param side: Buy or sell
        :param amount: Amount to trade
        :param rate: Price rate (for limit orders)
        :param leverage: Leverage to use
        :param reduceOnly: Whether this is a reduce-only order
        :param time_in_force: Time in force for the order
        :return: Order creation response
        """
        try:
            # DRY-RUN PATH
            # In dry-run, VulcanTrader expects exchanges to be able to "place" orders without
            # requiring exchange-specific private SDKs / credentials.
            # Drift's live connector would use `driftpy`, but that should NOT be a dependency
            # for dry-run testing.
            if self._config.get("dry_run", False):
                now_ms = int(datetime.now(tz=UTC).timestamp() * 1000)
                # For market orders or missing rate, use the current pricing logic.
                # This respects `entry_pricing.use_order_book` which we implemented via DLOB.
                price = float(rate) if rate is not None else float(
                    self.get_rate(pair, side="entry" if side ==
                                  "buy" else "exit", refresh=True)
                )

                # Create a deterministic, ccxt-like order structure compatible with VulcanTrader.
                order_id = f"dryrun_drift_{uuid.uuid4().hex[:12]}"
                return {
                    "id": order_id,
                    "clientOrderId": order_id,
                    "timestamp": now_ms,
                    "datetime": datetime.fromtimestamp(now_ms / 1000, tz=UTC).isoformat(),
                    "status": "closed",  # dry-run orders are considered immediately filled
                    "symbol": pair,
                    "type": ordertype,
                    "side": side,
                    "price": price,
                    "average": price,
                    "amount": amount,
                    "filled": amount,
                    "remaining": 0.0,
                    "cost": price * amount,
                    "fee": None,
                    "fees": [],
                    "trades": [],
                    "info": {
                        "exchange": "drift",
                        "dry_run": True,
                        "reduceOnly": reduceOnly,
                        "time_in_force": time_in_force,
                        "leverage": leverage,
                    },
                }

            # Convert to Drift market format
            market = self._convert_pair_to_drift_market(
                pair, CandleType.FUTURES)

            # Determine market index (would be fetched from Drift in production)
            market_index = self._get_market_index(market)

            # Convert amount to Drift precision
            base_amount = int(amount * self.BASE_PRECISION)

            # Convert price to Drift precision if limit order
            limit_price = None
            if ordertype == 'limit' and rate:
                limit_price = int(rate * self.PRICE_PRECISION)

            # LIVE PATH (requires driftpy)
            # NOTE: This is not currently used for dry-run.
            from driftpy.types import PositionDirection, OrderType
            direction = PositionDirection.LONG() if side == 'buy' else PositionDirection.SHORT()

            # Map order type
            order_type_map = {
                'market': OrderType.MARKET,
                'limit': OrderType.LIMIT,
            }
            drift_order_type = order_type_map.get(ordertype, OrderType.LIMIT)()

            # In production, this would call the actual Drift client to place the order
            # For now, return a mock order response
            order_response = {
                'id': f"drift_order_{pair}_{side}_{amount}",
                'info': {
                    'market_index': market_index,
                    'direction': direction.name,
                    'base_amount': base_amount,
                    'order_type': drift_order_type.name,
                },
                'timestamp': int(datetime.now(tz=UTC).timestamp() * 1000),
                'datetime': datetime.now(tz=UTC).isoformat(),
                'status': 'open',
                'symbol': pair,
                'type': ordertype,
                'side': side,
                'price': rate,
                'amount': amount,
                'filled': 0,
                'remaining': amount,
                'average': None,
                'fee': None,
                'fees': [],
                'cost': 0,
                'trades': [],
            }

            return order_response

        except Exception as e:
            logger.error(f"Error creating order for {pair}: {e}")
            raise OperationalException(f"Could not create order: {e}") from e

    def _get_market_index(self, market: str) -> int:
        """
        Get Drift market index for a given market symbol.

        :param market: Market symbol (e.g., "BTC-PERP")
        :return: Market index
        """
        # In production, this would fetch from Drift protocol
        # These are example indices
        market_indices = {
            "BTC-PERP": 0,
            "ETH-PERP": 1,
            "SOL-PERP": 2,
            "BTC-USDC": 100,  # Spot markets start at 100
            "ETH-USDC": 101,
            "SOL-USDC": 102,
        }
        return market_indices.get(market, 0)

    def cancel_order(self, order_id: str, pair: str, params: dict = {}) -> dict:
        """
        Cancel an order on Drift.

        :param order_id: Order ID to cancel
        :param pair: Pair the order is for
        :param params: Additional parameters
        :return: Cancellation response
        """
        try:
            # In production, call Drift client to cancel order
            return {
                'id': order_id,
                'info': {'cancelled': True},
                'status': 'canceled',
            }
        except Exception as e:
            logger.error(f"Error canceling order {order_id}: {e}")
            raise OperationalException(f"Could not cancel order: {e}") from e

    def get_order(self, order_id: str, pair: str, params: dict = {}) -> dict:
        """
        Fetch order details from Drift.

        :param order_id: Order ID to fetch
        :param pair: Pair the order is for
        :param params: Additional parameters
        :return: Order details
        """
        try:
            # In production, fetch actual order details from Drift
            return {
                'id': order_id,
                'symbol': pair,
                'status': 'closed',
                'filled': 0,
                'remaining': 0,
                'average': 0,
                'info': {}
            }
        except Exception as e:
            logger.error(f"Error fetching order {order_id}: {e}")
            raise TemporaryError(f"Could not fetch order: {e}") from e

    def fetch_funding_rate_history(
        self,
        pair: str,
        limit: int | None = None,
        since: int | None = None,
    ) -> list:
        """
        Fetch funding rate history from Drift Protocol.

        :param pair: Trading pair (e.g., "BTC-PERP")
        :param limit: Maximum number of funding rate records to return
        :param since: Timestamp in milliseconds to start from
        :return: List of funding rate dictionaries
        """
        try:
            # Convert pair to Drift market format
            market = self._convert_pair_to_drift_market(
                pair, CandleType.FUTURES)

            # Determine date range
            if since:
                start_date = datetime.fromtimestamp(since / 1000, tz=UTC)
            else:
                # Default to last 30 days if no since provided
                start_date = datetime.now(tz=UTC) - timedelta(days=30)

            end_date = datetime.now(tz=UTC)

            # Download funding rate data
            funding_rates = self._download_drift_funding_rates(
                market=market,
                start_date=start_date,
                end_date=end_date,
                limit=limit
            )

            logger.info(
                f"Fetched {len(funding_rates)} funding rate records for {pair}")
            return funding_rates

        except Exception as e:
            logger.error(
                f"Error fetching funding rate history for {pair}: {e}")
            return []

    def _download_drift_funding_rates(
        self,
        market: str,
        start_date: datetime,
        end_date: datetime,
        limit: int | None = None
    ) -> list:
        """
        Download funding rate data from Drift Protocol API using AsyncHTTPDownloader.

        :param market: Drift market identifier (e.g., "BTC-PERP")
        :param start_date: Start date for funding rate data
        :param end_date: End date for funding rate data
        :param limit: Maximum number of records to return
        :return: List of funding rate dictionaries
        """
        try:
            # Generate date range
            dates = []
            current_date = start_date
            while current_date <= end_date:
                dates.append(current_date)
                current_date += timedelta(days=1)

            if not dates:
                logger.warning("No dates in range for funding rate download")
                return []

            logger.info(
                f"Downloading funding rates for {market} from {start_date.date()} to {end_date.date()}")
            logger.info(f"Total days to download: {len(dates)}")

            # Run async download
            try:
                # Get data directory from config
                datadir_config = self._config.get(
                    'datadir', 'user_data/data/drift')

                # Use conservative settings for funding rate downloads
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                result_funding_rates = loop.run_until_complete(
                    self._async_download_drift_funding_rates(
                        "https://data.api.drift.trade", market, dates, limit, datadir_config
                    )
                )
                loop.close()
            except RuntimeError:
                # Event loop already running, use existing loop
                result_funding_rates = asyncio.run(
                    self._async_download_drift_funding_rates(
                        "https://data.api.drift.trade", market, dates, limit,
                        self._config.get('datadir', 'user_data/data/drift')
                    )
                )

            if not result_funding_rates:
                logger.warning(f"No funding rate data downloaded for {market}")
                return []

            # Sort by timestamp (newest first, matching VulcanTrader convention)
            result_funding_rates.sort(
                key=lambda x: x['timestamp'], reverse=True)

            logger.info(
                f"Downloaded {len(result_funding_rates)} total funding rate records for {market}")
            return result_funding_rates

        except Exception as e:
            logger.error(f"Error downloading funding rates for {market}: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return []

    async def _async_download_drift_funding_rates(
        self,
        base_url: str,
        market: str,
        dates: list[datetime],
        limit: int | None = None,
        data_dir: str = None
    ) -> list:
        """
        Async function to download funding rate data using AsyncHTTPDownloader.

        :param base_url: Base URL for Drift API
        :param market: Market identifier
        :param dates: List of dates to download
        :param limit: Maximum number of records to return
        :param data_dir: Data directory path
        :return: List of funding rate dictionaries
        """
        # Use very conservative settings for funding rate API to avoid 403 errors
        max_concurrent = 1  # Reduce to 1 concurrent request for funding rates

        async with AsyncHTTPDownloader(
            max_concurrent=max_concurrent,
            retry_delay=30,  # Increased retry delay for rate limiting (was 20)
            max_retries=3,
            request_timeout=30,
            data_dir=data_dir
        ) as downloader:
            funding_rates, successful_days, failed_days, empty_days = await downloader.download_funding_rates_date_range(
                base_url, market, dates, limit
            )

            logger.info(
                f"Funding rate download complete: {successful_days} successful, {empty_days} empty, {failed_days} failed")

            if not funding_rates:
                logger.warning(
                    "No funding rate data was successfully downloaded")
                return []

            logger.info(
                f"Processing {len(funding_rates)} funding rate records...")
            return funding_rates

    def calculate_funding_fees(
        self,
        df: DataFrame,
        amount: float,
        is_short: bool,
        open_date: datetime,
        close_date: datetime,
        time_in_ratio: float | None = None,
    ) -> float:
        """
        Calculates the sum of all funding fees that occurred for a pair during a futures trade.

        Override the base method to fix funding fee calculation for Drift Protocol.
        The issue with the base method is that it assumes amount is already in quote currency,
        but in our case amount represents the stake amount (position value) in quote currency.

        :param df: Dataframe containing combined funding and mark rates
                   as `open_fund` and `open_mark`.
        :param amount: The stake amount (position value in quote currency)
        :param is_short: trade direction
        :param open_date: The date and time that the trade started
        :param close_date: The date and time that the trade ended
        :param time_in_ratio: Not used
        """
        try:
            fees: float = 0.0

            if not df.empty:
                df1 = df[(df["date"] >= open_date) &
                         (df["date"] <= close_date)]

                for idx, row in df1.iterrows():
                    funding_rate = row["open_fund"]
                    mark_price = row["open_mark"]

                    # For Drift Protocol simulation:
                    # - Use a small funding rate (typical rates are 0.01% = 0.0001)
                    # - Amount is already the position value in quote currency
                    # - No need to multiply by mark_price again
                    simulated_funding_rate = 0.0001  # 0.01% funding rate

                    if is_short:
                        # Short positions receive funding when rate is positive
                        period_fee = amount * simulated_funding_rate
                    else:
                        # Long positions pay funding when rate is positive
                        period_fee = -amount * simulated_funding_rate

                    fees += period_fee

            if isnan(fees):
                fees = 0.0

            return fees

        except Exception as e:
            logger.error(f"Error calculating Drift funding fees: {e}")
            return 0.0

    def get_funding_fees(
        self, pair: str, amount: float, is_short: bool, open_date: datetime
    ) -> float:
        """
        Calculate funding fees for a position on Drift Protocol.

        :param pair: Trading pair
        :param amount: Position size in base currency
        :param is_short: True if short position, False if long
        :param open_date: When the position was opened
        :return: Total funding fees paid/received
        """
        try:
            logger.debug(
                f"FUNDING FEE CALCULATION: Starting for {pair}, amount={amount}, is_short={is_short}, open_date={open_date}")

            if self.trading_mode != TradingMode.FUTURES:
                logger.debug("FUNDING FEE: Not futures mode, returning 0.0")
                return 0.0

            # Fetch funding rate history since position was opened
            since_ms = int(open_date.timestamp() * 1000)
            funding_history = self.fetch_funding_rate_history(
                pair, since=since_ms)

            logger.debug(
                f"FUNDING FEE: Got {len(funding_history) if funding_history else 0} funding records since {since_ms}")

            if not funding_history:
                logger.warning(f"No funding rate history available for {pair}")
                return 0.0

            total_funding = 0.0
            position_value = 0.0  # Will be calculated from mark prices

            for i, funding_record in enumerate(funding_history):
                logger.debug(
                    f"FUNDING FEE: Processing record {i+1}/{len(funding_history)}")

                # Skip records before position was opened
                if funding_record['timestamp'] < since_ms:
                    logger.debug(
                        f"FUNDING FEE: Skipping record {i+1} - before position open")
                    continue

                # Use appropriate funding rate based on position direction
                if is_short:
                    funding_rate = funding_record.get(
                        'fundingRateShort', funding_record['fundingRate'])
                else:
                    funding_rate = funding_record.get(
                        'fundingRateLong', funding_record['fundingRate'])

                # For futures trading, amount is already the position value in the quote currency
                # No need to multiply by mark price as that would double-convert
                # The amount parameter represents the stake value (e.g., $100 USDC position)

                # DEBUG: Log funding calculation details
                logger.debug(
                    f"FUNDING DEBUG: record {i+1} - funding_rate={funding_rate}, amount={amount}, is_short={is_short}")

                # Calculate funding fee for this period
                # Funding fee = position_value * funding_rate * (funding_rate_factor)
                # For simulation purposes, use a small funding rate (0.01% = 0.0001)
                simulated_funding_rate = funding_rate * 0.0001 if funding_rate != 0 else 0.0001

                # The funding_fees field represents NET BENEFIT to the trade:
                # Positive = trade gained from fees
                # Negative = trade paid fees

                if is_short:
                    # For short positions:
                    # - When funding_rate > 0: shorts RECEIVE funding (benefit = positive)
                    # - When funding_rate < 0: shorts PAY funding (benefit = negative)
                    funding_fee = amount * simulated_funding_rate
                else:
                    # For long positions:
                    # - When funding_rate > 0: longs PAY funding (benefit = negative)
                    # - When funding_rate < 0: longs RECEIVE funding (benefit = positive)
                    funding_fee = -amount * simulated_funding_rate
                logger.debug(
                    f"FUNDING DEBUG: record {i+1} - calculated funding_fee={funding_fee} (simulated_rate={simulated_funding_rate})")
                total_funding += funding_fee

            logger.debug(
                f"FUNDING FEE: Final total_funding={total_funding}")
            logger.debug(
                f"Calculated total funding fees for {pair}: {total_funding}")
            return total_funding

        except Exception as e:
            logger.error(f"Error calculating funding fees for {pair}: {e}")
            return 0.0

    def get_max_leverage(self, pair: str, stake_amount: float | None) -> float:
        """
        Get maximum leverage for a pair on Drift.

        :param pair: Pair to get max leverage for
        :param stake_amount: Stake amount (unused on Drift)
        :return: Maximum leverage
        """
        if self.trading_mode == TradingMode.FUTURES:
            # Drift typically supports up to 10x leverage
            # In production, this would be fetched from market data
            return 10.0
        return 1.0

    def get_maintenance_ratio_and_amt(self, pair: str, nominal_value: float) -> tuple[float, float]:
        """
        Get maintenance margin ratio and amount for a pair.

        :param pair: Pair to get maintenance info for
        :param nominal_value: Nominal position value
        :return: Tuple of (maintenance_ratio, maintenance_amount)
        """
        # Drift uses a simple maintenance margin model
        # Typically 5% for most markets
        maintenance_ratio = 0.05
        maintenance_amount = nominal_value * maintenance_ratio

        return maintenance_ratio, maintenance_amount

    async def place_perp_order(
        self,
        market_index: int,
        direction: str,
        base_asset_amount: float,
        order_type: str = "limit",
        limit_price: float = None,
        reduce_only: bool = False,
        post_only: bool = False,
        immediate_or_cancel: bool = False,
        **kwargs
    ) -> Optional[str]:
        """
        Place a perpetual futures order on Drift.

        :param market_index: Market index for the perp market
        :param direction: 'long' or 'short'
        :param base_asset_amount: Amount in base asset
        :param order_type: 'market' or 'limit'
        :param limit_price: Price for limit orders
        :param reduce_only: Whether order should only reduce position
        :param post_only: Whether order should only be maker
        :param immediate_or_cancel: IOC order type
        :return: Order signature/ID if successful
        """
        try:
            # Convert amounts to Drift precision
            base_amount_raw = int(base_asset_amount * self.BASE_PRECISION)
            limit_price_raw = int(
                limit_price * self.PRICE_PRECISION) if limit_price else None

            # Determine order parameters
            order_params = {
                'market_index': market_index,
                'direction': direction,
                'base_asset_amount': base_amount_raw,
                'order_type': order_type,
                'reduce_only': reduce_only,
                'post_only': post_only,
                'immediate_or_cancel': immediate_or_cancel,
            }

            if limit_price_raw:
                order_params['price'] = limit_price_raw

            # In production, this would call the actual Drift SDK
            # For now, return a mock order ID
            order_id = f"drift_perp_{market_index}_{direction}_{base_asset_amount}"

            logger.info(f"Placed perp order: {order_id}")
            return order_id

        except Exception as e:
            logger.error(f"Error placing perp order: {e}")
            return None

    async def get_perp_position(self, market_index: int) -> Optional[dict]:
        """
        Get current perpetual position for a market.

        :param market_index: Market index to query
        :return: Position details or None
        """
        try:
            # In production, fetch from Drift protocol
            # Mock response for now
            return {
                'market_index': market_index,
                'base_asset_amount': 0,
                'quote_asset_amount': 0,
                'pnl': 0,
                'funding_payment': 0,
                'entry_price': 0,
            }
        except Exception as e:
            logger.error(f"Error fetching perp position: {e}")
            return None

    def populate_orderflow_indicators(self, dataframe: DataFrame, pair: str) -> DataFrame:
        """
        Populate orderflow indicators for Drift data.
        This method can be called from a strategy to add orderflow data.

        Note: For standard orderflow functionality, it's recommended to use 
        VulcanTrader's built-in orderflow system by enabling 'use_public_trades' 
        in the exchange configuration. This method provides additional 
        Drift-specific indicators.

        :param dataframe: OHLCV dataframe
        :param pair: Trading pair
        :return: Dataframe with orderflow indicators
        """
        try:
            if len(dataframe) < 2:
                return dataframe

            # Check if orderflow data is already available (from standard system)
            has_orderflow = any(col in dataframe.columns for col in
                                ['trades', 'orderflow', 'delta', 'bid', 'ask'])

            if not has_orderflow:
                logger.info(f"Standard orderflow data not found for {pair}. "
                            f"Consider enabling 'use_public_trades' in exchange config for full orderflow support.")

                # Fetch basic orderflow data using Drift-specific method
                time_diff = dataframe['date'].iloc[1] - \
                    dataframe['date'].iloc[0]
                timeframe = f"{int(time_diff.total_seconds() / 60)}m"
                since_ms = int(dataframe['date'].iloc[0].timestamp() * 1000)

                # Get trades data for orderflow calculation
                trades_df = self.get_orderflow_data(
                    pair=pair,
                    timeframe=timeframe,
                    since_ms=since_ms,
                    candle_type=CandleType.FUTURES if self.trading_mode == TradingMode.FUTURES else CandleType.SPOT
                )

                if not trades_df.empty:
                    # Use VulcanTrader's standard orderflow processor
                    from VulcanTrader.data.converter import populate_dataframe_with_trades

                    # Create a basic orderflow config
                    orderflow_config = {
                        "timeframe": timeframe,
                        "orderflow": {
                            "cache_size": 1000,
                            "max_candles": len(dataframe),
                            "scale": 0.01,  # Price scale for orderflow binning
                            "imbalance_ratio": 3,
                            "imbalance_volume": 0,
                            "stacked_imbalance_range": 3,
                        }
                    }

                    # Process orderflow data
                    dataframe, _ = populate_dataframe_with_trades(
                        None, orderflow_config, dataframe, trades_df
                    )

            # Add Drift-specific orderflow indicators if we have the base data
            if 'delta' in dataframe.columns:
                # Delta momentum indicators
                dataframe['delta_ma_5'] = dataframe['delta'].rolling(
                    window=5).mean()
                dataframe['delta_ma_20'] = dataframe['delta'].rolling(
                    window=20).mean()
                dataframe['delta_momentum'] = dataframe['delta_ma_5'] - \
                    dataframe['delta_ma_20']

                # Cumulative delta analysis
                if 'cumulative_delta' not in dataframe.columns:
                    dataframe['cumulative_delta'] = dataframe['delta'].cumsum()

                dataframe['cum_delta_change'] = dataframe['cumulative_delta'].diff()
                dataframe['cum_delta_slope'] = dataframe['cumulative_delta'].rolling(window=5).apply(
                    lambda x: (x.iloc[-1] - x.iloc[0]) / len(x) if len(x) > 1 else 0, raw=False
                )

            if 'total_volume' in dataframe.columns:
                # Volume profile analysis
                dataframe['volume_ma'] = dataframe['total_volume'].rolling(
                    window=20).mean()
                dataframe['volume_ratio'] = dataframe['total_volume'] / \
                    dataframe['volume_ma']
                dataframe['volume_breakout'] = (
                    dataframe['volume_ratio'] > 2.0).astype(int)

            if 'buy_volume' in dataframe.columns and 'sell_volume' in dataframe.columns:
                # Buy/sell pressure indicators
                total_volume = dataframe['buy_volume'] + \
                    dataframe['sell_volume']
                dataframe['buy_pressure'] = dataframe['buy_volume'] / \
                    total_volume.replace(0, 1)
                dataframe['sell_pressure'] = dataframe['sell_volume'] / \
                    total_volume.replace(0, 1)

                # Pressure momentum
                dataframe['pressure_momentum'] = (
                    dataframe['buy_pressure'].rolling(window=5).mean() -
                    dataframe['sell_pressure'].rolling(window=5).mean()
                )

            logger.info(f"Added Drift orderflow indicators to {pair}")

        except Exception as e:
            logger.error(f"Error adding orderflow indicators: {e}")

        return dataframe
