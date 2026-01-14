"""
Nifty 500 Extractor Module
Extracts 5-minute OHLCV data for Nifty 500 stocks from Zerodha Kite API
"""
import os
import logging
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from functools import lru_cache

from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential

load_dotenv()

logger = logging.getLogger(__name__)

# Nifty 500 constituents - fetched from NSE or cached
# This list can be updated periodically or fetched dynamically
NIFTY500_SYMBOLS_URL = "https://archives.nseindia.com/content/indices/ind_nifty500list.csv"


class Nifty500Extractor:
    """Extracts 5-minute OHLCV data for Nifty 500 stocks from Kite API."""
    
    # Rate limiting: ~3 requests per second
    REQUEST_DELAY = 0.35  # seconds between requests
    
    def __init__(self, api_key: str = None, access_token: str = None):
        """
        Initialize Nifty 500 extractor.
        
        Args:
            api_key: Kite API key (defaults to env var KITE_API_KEY)
            access_token: Kite access token (defaults to env var KITE_ACCESS_TOKEN)
        """
        self.api_key = api_key or os.getenv("KITE_API_KEY")
        self.access_token = access_token or os.getenv("KITE_ACCESS_TOKEN")
        self._kite = None
        self._instruments_cache = None
    
    @property
    def kite(self):
        """Lazy load KiteConnect client."""
        if self._kite is None:
            from kiteconnect import KiteConnect
            self._kite = KiteConnect(api_key=self.api_key)
            self._kite.set_access_token(self.access_token)
        return self._kite
    
    def get_nse_instruments(self) -> List[Dict]:
        """
        Fetch all NSE equity instruments.
        
        Returns:
            List of NSE equity instrument dictionaries
        """
        if self._instruments_cache is None:
            logger.info("Fetching NSE instruments from Kite API")
            all_instruments = self.kite.instruments("NSE")
            # Filter for equity segment only
            self._instruments_cache = [
                inst for inst in all_instruments
                if inst.get("segment") == "NSE" and inst.get("instrument_type") == "EQ"
            ]
            logger.info(f"Cached {len(self._instruments_cache)} NSE equity instruments")
        return self._instruments_cache
    
    def get_nifty500_instruments(self, symbols: List[str] = None) -> List[Dict]:
        """
        Get instrument details for Nifty 500 stocks.
        
        Args:
            symbols: Optional list of trading symbols. If None, fetches all Nifty 500.
            
        Returns:
            List of instrument dictionaries for Nifty 500 stocks
        """
        nse_instruments = self.get_nse_instruments()
        
        if symbols is None:
            symbols = self._get_nifty500_symbols()
        
        # Create symbol to instrument mapping
        symbol_map = {inst["tradingsymbol"]: inst for inst in nse_instruments}
        
        nifty500_instruments = []
        missing = []
        for symbol in symbols:
            if symbol in symbol_map:
                nifty500_instruments.append(symbol_map[symbol])
            else:
                missing.append(symbol)
        
        if missing:
            logger.warning(f"Could not find instruments for {len(missing)} symbols: {missing[:10]}...")
        
        logger.info(f"Found {len(nifty500_instruments)} Nifty 500 instruments")
        return nifty500_instruments
    
    def _get_nifty500_symbols(self) -> List[str]:
        """
        Get list of Nifty 500 trading symbols.
        
        Returns:
            List of trading symbols
        """
        # Try to fetch from NSE, fallback to hardcoded top stocks
        try:
            import pandas as pd
            df = pd.read_csv(NIFTY500_SYMBOLS_URL)
            symbols = df["Symbol"].tolist()
            logger.info(f"Fetched {len(symbols)} Nifty 500 symbols from NSE")
            return symbols
        except Exception as e:
            logger.warning(f"Failed to fetch Nifty 500 list from NSE: {e}")
            # Fallback to some major stocks for testing
            return self._get_fallback_symbols()
    
    def _get_fallback_symbols(self) -> List[str]:
        """Fallback list of major Nifty stocks for testing."""
        return [
            "RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK",
            "HINDUNILVR", "SBIN", "BHARTIARTL", "KOTAKBANK", "ITC",
            "LT", "AXISBANK", "BAJFINANCE", "ASIANPAINT", "MARUTI",
            "TITAN", "SUNPHARMA", "ULTRACEMCO", "WIPRO", "HCLTECH"
        ]
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def extract_5min_ohlcv(
        self,
        instrument_token: int,
        from_date: datetime,
        to_date: datetime
    ) -> List[Dict]:
        """
        Extract 5-minute OHLCV data for a single instrument.
        
        Args:
            instrument_token: Kite instrument token
            from_date: Start datetime
            to_date: End datetime
            
        Returns:
            List of OHLCV dictionaries
        """
        try:
            data = self.kite.historical_data(
                instrument_token=instrument_token,
                from_date=from_date,
                to_date=to_date,
                interval="5minute"
            )
            return data
        except Exception as e:
            logger.error(f"Failed to extract data for token {instrument_token}: {e}")
            raise
    
    def extract_daily_data(
        self,
        trade_date: datetime,
        symbols: List[str] = None,
        max_instruments: int = None
    ) -> Tuple[Dict[str, List[Dict]], List[str]]:
        """
        Extract full day's 5-minute data for Nifty 500 stocks.
        
        Args:
            trade_date: Trading date
            symbols: Optional list of symbols to extract. If None, uses all Nifty 500.
            max_instruments: Optional limit on number of instruments (for testing)
            
        Returns:
            Tuple of (data_dict, failed_symbols)
            - data_dict: Dict mapping tradingsymbol to list of candle data
            - failed_symbols: List of symbols that failed extraction
        """
        instruments = self.get_nifty500_instruments(symbols)
        
        if max_instruments:
            instruments = instruments[:max_instruments]
            logger.info(f"Limited to {max_instruments} instruments for testing")
        
        # Define market hours (9:15 AM to 3:30 PM IST)
        from_dt = trade_date.replace(hour=9, minute=15, second=0, microsecond=0)
        to_dt = trade_date.replace(hour=15, minute=30, second=0, microsecond=0)
        
        results = {}
        failed = []
        total = len(instruments)
        
        logger.info(f"Starting extraction for {total} instruments on {trade_date.date()}")
        
        for idx, inst in enumerate(instruments, 1):
            symbol = inst["tradingsymbol"]
            token = inst["instrument_token"]
            
            try:
                data = self.extract_5min_ohlcv(token, from_dt, to_dt)
                
                if data:
                    # Enrich with instrument metadata
                    for candle in data:
                        candle["tradingsymbol"] = symbol
                        candle["instrument_token"] = token
                        candle["exchange"] = "NSE"
                    
                    results[symbol] = data
                    
                if idx % 50 == 0:
                    logger.info(f"Progress: {idx}/{total} instruments extracted")
                    
            except Exception as e:
                logger.error(f"Failed to extract {symbol}: {e}")
                failed.append(symbol)
            
            # Rate limiting
            time.sleep(self.REQUEST_DELAY)
        
        logger.info(f"Extraction complete: {len(results)} succeeded, {len(failed)} failed")
        return results, failed
    
    def extract_single_stock(
        self,
        symbol: str,
        trade_date: datetime
    ) -> Optional[List[Dict]]:
        """
        Extract data for a single stock (convenience method).
        
        Args:
            symbol: Trading symbol (e.g., "RELIANCE")
            trade_date: Trading date
            
        Returns:
            List of candle data or None if failed
        """
        instruments = self.get_nifty500_instruments([symbol])
        
        if not instruments:
            logger.error(f"Instrument not found: {symbol}")
            return None
        
        inst = instruments[0]
        from_dt = trade_date.replace(hour=9, minute=15, second=0, microsecond=0)
        to_dt = trade_date.replace(hour=15, minute=30, second=0, microsecond=0)
        
        try:
            data = self.extract_5min_ohlcv(inst["instrument_token"], from_dt, to_dt)
            for candle in data:
                candle["tradingsymbol"] = symbol
                candle["instrument_token"] = inst["instrument_token"]
                candle["exchange"] = "NSE"
            return data
        except Exception as e:
            logger.error(f"Failed to extract {symbol}: {e}")
            return None
