"""
Equity Data Transformer Module
Cleans, validates, and enriches raw equity OHLCV data (no OI/expiry like F&O)
"""
import logging
from datetime import datetime
from typing import List, Dict
import pandas as pd

logger = logging.getLogger(__name__)


class EquityTransformer:
    """Transforms raw equity OHLCV data into clean, analytics-ready format."""
    
    # Standard output schema for equity data
    SCHEMA = {
        "timestamp": "datetime64[ns]",
        "open": "float64",
        "high": "float64",
        "low": "float64",
        "close": "float64",
        "volume": "int64",
        "tradingsymbol": "string",
        "exchange": "string",
        "trade_date": "datetime64[ns]"
    }
    
    # Market hours (IST)
    MARKET_OPEN = (9, 15)   # 9:15 AM
    MARKET_CLOSE = (15, 30)  # 3:30 PM
    
    def __init__(self, trade_date: datetime):
        """
        Initialize transformer.
        
        Args:
            trade_date: The trading date for this data
        """
        self.trade_date = trade_date
    
    def transform(self, raw_data: List[Dict]) -> pd.DataFrame:
        """
        Transform raw candle data to clean DataFrame.
        
        Args:
            raw_data: List of raw candle dictionaries
            
        Returns:
            Cleaned and enriched DataFrame
        """
        if not raw_data:
            logger.warning("Empty data received, returning empty DataFrame")
            return pd.DataFrame()
        
        df = pd.DataFrame(raw_data)
        
        # Rename columns to standard schema
        df = self._rename_columns(df)
        
        # Convert data types
        df = self._convert_dtypes(df)
        
        # Validate and clean
        df = self._validate_timestamps(df)
        df = self._dedupe(df)
        df = self._filter_market_hours(df)
        df = self._validate_ohlc(df)
        
        # Add trade date
        df["trade_date"] = self.trade_date
        
        logger.info(f"Transformed {len(df)} records")
        return df
    
    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Rename columns to match standard schema."""
        column_map = {
            "date": "timestamp"
        }
        return df.rename(columns=column_map)
    
    def _convert_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert columns to appropriate data types."""
        # Timestamp
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
        
        # Numeric columns
        numeric_cols = ["open", "high", "low", "close", "volume"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        
        # Fill missing volume with 0
        if "volume" in df.columns:
            df["volume"] = df["volume"].fillna(0).astype("int64")
            
        return df
    
    def _validate_timestamps(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove rows with null or invalid timestamps."""
        before = len(df)
        df = df.dropna(subset=["timestamp"])
        after = len(df)
        
        if before > after:
            logger.warning(f"Dropped {before - after} rows with null timestamps")
        
        return df
    
    def _dedupe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate timestamps per symbol."""
        before = len(df)
        df = df.drop_duplicates(subset=["tradingsymbol", "timestamp"], keep="last")
        after = len(df)
        
        if before > after:
            logger.warning(f"Removed {before - after} duplicate rows")
        
        return df
    
    def _filter_market_hours(self, df: pd.DataFrame) -> pd.DataFrame:
        """Keep only market hours data."""
        if "timestamp" not in df.columns:
            return df
            
        market_open = df["timestamp"].dt.time >= pd.Timestamp(
            f"{self.MARKET_OPEN[0]:02d}:{self.MARKET_OPEN[1]:02d}"
        ).time()
        
        market_close = df["timestamp"].dt.time <= pd.Timestamp(
            f"{self.MARKET_CLOSE[0]:02d}:{self.MARKET_CLOSE[1]:02d}"
        ).time()
        
        before = len(df)
        df = df[market_open & market_close]
        after = len(df)
        
        if before > after:
            logger.info(f"Filtered {before - after} off-market records")
        
        return df
    
    def _validate_ohlc(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate OHLC constraints: high >= low, open/close between high/low."""
        before = len(df)
        
        # Basic OHLC validation
        valid_mask = (
            (df["high"] >= df["low"]) &
            (df["open"] <= df["high"]) &
            (df["open"] >= df["low"]) &
            (df["close"] <= df["high"]) &
            (df["close"] >= df["low"])
        )
        
        df = df[valid_mask]
        after = len(df)
        
        if before > after:
            logger.warning(f"Removed {before - after} rows with invalid OHLC values")
        
        return df
    
    def transform_batch(self, data_dict: Dict[str, List[Dict]]) -> pd.DataFrame:
        """
        Transform multiple symbols in batch.
        
        Args:
            data_dict: Dict mapping symbol to list of candles
            
        Returns:
            Combined DataFrame with all symbols
        """
        dfs = []
        for symbol, candles in data_dict.items():
            df = self.transform(candles)
            if not df.empty:
                dfs.append(df)
        
        if not dfs:
            return pd.DataFrame()
        
        combined = pd.concat(dfs, ignore_index=True)
        logger.info(f"Batch transformed {len(combined)} total records across {len(dfs)} symbols")
        return combined
