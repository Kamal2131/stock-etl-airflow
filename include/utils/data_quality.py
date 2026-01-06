"""
Data Quality Checks Module
Validates data integrity and quality for F&O ETL pipeline
"""
import logging
from datetime import datetime
from typing import List, Tuple
import pandas as pd

logger = logging.getLogger(__name__)


class DataQualityChecker:
    """Performs data quality validations on F&O data."""
    
    def __init__(self, df: pd.DataFrame):
        """
        Initialize checker with DataFrame.
        
        Args:
            df: DataFrame to validate
        """
        self.df = df
        self.errors: List[str] = []
        self.warnings: List[str] = []
    
    def run_all_checks(self) -> Tuple[bool, List[str], List[str]]:
        """
        Run all quality checks.
        
        Returns:
            Tuple of (passed, errors, warnings)
        """
        self.check_not_empty()
        self.check_no_null_timestamps()
        self.check_unique_symbol_timestamps()
        self.check_market_hours()
        self.check_price_validity()
        
        passed = len(self.errors) == 0
        return passed, self.errors, self.warnings
    
    def check_not_empty(self) -> bool:
        """Check that DataFrame is not empty."""
        if self.df.empty:
            self.errors.append("DataFrame is empty")
            return False
        logger.info(f"Row count check passed: {len(self.df)} rows")
        return True
    
    def check_no_null_timestamps(self) -> bool:
        """Check for null timestamps."""
        if "timestamp" not in self.df.columns:
            self.errors.append("Missing 'timestamp' column")
            return False
            
        null_count = self.df["timestamp"].isnull().sum()
        if null_count > 0:
            self.errors.append(f"Found {null_count} null timestamps")
            return False
        return True
    
    def check_unique_symbol_timestamps(self) -> bool:
        """Check for duplicate (symbol, timestamp) pairs."""
        if "tradingsymbol" not in self.df.columns or "timestamp" not in self.df.columns:
            return True
            
        duplicates = self.df.duplicated(subset=["tradingsymbol", "timestamp"]).sum()
        if duplicates > 0:
            self.warnings.append(f"Found {duplicates} duplicate (symbol, timestamp) pairs")
            return False
        return True
    
    def check_market_hours(self) -> bool:
        """Check that all data is within market hours."""
        if "timestamp" not in self.df.columns:
            return True
            
        times = self.df["timestamp"].dt.time
        market_open = pd.Timestamp("09:15").time()
        market_close = pd.Timestamp("15:30").time()
        
        outside_hours = ((times < market_open) | (times > market_close)).sum()
        if outside_hours > 0:
            self.warnings.append(f"{outside_hours} records outside market hours")
        return True
    
    def check_price_validity(self) -> bool:
        """Check for invalid prices (negative, zero, or extreme values)."""
        price_cols = ["open", "high", "low", "close"]
        
        for col in price_cols:
            if col not in self.df.columns:
                continue
                
            negative = (self.df[col] < 0).sum()
            if negative > 0:
                self.errors.append(f"Found {negative} negative values in {col}")
                
            zero = (self.df[col] == 0).sum()
            if zero > 0:
                self.warnings.append(f"Found {zero} zero values in {col}")
        
        # Check OHLC logic
        if all(col in self.df.columns for col in price_cols):
            invalid_high = (self.df["high"] < self.df["low"]).sum()
            if invalid_high > 0:
                self.errors.append(f"Found {invalid_high} rows where high < low")
        
        return len(self.errors) == 0


def validate_dataframe(df: pd.DataFrame) -> bool:
    """
    Convenience function to validate a DataFrame.
    
    Args:
        df: DataFrame to validate
        
    Returns:
        True if all checks pass
        
    Raises:
        ValueError if critical checks fail
    """
    checker = DataQualityChecker(df)
    passed, errors, warnings = checker.run_all_checks()
    
    for warning in warnings:
        logger.warning(f"Quality Warning: {warning}")
    
    if not passed:
        for error in errors:
            logger.error(f"Quality Error: {error}")
        raise ValueError(f"Data quality checks failed: {errors}")
    
    logger.info("All data quality checks passed")
    return True
