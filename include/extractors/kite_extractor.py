"""
Kite API Extractor Module
Extracts 1-minute F&O OHLCV + OI data from Zerodha Kite API
"""
import os
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class KiteExtractor:
    """Extracts F&O market data from Zerodha Kite API."""
    
    def __init__(self, api_key: str = None, access_token: str = None):
        """
        Initialize Kite extractor.
        
        Args:
            api_key: Kite API key (defaults to env var KITE_API_KEY)
            access_token: Kite access token (defaults to env var KITE_ACCESS_TOKEN)
        """
        self.api_key = api_key or os.getenv("KITE_API_KEY")
        self.access_token = access_token or os.getenv("KITE_ACCESS_TOKEN")
        self._kite = None
    
    @property
    def kite(self):
        """Lazy load KiteConnect client."""
        if self._kite is None:
            from kiteconnect import KiteConnect
            self._kite = KiteConnect(api_key=self.api_key)
            self._kite.set_access_token(self.access_token)
        return self._kite
    
    def get_fno_instruments(self, underlying: str = "BANKNIFTY") -> List[Dict]:
        """
        Fetch NFO instruments for given underlying.
        
        Args:
            underlying: Underlying symbol (BANKNIFTY, NIFTY, etc.)
            
        Returns:
            List of instrument dictionaries
        """
        logger.info(f"Fetching NFO instruments for {underlying}")
        instruments = self.kite.instruments("NFO")
        
        # Filter for the specific underlying
        filtered = [
            inst for inst in instruments 
            if inst.get("name") == underlying
        ]
        
        logger.info(f"Found {len(filtered)} instruments for {underlying}")
        return filtered
    
    def extract_historical_data(
        self,
        instrument_token: int,
        from_date: datetime,
        to_date: datetime,
        interval: str = "minute"
    ) -> List[Dict]:
        """
        Extract historical OHLCV data for an instrument.
        
        Args:
            instrument_token: Kite instrument token
            from_date: Start datetime
            to_date: End datetime  
            interval: Candle interval (minute, 3minute, 5minute, etc.)
            
        Returns:
            List of OHLCV dictionaries
        """
        try:
            data = self.kite.historical_data(
                instrument_token=instrument_token,
                from_date=from_date,
                to_date=to_date,
                interval=interval,
                oi=True  # Include Open Interest
            )
            logger.info(f"Extracted {len(data)} candles for token {instrument_token}")
            return data
        except Exception as e:
            logger.error(f"Failed to extract data for {instrument_token}: {e}")
            return []
    
    def extract_daily_fno_data(
        self,
        underlying: str,
        trade_date: datetime,
        expiry_filter: Optional[datetime] = None
    ) -> Dict[str, List[Dict]]:
        """
        Extract full day's 1-minute data for all F&O instruments of an underlying.
        
        Args:
            underlying: Underlying symbol
            trade_date: Trading date
            expiry_filter: Optional expiry date to filter instruments
            
        Returns:
            Dict mapping tradingsymbol to list of candle data
        """
        instruments = self.get_fno_instruments(underlying)
        
        # Filter by expiry if provided
        if expiry_filter:
            instruments = [
                inst for inst in instruments
                if inst.get("expiry") == expiry_filter.date()
            ]
        
        # Define market hours (9:15 AM to 3:30 PM IST)
        from_dt = trade_date.replace(hour=9, minute=15, second=0, microsecond=0)
        to_dt = trade_date.replace(hour=15, minute=30, second=0, microsecond=0)
        
        results = {}
        for inst in instruments:
            symbol = inst["tradingsymbol"]
            token = inst["instrument_token"]
            
            data = self.extract_historical_data(token, from_dt, to_dt)
            if data:
                # Enrich with instrument metadata
                for candle in data:
                    candle["tradingsymbol"] = symbol
                    candle["instrument_token"] = token
                    candle["underlying"] = underlying
                    candle["expiry"] = inst.get("expiry")
                    candle["strike"] = inst.get("strike")
                    candle["instrument_type"] = inst.get("instrument_type")
                    candle["lot_size"] = inst.get("lot_size")
                
                results[symbol] = data
                logger.info(f"Extracted {len(data)} candles for {symbol}")
        
        return results
