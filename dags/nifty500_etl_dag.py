"""
Nifty 500 ETL DAG - Daily Pipeline
Extracts, transforms, and loads 5-minute OHLCV data for Nifty 500 stocks to Parquet and S3.

Schedule: Daily at 4:00 PM IST (after market close) on trading days
Catchup: Enabled for backfill support
"""
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, List

from airflow.decorators import dag, task
from airflow.models import Variable

# Configure logging
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

# Configuration from environment
MAX_INSTRUMENTS = int(os.getenv("NIFTY500_MAX_INSTRUMENTS", "0"))  # 0 = all


@dag(
    dag_id="nifty500_etl_daily",
    default_args=default_args,
    description="Daily Nifty 500 5-minute OHLCV ETL pipeline to Parquet and S3",
    schedule="0 16 * * 1-5",  # 4:00 PM IST, Mon-Fri
    start_date=datetime(2025, 1, 1),
    catchup=True,
    max_active_runs=1,
    tags=["nifty500", "equity", "etl", "parquet", "s3", "production"],
)
def nifty500_etl_dag():
    """
    Nifty 500 ETL Pipeline DAG
    
    Stages:
    1. Extract: Pull 5-minute OHLCV from Kite API for Nifty 500 stocks
    2. Transform: Clean, validate, and enrich data
    3. Load Raw: Write to raw layer of Parquet lake
    4. Process: Additional cleaning for analytics
    5. Upload: Upload processed data to AWS S3
    6. Quality Check: Validate data integrity
    """
    
    @task()
    def extract_nifty500_data(trade_date: str) -> Dict:
        """
        Extract 5-minute OHLCV data for Nifty 500 stocks.
        
        Args:
            trade_date: Trade date string (YYYY-MM-DD)
            
        Returns:
            Dict with extraction results
        """
        from include.extractors.nifty500_extractor import Nifty500Extractor
        
        logger.info(f"Extracting Nifty 500 data for {trade_date}")
        
        extractor = Nifty500Extractor()
        dt = datetime.strptime(trade_date, "%Y-%m-%d")
        
        # Use MAX_INSTRUMENTS for testing (0 = all)
        max_inst = MAX_INSTRUMENTS if MAX_INSTRUMENTS > 0 else None
        
        try:
            data, failed = extractor.extract_daily_data(dt, max_instruments=max_inst)
            
            total_candles = sum(len(v) for v in data.values())
            logger.info(f"Extracted {total_candles} candles for {len(data)} instruments")
            
            if failed:
                logger.warning(f"Failed to extract {len(failed)} instruments: {failed[:5]}...")
            
            return {
                "trade_date": trade_date,
                "instruments_count": len(data),
                "total_candles": total_candles,
                "failed_count": len(failed),
                "failed_symbols": failed[:20],  # Store first 20 for debugging
                "data": data,
                "status": "success"
            }
        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            return {
                "trade_date": trade_date,
                "status": "failed",
                "error": str(e)
            }
    
    @task()
    def transform_data(extract_result: Dict) -> Dict:
        """
        Transform raw OHLCV data.
        
        Args:
            extract_result: Output from extract task
            
        Returns:
            Dict with transformed DataFrame info
        """
        from include.transformers.equity_transformer import EquityTransformer
        import pandas as pd
        
        if extract_result.get("status") != "success":
            logger.warning("Skipping transform - extraction failed")
            return extract_result
        
        trade_date = datetime.strptime(extract_result["trade_date"], "%Y-%m-%d")
        
        logger.info("Transforming Nifty 500 data")
        
        transformer = EquityTransformer(trade_date)
        df = transformer.transform_batch(extract_result["data"])
        
        logger.info(f"Transformed {len(df)} records")
        
        return {
            "trade_date": extract_result["trade_date"],
            "row_count": len(df),
            "columns": list(df.columns),
            "symbols_count": df["tradingsymbol"].nunique() if "tradingsymbol" in df.columns else 0,
            "dataframe": df.to_dict(orient="records"),
            "status": "success"
        }
    
    @task()
    def load_raw_data(transform_result: Dict) -> Dict:
        """
        Load data to raw layer of Parquet lake.
        
        Args:
            transform_result: Output from transform task
            
        Returns:
            Dict with load results
        """
        from include.loaders.parquet_loader import ParquetLoader
        import pandas as pd
        
        if transform_result.get("status") != "success":
            logger.warning("Skipping load - transform failed")
            return transform_result
        
        trade_date = datetime.strptime(transform_result["trade_date"], "%Y-%m-%d")
        
        df = pd.DataFrame(transform_result["dataframe"])
        
        loader = ParquetLoader()
        file_path = loader.write_equity_raw(df, trade_date)
        
        logger.info(f"Wrote raw data to {file_path}")
        
        return {
            "trade_date": transform_result["trade_date"],
            "file_path": file_path,
            "row_count": len(df),
            "status": "success"
        }
    
    @task()
    def process_data(load_result: Dict) -> Dict:
        """
        Process raw data and write to processed layer.
        
        Args:
            load_result: Output from load_raw task
            
        Returns:
            Dict with process results
        """
        from include.loaders.parquet_loader import ParquetLoader
        import pandas as pd
        
        if load_result.get("status") != "success":
            logger.warning("Skipping process - load failed")
            return load_result
        
        trade_date = datetime.strptime(load_result["trade_date"], "%Y-%m-%d")
        
        loader = ParquetLoader()
        
        # Read from raw
        raw_path = load_result.get("file_path", "")
        if raw_path:
            df = pd.read_parquet(raw_path)
        else:
            logger.warning("No raw file path, returning")
            return {"status": "skipped", "reason": "no raw data path"}
        
        if df.empty:
            logger.warning("No raw data found for processing")
            return {"status": "skipped", "reason": "no raw data"}
        
        # Additional processing
        # 1. Sort by symbol and timestamp
        if "tradingsymbol" in df.columns and "timestamp" in df.columns:
            df = df.sort_values(["tradingsymbol", "timestamp"])
        
        # 2. Reset index
        df = df.reset_index(drop=True)
        
        # Write to processed layer
        file_path = loader.write_equity_processed(df, trade_date)
        
        logger.info(f"Wrote processed data to {file_path}")
        
        return {
            "trade_date": load_result["trade_date"],
            "file_path": file_path,
            "row_count": len(df),
            "status": "success"
        }
    
    @task()
    def upload_to_s3(process_result: Dict) -> Dict:
        """
        Upload processed data to AWS S3.
        
        Args:
            process_result: Output from process task
            
        Returns:
            Dict with upload results
        """
        from include.loaders.s3_loader import S3Loader
        
        if process_result.get("status") != "success":
            logger.warning("Skipping S3 upload - process failed")
            return process_result
        
        loader = S3Loader()
        
        if not loader.is_configured():
            logger.info("S3 not configured, skipping upload")
            return {
                **process_result,
                "s3_status": "skipped",
                "s3_reason": "not configured"
            }
        
        local_path = process_result.get("file_path", "")
        trade_date = process_result["trade_date"]
        
        s3_key = loader.upload_equity_data(local_path, trade_date)
        
        if s3_key:
            logger.info(f"Uploaded to S3: {s3_key}")
            return {
                **process_result,
                "s3_key": s3_key,
                "s3_status": "success"
            }
        else:
            logger.warning("S3 upload failed")
            return {
                **process_result,
                "s3_status": "failed"
            }
    
    @task()
    def quality_check(upload_result: Dict) -> Dict:
        """
        Run data quality checks on processed data.
        
        Args:
            upload_result: Output from upload task
            
        Returns:
            Dict with quality check results
        """
        import pandas as pd
        
        if upload_result.get("status") != "success":
            logger.warning("Skipping quality check - process failed")
            return upload_result
        
        file_path = upload_result.get("file_path", "")
        if not file_path:
            return {"status": "skipped", "reason": "no file path"}
        
        df = pd.read_parquet(file_path)
        
        # Quality checks
        errors = []
        warnings = []
        
        # 1. Check row count (expect ~75 candles per stock for 5-min interval)
        expected_candles_per_stock = 75
        symbols_count = df["tradingsymbol"].nunique() if "tradingsymbol" in df.columns else 0
        expected_min = symbols_count * expected_candles_per_stock * 0.8  # 80% threshold
        
        if len(df) < expected_min:
            warnings.append(f"Row count {len(df)} below expected minimum {expected_min}")
        
        # 2. Check for nulls in critical columns
        critical_cols = ["timestamp", "open", "high", "low", "close"]
        for col in critical_cols:
            if col in df.columns:
                null_pct = df[col].isnull().sum() / len(df) * 100
                if null_pct > 1.0:
                    errors.append(f"Column {col} has {null_pct:.2f}% nulls")
        
        # 3. Check OHLC constraints
        invalid_ohlc = (
            (df["high"] < df["low"]) |
            (df["open"] > df["high"]) |
            (df["close"] > df["high"]) |
            (df["open"] < df["low"]) |
            (df["close"] < df["low"])
        ).sum()
        
        if invalid_ohlc > 0:
            errors.append(f"Found {invalid_ohlc} rows with invalid OHLC values")
        
        passed = len(errors) == 0
        
        result = {
            "trade_date": upload_result["trade_date"],
            "passed": passed,
            "errors": errors,
            "warnings": warnings,
            "row_count": len(df),
            "symbols_count": symbols_count,
            "s3_key": upload_result.get("s3_key"),
            "status": "success" if passed else "quality_failed"
        }
        
        if passed:
            logger.info(f"Quality checks passed for {upload_result['trade_date']}")
        else:
            logger.error(f"Quality checks failed: {errors}")
        
        return result
    
    # Build the DAG flow
    trade_date = "{{ ds }}"
    
    extract = extract_nifty500_data(trade_date)
    transform = transform_data(extract)
    load = load_raw_data(transform)
    process = process_data(load)
    upload = upload_to_s3(process)
    qc = quality_check(upload)


# Instantiate the DAG
nifty500_etl_daily = nifty500_etl_dag()
