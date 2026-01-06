"""
F&O ETL DAG - Daily Pipeline
Extracts, transforms, and loads 1-minute F&O market data to Parquet data lake.

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

# Underlyings to process
UNDERLYINGS = os.getenv("UNDERLYINGS", "BANKNIFTY,NIFTY").split(",")


@dag(
    dag_id="fno_etl_daily",
    default_args=default_args,
    description="Daily F&O market data ETL pipeline to Parquet data lake",
    schedule="0 16 * * 1-5",  # 4:00 PM IST, Mon-Fri
    start_date=datetime(2025, 1, 1),
    catchup=True,
    max_active_runs=1,
    tags=["fno", "etl", "parquet", "production"],
)
def fno_etl_dag():
    """
    F&O ETL Pipeline DAG
    
    Stages:
    1. Extract: Pull 1-minute OHLCV+OI from Kite API
    2. Transform: Clean, validate, and enrich data
    3. Load Raw: Write to raw layer of Parquet lake
    4. Process: Additional cleaning for analytics
    5. Quality Check: Validate data integrity
    """
    
    @task()
    def extract_fno_data(underlying: str, trade_date: str) -> Dict:
        """
        Extract F&O data from Kite API.
        
        Args:
            underlying: Underlying symbol (BANKNIFTY, NIFTY)
            trade_date: Trade date string (YYYY-MM-DD)
            
        Returns:
            Dict with extraction results
        """
        from include.extractors.kite_extractor import KiteExtractor
        
        logger.info(f"Extracting data for {underlying} on {trade_date}")
        
        extractor = KiteExtractor()
        dt = datetime.strptime(trade_date, "%Y-%m-%d")
        
        try:
            data = extractor.extract_daily_fno_data(underlying, dt)
            
            total_candles = sum(len(v) for v in data.values())
            logger.info(f"Extracted {total_candles} candles for {len(data)} instruments")
            
            return {
                "underlying": underlying,
                "trade_date": trade_date,
                "instruments_count": len(data),
                "total_candles": total_candles,
                "data": data,
                "status": "success"
            }
        except Exception as e:
            logger.error(f"Extraction failed for {underlying}: {e}")
            return {
                "underlying": underlying,
                "trade_date": trade_date,
                "status": "failed",
                "error": str(e)
            }
    
    @task()
    def transform_fno_data(extract_result: Dict) -> Dict:
        """
        Transform raw F&O data.
        
        Args:
            extract_result: Output from extract task
            
        Returns:
            Dict with transformed DataFrame info
        """
        from include.transformers.fno_transformer import FNOTransformer
        import pandas as pd
        
        if extract_result.get("status") != "success":
            logger.warning(f"Skipping transform - extraction failed")
            return extract_result
        
        underlying = extract_result["underlying"]
        trade_date = datetime.strptime(extract_result["trade_date"], "%Y-%m-%d")
        
        logger.info(f"Transforming data for {underlying}")
        
        transformer = FNOTransformer(trade_date)
        df = transformer.transform_batch(extract_result["data"])
        
        logger.info(f"Transformed {len(df)} records")
        
        return {
            "underlying": underlying,
            "trade_date": extract_result["trade_date"],
            "row_count": len(df),
            "columns": list(df.columns),
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
        
        underlying = transform_result["underlying"]
        trade_date = datetime.strptime(transform_result["trade_date"], "%Y-%m-%d")
        
        df = pd.DataFrame(transform_result["dataframe"])
        
        loader = ParquetLoader()
        file_path = loader.write_raw(df, underlying, trade_date)
        
        logger.info(f"Wrote raw data to {file_path}")
        
        return {
            "underlying": underlying,
            "trade_date": transform_result["trade_date"],
            "file_path": file_path,
            "row_count": len(df),
            "status": "success"
        }
    
    @task()
    def process_data(load_result: Dict) -> Dict:
        """
        Process raw data and write to processed layer.
        Includes additional cleaning for analytics use.
        
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
        
        underlying = load_result["underlying"]
        trade_date = datetime.strptime(load_result["trade_date"], "%Y-%m-%d")
        
        loader = ParquetLoader()
        
        # Read from raw
        df = loader.read_partition("raw", underlying, trade_date)
        
        if df.empty:
            logger.warning("No raw data found for processing")
            return {"status": "skipped", "reason": "no raw data"}
        
        # Additional processing
        # 1. Sort by symbol and timestamp
        if "tradingsymbol" in df.columns and "timestamp" in df.columns:
            df = df.sort_values(["tradingsymbol", "timestamp"])
        
        # 2. Remove any remaining off-market data
        # (already handled in transform, but double-check)
        
        # 3. Reset index
        df = df.reset_index(drop=True)
        
        # Write to processed layer
        file_path = loader.write_processed(df, underlying, trade_date)
        
        logger.info(f"Wrote processed data to {file_path}")
        
        return {
            "underlying": underlying,
            "trade_date": load_result["trade_date"],
            "file_path": file_path,
            "row_count": len(df),
            "status": "success"
        }
    
    @task()
    def quality_check(process_result: Dict) -> Dict:
        """
        Run data quality checks on processed data.
        
        Args:
            process_result: Output from process task
            
        Returns:
            Dict with quality check results
        """
        from include.loaders.parquet_loader import ParquetLoader
        from include.utils.data_quality import DataQualityChecker
        
        if process_result.get("status") != "success":
            logger.warning("Skipping quality check - process failed")
            return process_result
        
        underlying = process_result["underlying"]
        trade_date = datetime.strptime(process_result["trade_date"], "%Y-%m-%d")
        
        loader = ParquetLoader()
        df = loader.read_partition("processed", underlying, trade_date)
        
        checker = DataQualityChecker(df)
        passed, errors, warnings = checker.run_all_checks()
        
        result = {
            "underlying": underlying,
            "trade_date": process_result["trade_date"],
            "passed": passed,
            "errors": errors,
            "warnings": warnings,
            "row_count": len(df),
            "status": "success" if passed else "quality_failed"
        }
        
        if passed:
            logger.info(f"Quality checks passed for {underlying}")
        else:
            logger.error(f"Quality checks failed: {errors}")
        
        return result
    
    # Build the DAG flow for each underlying
    for underlying in UNDERLYINGS:
        # Use execution_date for backfill support
        trade_date = "{{ ds }}"
        
        extract = extract_fno_data(underlying, trade_date)
        transform = transform_fno_data(extract)
        load = load_raw_data(transform)
        process = process_data(load)
        qc = quality_check(process)


# Instantiate the DAG
fno_etl_daily = fno_etl_dag()
