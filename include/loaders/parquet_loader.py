"""
Parquet Data Lake Loader Module
Writes data to partitioned Parquet files in a data lake structure
"""
import os
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


class ParquetLoader:
    """Loads data into partitioned Parquet data lake."""
    
    def __init__(self, base_path: str = None):
        """
        Initialize loader.
        
        Args:
            base_path: Base path for data lake (defaults to DATA_LAKE_PATH env var)
        """
        self.base_path = Path(base_path or os.getenv("DATA_LAKE_PATH", "/opt/airflow/data/lake"))
    
    def write_raw(
        self,
        df: pd.DataFrame,
        underlying: str,
        trade_date: datetime,
        overwrite: bool = True
    ) -> str:
        """
        Write data to raw layer of data lake.
        
        Args:
            df: DataFrame to write
            underlying: Underlying symbol (partition key)
            trade_date: Trade date (partition key)
            overwrite: Whether to overwrite existing data
            
        Returns:
            Path to written file
        """
        return self._write_layer("raw", df, underlying, trade_date, overwrite)
    
    def write_processed(
        self,
        df: pd.DataFrame,
        underlying: str,
        trade_date: datetime,
        overwrite: bool = True
    ) -> str:
        """Write data to processed layer."""
        return self._write_layer("processed", df, underlying, trade_date, overwrite)
    
    def write_analytics(
        self,
        df: pd.DataFrame,
        underlying: str,
        trade_date: datetime,
        filename: str = "indicators.parquet",
        overwrite: bool = True
    ) -> str:
        """Write data to analytics layer."""
        return self._write_layer("analytics", df, underlying, trade_date, overwrite, filename)
    
    def _write_layer(
        self,
        layer: str,
        df: pd.DataFrame,
        underlying: str,
        trade_date: datetime,
        overwrite: bool = True,
        filename: str = "data.parquet"
    ) -> str:
        """
        Write data to specified layer.
        
        Partition scheme: layer/underlying=X/date=YYYY-MM-DD/data.parquet
        """
        if df.empty:
            logger.warning(f"Empty DataFrame, skipping write to {layer}")
            return ""
        
        # Build partition path
        date_str = trade_date.strftime("%Y-%m-%d")
        partition_path = self.base_path / "fno" / layer / f"underlying={underlying}" / f"date={date_str}"
        
        # Create directory if needed
        partition_path.mkdir(parents=True, exist_ok=True)
        
        # Full file path
        file_path = partition_path / filename
        
        # Check if file exists
        if file_path.exists() and not overwrite:
            logger.warning(f"File exists and overwrite=False: {file_path}")
            return str(file_path)
        
        # Write parquet
        table = pa.Table.from_pandas(df)
        pq.write_table(
            table,
            file_path,
            compression="snappy",
            use_dictionary=True,
            write_statistics=True
        )
        
        logger.info(f"Wrote {len(df)} rows to {file_path}")
        return str(file_path)
    
    def read_partition(
        self,
        layer: str,
        underlying: str,
        trade_date: datetime
    ) -> pd.DataFrame:
        """
        Read data from a specific partition.
        
        Args:
            layer: Data lake layer (raw, processed, analytics)
            underlying: Underlying symbol
            trade_date: Trade date
            
        Returns:
            DataFrame with partition data
        """
        date_str = trade_date.strftime("%Y-%m-%d")
        partition_path = self.base_path / "fno" / layer / f"underlying={underlying}" / f"date={date_str}"
        
        if not partition_path.exists():
            logger.warning(f"Partition not found: {partition_path}")
            return pd.DataFrame()
        
        # Read all parquet files in partition
        dfs = []
        for parquet_file in partition_path.glob("*.parquet"):
            df = pd.read_parquet(parquet_file)
            dfs.append(df)
        
        if not dfs:
            return pd.DataFrame()
        
        return pd.concat(dfs, ignore_index=True)
    
    def list_partitions(self, layer: str = "raw") -> list:
        """List all partitions in a layer."""
        layer_path = self.base_path / "fno" / layer
        
        if not layer_path.exists():
            return []
        
        partitions = []
        for underlying_dir in layer_path.glob("underlying=*"):
            underlying = underlying_dir.name.split("=")[1]
            for date_dir in underlying_dir.glob("date=*"):
                date_str = date_dir.name.split("=")[1]
                partitions.append({
                    "underlying": underlying,
                    "date": date_str,
                    "path": str(date_dir)
                })
        
        return partitions
