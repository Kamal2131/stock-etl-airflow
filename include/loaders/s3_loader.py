"""
S3 Loader Module
Uploads Parquet files to AWS S3 bucket
"""
import os
import logging
from pathlib import Path
from typing import List, Optional

import boto3
from botocore.exceptions import ClientError
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


class S3Loader:
    """Uploads Parquet files to AWS S3."""
    
    def __init__(
        self,
        bucket_name: str = None,
        region: str = None,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None
    ):
        """
        Initialize S3 loader.
        
        Args:
            bucket_name: S3 bucket name (defaults to env var S3_BUCKET_NAME)
            region: AWS region (defaults to env var S3_REGION or ap-south-1)
            aws_access_key_id: AWS access key (defaults to env var AWS_ACCESS_KEY_ID)
            aws_secret_access_key: AWS secret key (defaults to env var AWS_SECRET_ACCESS_KEY)
        """
        self.bucket_name = bucket_name or os.getenv("S3_BUCKET_NAME")
        self.region = region or os.getenv("S3_REGION", "ap-south-1")
        self.aws_access_key_id = aws_access_key_id or os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = aws_secret_access_key or os.getenv("AWS_SECRET_ACCESS_KEY")
        self._client = None
    
    @property
    def client(self):
        """Lazy load S3 client."""
        if self._client is None:
            self._client = boto3.client(
                "s3",
                region_name=self.region,
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key
            )
        return self._client
    
    def is_configured(self) -> bool:
        """Check if S3 credentials are configured."""
        return all([
            self.bucket_name,
            self.aws_access_key_id,
            self.aws_secret_access_key
        ])
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def upload_file(self, local_path: str, s3_key: str) -> bool:
        """
        Upload a single file to S3.
        
        Args:
            local_path: Local file path
            s3_key: S3 object key (path in bucket)
            
        Returns:
            True if upload succeeded, False otherwise
        """
        if not self.is_configured():
            logger.warning("S3 not configured, skipping upload")
            return False
        
        try:
            self.client.upload_file(
                Filename=local_path,
                Bucket=self.bucket_name,
                Key=s3_key
            )
            logger.info(f"Uploaded {local_path} to s3://{self.bucket_name}/{s3_key}")
            return True
        except ClientError as e:
            logger.error(f"Failed to upload {local_path} to S3: {e}")
            raise
    
    def upload_directory(
        self,
        local_dir: str,
        s3_prefix: str,
        pattern: str = "*.parquet"
    ) -> List[str]:
        """
        Upload all matching files from a directory to S3.
        
        Args:
            local_dir: Local directory path
            s3_prefix: S3 prefix (folder path in bucket)
            pattern: Glob pattern for files to upload
            
        Returns:
            List of uploaded S3 keys
        """
        if not self.is_configured():
            logger.warning("S3 not configured, skipping upload")
            return []
        
        local_path = Path(local_dir)
        uploaded = []
        
        for file_path in local_path.glob(pattern):
            s3_key = f"{s3_prefix}/{file_path.name}"
            try:
                if self.upload_file(str(file_path), s3_key):
                    uploaded.append(s3_key)
            except Exception as e:
                logger.error(f"Failed to upload {file_path}: {e}")
        
        logger.info(f"Uploaded {len(uploaded)} files to s3://{self.bucket_name}/{s3_prefix}/")
        return uploaded
    
    def upload_equity_data(
        self,
        local_path: str,
        trade_date: str
    ) -> Optional[str]:
        """
        Upload equity data Parquet file to S3.
        
        Args:
            local_path: Local Parquet file path
            trade_date: Trade date string (YYYY-MM-DD) for S3 key
            
        Returns:
            S3 key if upload succeeded, None otherwise
        """
        if not self.is_configured():
            logger.warning("S3 not configured, skipping equity data upload")
            return None
        
        # S3 key format: nifty500/date=YYYY-MM-DD/data.parquet
        s3_key = f"nifty500/date={trade_date}/data.parquet"
        
        try:
            if self.upload_file(local_path, s3_key):
                return s3_key
        except Exception as e:
            logger.error(f"Failed to upload equity data: {e}")
        
        return None
    
    def check_file_exists(self, s3_key: str) -> bool:
        """
        Check if a file exists in S3.
        
        Args:
            s3_key: S3 object key
            
        Returns:
            True if file exists, False otherwise
        """
        if not self.is_configured():
            return False
        
        try:
            self.client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise
    
    def list_files(self, prefix: str) -> List[str]:
        """
        List files in S3 with given prefix.
        
        Args:
            prefix: S3 prefix to list
            
        Returns:
            List of S3 object keys
        """
        if not self.is_configured():
            return []
        
        try:
            response = self.client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            return [obj["Key"] for obj in response.get("Contents", [])]
        except ClientError as e:
            logger.error(f"Failed to list S3 objects: {e}")
            return []
