import logging
from typing import Optional
from datetime import datetime

from chalicelib.utils.aws_clients import aws_clients
from chalicelib.utils.validators import ValidationError

logger = logging.getLogger(__name__)

class S3Service:
    """Service class for S3 operations"""
    
    def __init__(self):
        self.s3_client = aws_clients.s3_client
    
    def validate_file_exists(self, s3_path: str) -> None:
        """
        Validate that a file exists in S3
        
        Args:
            s3_path: S3 path to validate
            
        Raises:
            ValidationError: If file doesn't exist or can't be accessed
        """
        try:
            bucket_name, key = s3_path.replace("s3://", "").split("/", 1)
            self.s3_client.head_object(Bucket=bucket_name, Key=key)
            logger.info(f"S3 file exists: {s3_path}")
            
        except self.s3_client.exceptions.NoSuchKey:
            logger.error(f"S3 file not found: {s3_path}")
            raise ValidationError(f"The file '{s3_path}' does not exist in S3. Please check the file path and ensure the file has been uploaded.")
            
        except self.s3_client.exceptions.ClientError as e:
            logger.error(f"S3 client error: {str(e)}")
            raise ValidationError(f"Unable to access S3 file: {str(e)}")
    
    def upload_script(self, script_content: str, bucket: str, key: str) -> str:
        """
        Upload a script to S3
        
        Args:
            script_content: Script content to upload
            bucket: S3 bucket name
            key: S3 key (file path)
            
        Returns:
            S3 path to the uploaded script
        """
        try:
            self.s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=script_content,
                ContentType='text/x-python'
            )
            
            script_path = f"s3://{bucket}/{key}"
            logger.info(f"Script uploaded to: {script_path}")
            
            return script_path
            
        except Exception as e:
            logger.error(f"Error uploading script to S3: {str(e)}")
            raise Exception(f"Failed to upload script to S3: {str(e)}")
    
    def generate_script_key(self, timestamp: str, prefix: str = "glue-scripts") -> str:
        """
        Generate S3 key for script upload
        
        Args:
            timestamp: Timestamp string
            prefix: S3 key prefix
            
        Returns:
            Generated S3 key
        """
        return f"{prefix}/segmentation-script-{timestamp}.py"
    
    def extract_file_name(self, s3_path: str) -> str:
        """
        Extract file name from S3 path
        
        Args:
            s3_path: S3 path
            
        Returns:
            File name
        """
        return s3_path.split('/')[-1]

# Global instance
s3_service = S3Service() 