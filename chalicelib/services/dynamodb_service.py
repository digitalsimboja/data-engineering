import logging
import boto3
from typing import Dict, Any, List, Optional
from datetime import datetime

from chalicelib.utils.aws_clients import aws_clients

logger = logging.getLogger(__name__)

class DynamoDBService:
    """Service class for DynamoDB operations"""
    
    def __init__(self, table_name: str = "data-categorization-file-metadata"):
        self.table_name = table_name
        self.table = aws_clients.get_dynamodb_table(table_name)
    
    def store_categorization_results(self, file_name: str, categorization_result: Dict[str, Any], 
                                   glue_script: str, script_path: str, data_count: int, 
                                   schema: List[str], job_name: str) -> str:
        """
        Store categorization results in DynamoDB
        
        Args:
            file_name: Name of the processed file
            categorization_result: Results from categorization
            glue_script: Generated Glue script
            script_path: S3 path to the script
            data_count: Number of sample records
            schema: Data schema
            job_name: Glue job name
            
        Returns:
            File ID of the stored record
        """
        try:
            timestamp = datetime.now().isoformat()
            file_id = f"{file_name}_{timestamp}"
            
            item = {
                'file_id': file_id,
                'file_name': file_name,
                'timestamp': timestamp,
                'job_name': job_name,
                'suggested_categories': categorization_result.get('suggested_categories', []),
                'reasoning': categorization_result.get('reasoning', ''),
                'segmentation_criteria': categorization_result.get('segmentation_criteria', {}),
                'generated_script_path': script_path,
                'sample_data_count': data_count,
                'schema': schema
            }
            
            self.table.put_item(Item=item)
            logger.info(f"Categorization results stored in DynamoDB with file_id: {file_id}")
            
            return file_id
            
        except Exception as e:
            logger.error(f"Error storing categorization results in DynamoDB: {str(e)}")
            raise Exception(f"Failed to store results in DynamoDB: {str(e)}")
    
    def store_segmentation_results(self, file_name: str, segmentation_criteria: Dict[str, Any],
                                 output_path: str, total_records: int, segments_created: List[str],
                                 schema: List[str], job_name: str) -> str:
        """
        Store segmentation results in DynamoDB
        
        Args:
            file_name: Name of the processed file
            segmentation_criteria: Criteria used for segmentation
            output_path: S3 output path
            total_records: Total number of records
            segments_created: List of created segments
            schema: Data schema
            job_name: Glue job name
            
        Returns:
            File ID of the stored record
        """
        try:
            timestamp = datetime.now().isoformat()
            file_id = f"{file_name}_{timestamp}"
            
            item = {
                'file_id': file_id,
                'file_name': file_name,
                'timestamp': timestamp,
                'job_name': job_name,
                'segmentation_criteria': segmentation_criteria,
                'output_path': output_path,
                'total_records': total_records,
                'segments_created': segments_created,
                'schema': schema,
                'segmented_rows': []  # This would be populated with actual segmented data if needed
            }
            
            self.table.put_item(Item=item)
            logger.info(f"Segmentation results stored in DynamoDB with file_id: {file_id}")
            
            return file_id
            
        except Exception as e:
            logger.error(f"Error storing segmentation results in DynamoDB: {str(e)}")
            raise Exception(f"Failed to store results in DynamoDB: {str(e)}")
    
    def get_latest_categorization_results(self, job_name: str) -> Optional[Dict[str, Any]]:
        """
        Get the most recent categorization results
        
        Args:
            job_name: Glue job name
            
        Returns:
            Latest categorization results or None
        """
        try:
            response = self.table.scan(
                FilterExpression=boto3.dynamodb.conditions.Attr('job_name').eq(job_name)
            )
            
            items = response.get('Items', [])
            if items:
                # Get the most recent result
                latest_item = max(items, key=lambda x: x.get('timestamp', ''))
                return latest_item
            
            return None
            
        except Exception as e:
            logger.error(f"Error retrieving categorization results from DynamoDB: {str(e)}")
            return None
    
    def get_latest_segmentation_results(self, job_name: str) -> Optional[Dict[str, Any]]:
        """
        Get the most recent segmentation results
        
        Args:
            job_name: Glue job name
            
        Returns:
            Latest segmentation results or None
        """
        try:
            response = self.table.scan(
                FilterExpression=boto3.dynamodb.conditions.Attr('job_name').eq(job_name)
            )
            
            items = response.get('Items', [])
            if items:
                # Get the most recent result
                latest_item = max(items, key=lambda x: x.get('timestamp', ''))
                return latest_item
            
            return None
            
        except Exception as e:
            logger.error(f"Error retrieving segmentation results from DynamoDB: {str(e)}")
            return None
    
    def get_latest_segmentation_script(self) -> Optional[Dict[str, Any]]:
        """
        Get the most recent segmentation script
        
        Returns:
            Latest segmentation script metadata or None
        """
        try:
            response = self.table.scan(
                FilterExpression=boto3.dynamodb.conditions.Attr('generated_script_path').exists()
            )
            
            items = response.get('Items', [])
            if items:
                # Get the most recent item with a script
                latest_item = max(items, key=lambda x: x.get('timestamp', ''))
                return latest_item
            
            return None
            
        except Exception as e:
            logger.error(f"Error retrieving segmentation script from DynamoDB: {str(e)}")
            return None

# Global instance
dynamodb_service = DynamoDBService() 