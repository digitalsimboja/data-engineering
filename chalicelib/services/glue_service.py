import logging
import json
from datetime import datetime
from typing import Dict, Any, Optional
from chalice import Response

from chalicelib.utils.aws_clients import aws_clients
from chalicelib.utils.validators import create_error_response, create_success_response

logger = logging.getLogger(__name__)

class GlueService:
    """Service class for AWS Glue operations"""
    
    def __init__(self):
        self.glue_client = aws_clients.glue_client
    
    def start_categorization_job(self, s3_path: str, lambda_function_name: str) -> Dict[str, Any]:
        """
        Start categorization Glue job
        
        Args:
            s3_path: S3 path to input file
            lambda_function_name: Lambda function name for categorization
            
        Returns:
            Dictionary with job information
        """
        try:
            logger.info(f"Starting categorization Glue job with S3 path: {s3_path}")
            
            response = self.glue_client.start_job_run(
                JobName=aws_clients.region_name,
                Arguments={
                    '--S3_FILE_PATH': s3_path,
                    '--LAMBDA_FUNCTION_NAME': lambda_function_name
                }
            )
            
            job_run_id = response["JobRunId"]
            logger.info(f"Categorization Glue job started successfully. JobRunId: {job_run_id}")
            
            return {
                "message": "Categorization Glue job started successfully",
                "jobRunId": job_run_id,
                "status": "STARTED",
                "segmentedRows": [],
                "columns": []
            }
            
        except self.glue_client.exceptions.EntityNotFoundException:
            logger.error(f"Glue job not found: {aws_clients.region_name}")
            raise Exception(f"The Glue job '{aws_clients.region_name}' does not exist. Please check your configuration.")
            
        except self.glue_client.exceptions.ClientError as e:
            logger.error(f"AWS Glue client error: {str(e)}")
            raise Exception(f"AWS Glue service error: {str(e)}")
    
    def start_segmentation_job(self, s3_path: str, segmentation_criteria: Dict[str, Any]) -> Dict[str, Any]:
        """
        Start segmentation Glue job
        
        Args:
            s3_path: S3 path to input file
            segmentation_criteria: Segmentation criteria dictionary
            
        Returns:
            Dictionary with job information
        """
        try:
            logger.info(f"Starting segmentation Glue job with S3 path: {s3_path}")
            
            # Generate output path with timestamp
            output_path = f"s3://data-categorization-temp/segmentation-output/{datetime.now().isoformat()}"
            
            response = self.glue_client.start_job_run(
                JobName=aws_clients.region_name,
                Arguments={
                    '--s3_input_path': s3_path,
                    '--s3_output_path': output_path,
                    '--segmentation_criteria': json.dumps(segmentation_criteria)
                }
            )
            
            job_run_id = response["JobRunId"]
            logger.info(f"Segmentation Glue job started successfully. JobRunId: {job_run_id}")
            
            return {
                "message": "Segmentation Glue job started successfully",
                "jobRunId": job_run_id,
                "status": "STARTED",
                "segmentationCriteria": segmentation_criteria
            }
            
        except self.glue_client.exceptions.EntityNotFoundException:
            logger.error(f"Glue job not found: {aws_clients.region_name}")
            raise Exception(f"The Glue job '{aws_clients.region_name}' does not exist. Please check your configuration.")
            
        except self.glue_client.exceptions.ClientError as e:
            logger.error(f"AWS Glue client error: {str(e)}")
            raise Exception(f"AWS Glue service error: {str(e)}")
    
    def get_job_status(self, job_run_id: str, job_name: str) -> Dict[str, Any]:
        """
        Get status of a Glue job run
        
        Args:
            job_run_id: Job run ID
            job_name: Job name
            
        Returns:
            Dictionary with job status information
        """
        try:
            logger.info(f"Checking status for job run: {job_run_id}, job: {job_name}")
            
            response = self.glue_client.get_job_run(
                JobName=job_name,
                RunId=job_run_id
            )
            
            job_run = response['JobRun']
            status = job_run['JobRunState']
            
            logger.info(f"Job {job_run_id} status: {status}")
            
            return {
                "jobRunId": job_run_id,
                "status": status,
                "message": f"Job is {status.lower()}" if status not in ['SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT'] else f"Job {status.lower()}",
                "errorMessage": job_run.get('ErrorMessage') if status in ['FAILED', 'STOPPED', 'TIMEOUT'] else None
            }
            
        except self.glue_client.exceptions.EntityNotFoundException:
            logger.error(f"Job run not found: {job_run_id}")
            raise Exception(f"The job run '{job_run_id}' does not exist.")
            
        except Exception as e:
            logger.error(f"Error checking job status: {str(e)}")
            raise Exception(f"Error checking job status: {str(e)}")

# Global instance
glue_service = GlueService() 