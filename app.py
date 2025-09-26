import os
from chalice import Chalice, Response, CORSConfig
import logging
from dotenv import load_dotenv
from datetime import datetime

# Import modular services
from chalicelib.utils.aws_clients import aws_clients
from chalicelib.utils.validators import (
    validate_s3_path, validate_request_body, create_error_response, 
    create_success_response, ValidationError
)
from chalicelib.services.glue_service import glue_service
from chalicelib.services.dynamodb_service import dynamodb_service
from chalicelib.services.s3_service import s3_service
from chalicelib.services.bedrock_service import bedrock_service

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# CORS configuration
cors_config = CORSConfig(
    allow_origin='http://localhost:3000',
    allow_headers=['X-Special-Header'],
    max_age=600,
    expose_headers=['X-Special-Header'],
    allow_credentials=True
)

# Initialize Chalice app
app = Chalice(app_name='data-segmentation-api')
app.debug = True

# Environment variables
CATEGORIZATION_GLUE_JOB = os.environ.get("CATEGORIZATION_GLUE_JOB", "data-categorization-job")
SEGMENTATION_GLUE_JOB = os.environ.get("SEGMENTATION_GLUE_JOB", "data-segmentation-job")
CATEGORIZATION_LAMBDA_FUNCTION_NAME = os.environ.get("CATEGORIZATION_LAMBDA_FUNCTION_NAME", "data-categorization-bedrock-api")


@app.route('/')
def index():
    """Health check endpoint"""
    return {'hello': 'world'}


@app.route('/categorize', methods=['POST'], cors=cors_config)
def categorize():
    """
    Categorize data using Glue job
    Expected payload: {"s3FilePath": "s3://bucket/path/to/file"}
    """
    try:
        request = app.current_request
        body = request.json_body or {}
        
        logger.info(f"Categorize request received: {body}")
        print(f"Categorize request received: {body}")
        
        # Validate request
        validate_request_body(body, ['s3FilePath'])
        s3_path = body.get("s3FilePath")
        if not s3_path:
            raise ValidationError("S3 file path is required")
        validate_s3_path(s3_path)
        
        # Validate S3 file exists
        s3_service.validate_file_exists(s3_path)
        
        # Start Glue job for categorization
        result = glue_service.start_categorization_job(s3_path, CATEGORIZATION_LAMBDA_FUNCTION_NAME)
        
        return create_success_response(result)
        
    except ValidationError as e:
        logger.error(f"Validation error: {str(e)}")
        return create_error_response(
            error="Validation error",
            details=str(e),
            error_type="validation",
            status_code=400
        )
    except Exception as e:
        logger.error(f"Unexpected error during categorization: {str(e)}")
        return create_error_response(
            error="Internal server error",
            details="An unexpected error occurred during categorization",
            error_type="server",
            status_code=500
        )


@app.route('/job-status/{job_run_id}', methods=['GET'])
def get_job_status(job_run_id):
    """
    Get the status of a Glue job run
    Query params: type (categorization|segmentation)
    """
    try:
        request = app.current_request
        job_type = request.query_params.get('type', 'categorize')
        
        # Determine which job to check based on type
        job_name = SEGMENTATION_GLUE_JOB if job_type == 'segmentation' else CATEGORIZATION_GLUE_JOB
        
        # Get job status
        status_info = glue_service.get_job_status(job_run_id, job_name)
        
        # If job is completed, try to get results from DynamoDB
        if status_info['status'] == 'SUCCEEDED':
            if job_type == 'segmentation':
                latest_results = dynamodb_service.get_latest_segmentation_results(job_name)
            else:
                latest_results = dynamodb_service.get_latest_categorization_results(job_name)
            
            if latest_results:
                if job_type == 'segmentation':
                    return {
                        "jobRunId": job_run_id,
                        "status": status_info['status'],
                        "message": "Segmentation job completed successfully",
                        "segmentedRows": latest_results.get('segmented_rows', []),
                        "columns": latest_results.get('schema', []),
                        "segmentationCriteria": latest_results.get('segmentation_criteria', {}),
                        "outputPath": latest_results.get('output_path', '')
                    }
                else:
                    return {
                        "jobRunId": job_run_id,
                        "status": status_info['status'],
                        "message": "Job completed successfully",
                        "suggestedCategories": latest_results.get('suggested_categories', []),
                        "generatedScriptPath": latest_results.get('generated_script_path', ''),
                        "segmentedRows": [],
                        "columns": latest_results.get('suggested_categories', [])
                    }
            else:
                return {
                    "jobRunId": job_run_id,
                    "status": status_info['status'],
                    "message": "Job completed successfully but no results found",
                    "segmentedRows": [],
                    "columns": []
                }
        elif status_info['status'] in ['FAILED', 'STOPPED', 'TIMEOUT']:
            return create_error_response(
                error=f"Job {status_info['status'].lower()}",
                details=status_info.get('errorMessage', 'Unknown error'),
                error_type="glue",
                status_code=500
            )
        else:
            # Job is still running
            return {
                "jobRunId": job_run_id,
                "status": status_info['status'],
                "message": status_info['message']
            }
            
    except Exception as e:
        logger.error(f"Error checking job status: {str(e)}")
        return create_error_response(
            error="Error checking job status",
            details=str(e),
            error_type="server",
            status_code=500
        )


@app.route('/segment', methods=['POST'], cors=cors_config)
def segment():
    """
    Segment data using stored Glue script from DynamoDB
    Expected payload: {"s3FilePath": "s3://bucket/path/to/file"}
    """
    try:
        request = app.current_request
        body = request.json_body or {}
        
        logger.info(f"Segment request received: {body}")
        
        # Validate request
        validate_request_body(body, ['s3FilePath'])
        s3_path = body.get("s3FilePath")
        if not s3_path:
            raise ValidationError("S3 file path is required")
        validate_s3_path(s3_path)
        
        # Validate S3 file exists
        s3_service.validate_file_exists(s3_path)
        
        # Get the most recent segmentation script from DynamoDB
        latest_script = dynamodb_service.get_latest_segmentation_script()
        
        if not latest_script:
            return create_error_response(
                error="No segmentation script found",
                details="Please run categorization first to generate a segmentation script",
                error_type="script",
                status_code=404
            )
        
        segmentation_criteria = latest_script.get('segmentation_criteria', {})
        script_path = latest_script.get('generated_script_path', '')
        
        logger.info(f"Using segmentation criteria: {segmentation_criteria}")
        logger.info(f"Using script path: {script_path}")
        
        # Start Glue job with segmentation criteria
        result = glue_service.start_segmentation_job(s3_path, segmentation_criteria)
        
        return create_success_response(result)
        
    except ValidationError as e:
        logger.error(f"Validation error: {str(e)}")
        return create_error_response(
            error="Validation error",
            details=str(e),
            error_type="validation",
            status_code=400
        )
    except Exception as e:
        logger.error(f"Unexpected error during segmentation: {str(e)}")
        return create_error_response(
            error="Internal server error",
            details="An unexpected error occurred during segmentation",
            error_type="server",
            status_code=500
        )


@app.lambda_function(name='data-categorization-bedrock-api')
def categorize_with_bedrock(event, context):
    """
    Lambda function to categorize data using Amazon Bedrock
    This function is invoked by the Glue job with sample data
    """
    try:
        logger.info(f"Event: {event}")
        
        # Extract data from the event
        data = event.get('data', [])
        schema = event.get('schema', [])
        file_name = event.get('file_name', 'unknown')
        
        if not data:
            logger.error("No data provided in the event")
            return {
                'error': 'No data provided',
                'message': 'Sample data is required for categorization'
            }
        
        # Categorize data using Bedrock
        categorization_result = bedrock_service.categorize_data(data, schema, file_name)
        
        # Generate Glue script using Bedrock
        glue_script = bedrock_service.generate_glue_script(
            schema,
            categorization_result.get('suggested_categories', []),
            categorization_result.get('segmentation_criteria', {}),
            data
        )
        
        # Store results in DynamoDB
        timestamp = datetime.now().isoformat()
        script_key = s3_service.generate_script_key(timestamp)
        script_path = s3_service.upload_script(glue_script, 'data-categorization-temp', script_key)
        
        file_id = dynamodb_service.store_categorization_results(
            file_name=file_name,
            categorization_result=categorization_result,
            glue_script=glue_script,
            script_path=script_path,
            data_count=len(data),
            schema=schema,
            job_name=CATEGORIZATION_GLUE_JOB
        )
        
        logger.info(f"Results stored in DynamoDB with file_id: {file_id}")
        logger.info(f"Generated script uploaded to: {script_path}")
        
        return {
            'suggested_categories': categorization_result.get('suggested_categories', []),
            'reasoning': categorization_result.get('reasoning', ''),
            'segmentation_criteria': categorization_result.get('segmentation_criteria', {}),
            'generated_glue_script': glue_script,
            's3_script_path': script_path,
            'file_id': file_id,
            'message': 'Categorization completed successfully'
        }
        
    except Exception as e:
        logger.error(f"Error in categorization Lambda: {str(e)}")
        return {
            'error': 'Error in categorization Lambda',
            'message': str(e)
        }


def generate_glue_segmentation_script(schema, categories, criteria):
    """
    Generate a Glue script for data segmentation based on suggested categories
    This function is kept for backward compatibility but now uses the template
    """
    from chalicelib.utils.prompt_templates import get_script_template
    import json
    
    script_template = get_script_template()
    
    # Replace placeholders in the template
    script = script_template.replace('{categories}', json.dumps(categories))
    script = script.replace('{segmentation_criteria}', json.dumps(criteria))
    
    return script 