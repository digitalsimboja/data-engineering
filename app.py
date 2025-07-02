import os
from chalice import Chalice, Response, CORSConfig
import boto3
import json
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

cors_config = CORSConfig(
    allow_origin='http://localhost:3000',
    allow_headers=['X-Special-Header'],
    max_age=600,
    expose_headers=['X-Special-Header'],
    allow_credentials=True
)

app = Chalice(app_name='data-segmentation-api')
app.debug = True

# Environment variables
CATEGORIZATION_GLUE_JOB = os.environ.get("CATEGORIZATION_GLUE_JOB", "data-categorization-job")
SEGMENTATION_GLUE_JOB = os.environ.get("SEGMENTATION_GLUE_JOB", "data-segmentation-job")
AWS_REGION = os.environ.get("AWS_REGION", "eu-west-1")
CATEGORIZATION_LAMBDA_FUNCTION_NAME = os.environ.get("CATEGORIZATION_LAMBDA_FUNCTION_NAME", "data-categorization-api")

# AWS clients with profile
aws_profile = os.environ.get("AWS_PROFILE", "dev")

try:
    # Use the specified AWS profile
    session = boto3.Session(profile_name=aws_profile)
    glue_client = session.client("glue", region_name=AWS_REGION)
    s3_client = session.client("s3", region_name=AWS_REGION)
    logger.info(f"Using AWS profile: {aws_profile}")
except Exception as e:
    logger.error(f"Error creating AWS session with profile {aws_profile}: {str(e)}")
    # Fallback to default credential chain
    glue_client = boto3.client("glue", region_name=AWS_REGION)
    s3_client = boto3.client("s3", region_name=AWS_REGION)


@app.route('/')
def index():
    return {'hello': 'world'}

@app.route('/categorize', methods=['POST'], cors=cors_config)
def categorize():
    """
    Categorize data using Glue job
    Expected payload: {"s3FilePath": "s3://bucket/path/to/file"}
    """
    request = app.current_request
    body = request.json_body or {}
    
    logger.info(f"Categorize request received: {body}")
    print(f"Categorize request received: {body}")
    
    # Validate input
    s3_path = body.get("s3FilePath")
    if not s3_path:
        logger.error("Missing s3FilePath in request")
        return Response(
            body={
                "error": "Missing s3FilePath parameter",
                "details": "Please provide the S3 path to the file you want to categorize"
            },
            status_code=400
        )
    
    # Validate S3 path format
    if not s3_path.startswith("s3://"):
        logger.error(f"Invalid S3 path format: {s3_path}")
        return Response(
            body={
                "error": "Invalid S3 path format",
                "details": "S3 path must start with 's3://'"
            },
            status_code=400
        )
    
    try:
        logger.info(f"Starting Glue job: {CATEGORIZATION_GLUE_JOB}")
        logger.info(f"S3 input path: {s3_path}")
        
        # Start Glue job for categorization
        response = glue_client.start_job_run(
            JobName=CATEGORIZATION_GLUE_JOB,
            Arguments={
                '--S3_FILE_PATH': s3_path,
                '--LAMBDA_FUNCTION_NAME': CATEGORIZATION_LAMBDA_FUNCTION_NAME
            }
        )
        
        job_run_id = response["JobRunId"]
        logger.info(f"Glue job started successfully. JobRunId: {job_run_id}")
        
        # Wait for job completion (optional - you might want to make this asynchronous)
        # For now, we'll return immediately and let the frontend poll for results
        return Response(
            body={
                "message": "Categorization Glue job started successfully",
                "jobRunId": job_run_id,
                "status": "STARTED",
                "segmentedRows": [],
                "columns": []
            },
            status_code=200
        )
        
    except glue_client.exceptions.EntityNotFoundException:
        logger.error(f"Glue job not found: {CATEGORIZATION_GLUE_JOB}")
        return Response(
            body={
                "error": "Glue job not found",
                "details": f"The Glue job '{CATEGORIZATION_GLUE_JOB}' does not exist. Please check your configuration.",
                "type": "glue"
            },
            status_code=500
        )
        
    except glue_client.exceptions.ClientError as e:
        logger.error(f"AWS Glue client error: {str(e)}")
        return Response(
            body={
                "error": "AWS Glue service error",
                "details": str(e),
                "type": "glue"
            },
            status_code=500
        )
        
    except Exception as e:
        logger.error(f"Unexpected error during categorization: {str(e)}")
        return Response(
            body={
                "error": "Internal server error",
                "details": "An unexpected error occurred during categorization",
                "type": "server"
            },
            status_code=500
        )

@app.route('/job-status/{job_run_id}', methods=['GET'])
def get_job_status(job_run_id):
    """
    Get the status of a Glue job run
    """
    try:
        logger.info(f"Checking status for job run: {job_run_id}")
        
        # Get job run details
        response = glue_client.get_job_run(
            JobName=CATEGORIZATION_GLUE_JOB,
            RunId=job_run_id
        )
        
        job_run = response['JobRun']
        status = job_run['JobRunState']
        
        logger.info(f"Job {job_run_id} status: {status}")
        
        # If job is completed, try to get results from DynamoDB
        if status == 'SUCCEEDED':
            try:
                # Get results from DynamoDB
                dynamodb = boto3.resource('dynamodb')
                table = dynamodb.Table('data-categorization-file-metadata')
                
                # Query DynamoDB for the job results
                response = table.scan(
                    FilterExpression=boto3.dynamodb.conditions.Attr('job_name').eq(CATEGORIZATION_GLUE_JOB)
                )
                
                items = response.get('Items', [])
                if items:
                    # Get the most recent result
                    latest_item = max(items, key=lambda x: x.get('timestamp', ''))
                    
                    return {
                        "jobRunId": job_run_id,
                        "status": status,
                        "message": "Job completed successfully",
                        "suggestedCategories": latest_item.get('suggested_categories', []),
                        "generatedScriptPath": latest_item.get('generated_script_path', ''),
                        "segmentedRows": [],  # For categorization, this is empty
                        "columns": latest_item.get('suggested_categories', [])
                    }
                else:
                    return {
                        "jobRunId": job_run_id,
                        "status": status,
                        "message": "Job completed successfully but no results found",
                        "segmentedRows": [],
                        "columns": []
                    }
            except Exception as e:
                logger.error(f"Error retrieving results from DynamoDB: {str(e)}")
                return {
                    "jobRunId": job_run_id,
                    "status": status,
                    "message": "Job completed successfully",
                    "segmentedRows": [],
                    "columns": []
                }
        elif status in ['FAILED', 'STOPPED', 'TIMEOUT']:
            return Response(
                body={
                    "error": f"Job {status.lower()}",
                    "details": job_run.get('ErrorMessage', 'Unknown error'),
                    "type": "glue"
                },
                status_code=500
            )
        else:
            # Job is still running
            return {
                "jobRunId": job_run_id,
                "status": status,
                "message": f"Job is {status.lower()}"
            }
            
    except glue_client.exceptions.EntityNotFoundException:
        logger.error(f"Job run not found: {job_run_id}")
        return Response(
            body={
                "error": "Job run not found",
                "details": f"The job run '{job_run_id}' does not exist.",
                "type": "glue"
            },
            status_code=404
        )
    except Exception as e:
        logger.error(f"Error checking job status: {str(e)}")
        return Response(
            body={
                "error": "Error checking job status",
                "details": str(e),
                "type": "server"
            },
            status_code=500
        )

@app.route('/segmentation', methods=['POST'], cors=cors_config)
def segmentation():
    request = app.current_request
    body = request.json_body or {}
    
    logger.info(f"Segmentation request received: {body}")
    
    s3_path = body.get("s3FilePath")
    if not s3_path:
        logger.error("Missing s3FilePath in request")
        return Response(
            body={
                "error": "Missing s3FilePath parameter",
                "details": "Please provide the S3 path to the file you want to segment"
            },
            status_code=400
        )
    
    # Validate S3 path format
    if not s3_path.startswith("s3://"):
        logger.error(f"Invalid S3 path format: {s3_path}")
        return Response(
            body={
                "error": "Invalid S3 path format",
                "details": "S3 path must start with 's3://'"
            },
            status_code=400
        )
    
    # Validate S3 file exists
    try:
        bucket_name, key = s3_path.replace("s3://", "").split("/", 1)
        s3_client.head_object(Bucket=bucket_name, Key=key)
        logger.info(f"S3 file exists: {s3_path}")
    except s3_client.exceptions.NoSuchKey:
        logger.error(f"S3 file not found: {s3_path}")
        return Response(
            body={
                "error": "S3 file not found",
                "details": f"The file '{s3_path}' does not exist in S3. Please check the file path and ensure the file has been uploaded.",
                "type": "s3"
            },
            status_code=404
        )
    except s3_client.exceptions.ClientError as e:
        logger.error(f"S3 client error: {str(e)}")
        return Response(
            body={
                "error": "S3 access error",
                "details": f"Unable to access S3 file: {str(e)}",
                "type": "s3"
            },
            status_code=500
        )
    
    try:
        logger.info(f"Starting Glue job: {SEGMENTATION_GLUE_JOB}")
        logger.info(f"S3 input path: {s3_path}")
        
        response = glue_client.start_job_run(
            JobName=SEGMENTATION_GLUE_JOB,
            Arguments={
                '--s3_input_path': s3_path
            }
        )
        
        job_run_id = response["JobRunId"]
        logger.info(f"Segmentation Glue job started successfully. JobRunId: {job_run_id}")
        
        return Response(
            body={
                "message": "Segmentation Glue job started successfully",
                "jobRunId": job_run_id,
                "status": "STARTED"
            },
            status_code=200
        )
    except glue_client.exceptions.EntityNotFoundException:
        logger.error(f"Glue job not found: {SEGMENTATION_GLUE_JOB}")
        return Response(
            body={
                "error": "Glue job not found",
                "details": f"The Glue job '{SEGMENTATION_GLUE_JOB}' does not exist. Please check your configuration.",
                "type": "glue"
            },
            status_code=500
        )
    except glue_client.exceptions.ClientError as e:
        logger.error(f"AWS Glue client error: {str(e)}")
        return Response(
            body={
                "error": "AWS Glue service error",
                "details": str(e),
                "type": "glue"
            },
            status_code=500
        )
    except Exception as e:
        logger.error(f"Unexpected error during segmentation: {str(e)}")
        return Response(
            body={
                "error": "Internal server error",
                "details": "An unexpected error occurred during segmentation",
                "type": "server"
            },
            status_code=500
        )