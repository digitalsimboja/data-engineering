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
                '--s3_input_path': s3_path,
                '--operation': 'categorize'
            }
        )
        
        job_run_id = response["JobRunId"]
        logger.info(f"Glue job started successfully. JobRunId: {job_run_id}")
        
        # Wait for job completion (optional - you might want to make this asynchronous)
        # For now, we'll return immediately and let the frontend poll for results
        return {
            "message": "Categorization Glue job started successfully",
            "jobRunId": job_run_id,
            "status": "STARTED",
            "segmentedRows": [],  # Empty for now, will be populated when job completes
            "columns": []
        }
        
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
        
        # If job is completed, try to get results from S3
        if status == 'SUCCEEDED':
            # You would typically read results from S3 here
            # For now, return a placeholder
            return {
                "jobRunId": job_run_id,
                "status": status,
                "message": "Job completed successfully",
                "segmentedRows": [],  # This would be populated from S3
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

@app.route('/segmentation')
def segmentation():
    request = app.current_request
    body = request.json_body
    
    s3_path = body.get("s3FilePath")
    if not s3_path:
        return Response(
            body={
            "error": "Missing s3FilePath"
            },
            status_code=400
        )
    try:
        response = glue_client.start_job_run(
            JobName=SEGMENTATION_GLUE_JOB,
            Arguments={
                '--s3_input_path': s3_path
            }
        )
        
        return {
            "message": "Glue job started",
            "jobRunId": response["JobRunId"]
        }
    except Exception as e:
        return Response(
            body={
                "error": str(e)
            },
            status_code=500
        )