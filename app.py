import os
from chalice import Chalice, Response, CORSConfig
import boto3
import json
import logging
from dotenv import load_dotenv
from datetime import datetime
from chalicelib.utils.prompt_templates import get_categorization_prompt, get_script_generation_prompt, get_script_template

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
REGION_NAME = os.environ.get("AWS_REGION_NAME", "eu-west-1")
CATEGORIZATION_LAMBDA_FUNCTION_NAME = os.environ.get("CATEGORIZATION_LAMBDA_FUNCTION_NAME", "data-categorization-bedrock-api")

# AWS clients with profile
glue_client = boto3.client("glue", region_name=REGION_NAME)
s3_client = boto3.client("s3", region_name=REGION_NAME)
bedrock_client = boto3.client("bedrock-runtime", region_name=REGION_NAME)
dynamodb = boto3.resource("dynamodb", region_name=REGION_NAME)


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
                # Get results from DynamoDB using the same session configuration
                dynamodb = session.resource('dynamodb', region_name=REGION_NAME)
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

@app.lambda_function(name='data-categorization-bedrock-api')
def categorize_with_bedrock(event, context):
    """
    Lambda function to categorize data using Amazon Bedrock
    This function is invoked by the Glue job with sample data
    """
    
    logger.info(f"Received event: {json.dumps(event)}")
    
    try:
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
        
        logger.info(f"Processing {len(data)} sample records for file: {file_name}")
        logger.info(f"Schema: {schema}")
        
        # Prepare the prompt for categorization using template
        sample_data_str = json.dumps(data[:5], indent=2)  # Use first 5 records as sample
        schema_str = json.dumps(schema, indent=2)
        
        categorization_prompt = get_categorization_prompt(sample_data_str, schema_str)
        
        # Call Bedrock with Claude model for categorization
        logger.info("Calling Amazon Bedrock for categorization...")
        
        response = bedrock_client.invoke_model(
            modelId = "anthropic.claude-3-5-sonnet-20240620-v1:0",
            body=json.dumps({
                'prompt': f'\n\nHuman: {categorization_prompt}\n\nAssistant:',
                'max_tokens': 2000,
                'temperature': 0.1,
                'top_p': 0.9
            })
        )
        
        response_body = json.loads(response['body'].read())
        completion = response_body['completion']
        
        logger.info(f"Bedrock categorization response: {completion}")
        
        # Parse the response to extract JSON
        try:
            # Find JSON in the response
            start_idx = completion.find('{')
            end_idx = completion.rfind('}') + 1
            
            if start_idx != -1 and end_idx != -1:
                json_str = completion[start_idx:end_idx]
                categorization_result = json.loads(json_str)
            else:
                raise ValueError("No JSON found in response")
                
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON from Bedrock response: {e}")
            # Fallback: create basic categories based on schema
            categorization_result = {
                "suggested_categories": schema[:3] if len(schema) >= 3 else schema,
                "reasoning": "Fallback categorization based on available columns",
                "segmentation_criteria": {}
            }
        
        # Generate a Glue script using Bedrock
        script_generation_prompt = get_script_generation_prompt(
            schema, 
            categorization_result.get('suggested_categories', []),
            categorization_result.get('segmentation_criteria', {}),
            sample_data_str
        )
        
        # Call Bedrock to generate the Glue script
        logger.info("Calling Amazon Bedrock for script generation...")
        
        script_response = bedrock_client.invoke_model(
            modelId='anthropic.claude-3-sonnet-20240229-v1:0',
            body=json.dumps({
                'prompt': f'\n\nHuman: {script_generation_prompt}\n\nAssistant:',
                'max_tokens': 4000,
                'temperature': 0.1,
                'top_p': 0.9
            })
        )
        
        script_response_body = json.loads(script_response['body'].read())
        script_completion = script_response_body['completion']
        
        logger.info(f"Bedrock script generation response: {script_completion}")
        
        # Extract the generated script (remove any markdown formatting)
        glue_script = script_completion.strip()
        if glue_script.startswith('```python'):
            glue_script = glue_script[9:]
        if glue_script.endswith('```'):
            glue_script = glue_script[:-3]
        glue_script = glue_script.strip()
        
        # Store results in DynamoDB
        timestamp = datetime.now().isoformat()
        file_id = f"{file_name}_{timestamp}"
        
        # Use the same session configuration for DynamoDB
        dynamodb = session.resource('dynamodb', region_name=REGION_NAME)
        table = dynamodb.Table('data-categorization-file-metadata')
        
        # Generate S3 path for the script
        script_key = f"glue-scripts/segmentation-script-{timestamp}.py"
        script_path = f"s3://data-categorization-temp/{script_key}"
        
        # Store metadata in DynamoDB
        table.put_item(Item={
            'file_id': file_id,
            'file_name': file_name,
            'timestamp': timestamp,
            'job_name': CATEGORIZATION_GLUE_JOB,
            'suggested_categories': categorization_result.get('suggested_categories', []),
            'reasoning': categorization_result.get('reasoning', ''),
            'segmentation_criteria': categorization_result.get('segmentation_criteria', {}),
            'generated_script_path': script_path,
            'sample_data_count': len(data),
            'schema': schema
        })
        
        # Upload the generated script to S3
        s3_client.put_object(
            Bucket='data-categorization-temp',
            Key=script_key,
            Body=glue_script,
            ContentType='text/x-python'
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
            'error': 'Internal server error',
            'message': str(e)
        }


def generate_glue_segmentation_script(schema, categories, criteria):
    """
    Generate a Glue script for data segmentation based on suggested categories
    This function is kept for backward compatibility but now uses the template
    """
    script_template = get_script_template()
    
    # Replace placeholders in the template
    script = script_template.replace('{categories}', json.dumps(categories))
    script = script.replace('{segmentation_criteria}', json.dumps(criteria))
    
    return script