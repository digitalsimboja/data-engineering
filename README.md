# Data Segmentation API - Modular Architecture

This document describes the modular architecture of the Data Segmentation API, which has been refactored to improve maintainability, testability, and code organization.

## Architecture Overview

The application has been modularized into several layers:

```
data-segmentation-api/
├── app.py                    # Original monolithic app
├── app_modular.py           # New modular app
├── chalicelib/
│   ├── utils/
│   │   ├── aws_clients.py   # AWS service clients
│   │   ├── validators.py    # Validation utilities
│   │   └── prompt_templates.py # AI prompt templates
│   └── services/
│       ├── glue_service.py      # Glue job operations
│       ├── dynamodb_service.py  # DynamoDB operations
│       ├── s3_service.py        # S3 operations
│       └── bedrock_service.py   # Amazon Bedrock operations
```

## Service Classes

### 1. AWSClients (`chalicelib/utils/aws_clients.py`)

Centralized AWS service client configuration.

**Features:**
- Manages AWS service clients (Glue, S3, Bedrock, DynamoDB)
- Configurable region settings
- Singleton pattern for consistent client usage

**Usage:**
```python
from chalicelib.utils.aws_clients import aws_clients

# Access clients
glue_client = aws_clients.glue_client
s3_client = aws_clients.s3_client
dynamodb_table = aws_clients.get_dynamodb_table('table-name')
```

### 2. Validators (`chalicelib/utils/validators.py`)

Common validation functions and error handling.

**Features:**
- S3 path validation
- Request body validation
- Standardized error response creation
- Custom validation exceptions

**Usage:**
```python
from chalicelib.utils.validators import validate_s3_path, create_error_response

try:
    bucket, key = validate_s3_path("s3://bucket/file.csv")
except ValidationError as e:
    return create_error_response("Invalid S3 path", str(e))
```

### 3. GlueService (`chalicelib/services/glue_service.py`)

Handles AWS Glue job operations.

**Features:**
- Start categorization jobs
- Start segmentation jobs
- Get job status
- Error handling for Glue operations

**Methods:**
- `start_categorization_job(s3_path, lambda_function_name)`
- `start_segmentation_job(s3_path, segmentation_criteria)`
- `get_job_status(job_run_id, job_name)`

### 4. DynamoDBService (`chalicelib/services/dynamodb_service.py`)

Manages DynamoDB operations for storing and retrieving results.

**Features:**
- Store categorization results
- Store segmentation results
- Retrieve latest results by job type
- Retrieve latest segmentation script

**Methods:**
- `store_categorization_results(...)`
- `store_segmentation_results(...)`
- `get_latest_categorization_results(job_name)`
- `get_latest_segmentation_results(job_name)`
- `get_latest_segmentation_script()`

### 5. S3Service (`chalicelib/services/s3_service.py`)

Handles S3 file operations.

**Features:**
- Validate file existence
- Upload scripts
- Generate S3 keys
- Extract file names

**Methods:**
- `validate_file_exists(s3_path)`
- `upload_script(script_content, bucket, key)`
- `generate_script_key(timestamp, prefix)`
- `extract_file_name(s3_path)`

### 6. BedrockService (`chalicelib/services/bedrock_service.py`)

Manages Amazon Bedrock AI operations.

**Features:**
- Data categorization using Claude
- Glue script generation
- Response parsing and error handling
- Fallback mechanisms

**Methods:**
- `categorize_data(sample_data, schema, file_name)`
- `generate_glue_script(schema, categories, criteria, sample_data)`

## Benefits of Modular Architecture

### 1. **Separation of Concerns**
- Each service handles a specific AWS service or domain
- Clear boundaries between different functionalities
- Easier to understand and maintain

### 2. **Testability**
- Services can be unit tested independently
- Mock dependencies easily
- Better test coverage

### 3. **Reusability**
- Services can be reused across different endpoints
- Common functionality centralized
- DRY (Don't Repeat Yourself) principle

### 4. **Maintainability**
- Changes to one service don't affect others
- Easier to debug and fix issues
- Clear responsibility boundaries

### 5. **Scalability**
- Easy to add new services
- Services can be extended independently
- Better code organization

## Migration Guide

### From Monolithic to Modular

1. **Replace the main app file:**
   ```bash
   # Backup original
   cp app.py app_original.py
   
   # Use modular version
   cp app_modular.py app.py
   ```

2. **Update imports in existing code:**
   ```python
   # Old way
   import boto3
   glue_client = boto3.client("glue", region_name="eu-west-1")
   
   # New way
   from chalicelib.utils.aws_clients import aws_clients
   glue_client = aws_clients.glue_client
   ```

3. **Use service classes:**
   ```python
   # Old way
   response = glue_client.start_job_run(...)
   
   # New way
   result = glue_service.start_categorization_job(s3_path, lambda_name)
   ```

## Environment Variables

The modular version uses the same environment variables:

```bash
CATEGORIZATION_GLUE_JOB=data-categorization-job
SEGMENTATION_GLUE_JOB=data-segmentation-job
AWS_REGION_NAME=eu-west-1
CATEGORIZATION_LAMBDA_FUNCTION_NAME=data-categorization-bedrock-api
```

## Error Handling

The modular architecture provides consistent error handling:

```python
try:
    result = service.method()
    return create_success_response(result)
except ValidationError as e:
    return create_error_response("Validation error", str(e), "validation", 400)
except Exception as e:
    return create_error_response("Server error", str(e), "server", 500)
```

## Testing

Each service can be tested independently:

```python
# Test GlueService
def test_start_categorization_job():
    result = glue_service.start_categorization_job("s3://bucket/file.csv", "lambda-name")
    assert result["status"] == "STARTED"

# Test DynamoDBService
def test_store_categorization_results():
    file_id = dynamodb_service.store_categorization_results(...)
    assert file_id is not None
```

## Future Enhancements

The modular architecture makes it easy to add new features:

1. **New Services:**
   - `EmailService` for notifications
   - `MetricsService` for monitoring
   - `CacheService` for performance

2. **Enhanced Validation:**
   - File type validation
   - Size limits
   - Security checks

3. **Additional AI Models:**
   - Support for different Bedrock models
   - Custom model integration

4. **Monitoring and Logging:**
   - Structured logging
   - Performance metrics
   - Error tracking

## Conclusion

The modular architecture provides a solid foundation for the Data Segmentation API, making it more maintainable, testable, and scalable. Each service has a clear responsibility and can be developed, tested, and deployed independently. 