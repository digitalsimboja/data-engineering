# Data Segmentation API

A Chalice-based API for data segmentation and categorization using AWS Lambda functions.

## Endpoints

### POST /categorize

Starts a Glue job to categorize data.

**Request Body:**
```json
{
  "s3FilePath": "s3://your-bucket/path/to/file.csv"
}
```

**Response:**
```json
{
  "message": "Categorization Glue job started successfully",
  "jobRunId": "jr_1234567890",
  "status": "STARTED",
  "segmentedRows": [],
  "columns": []
}
```

**Error Response:**
```json
{
  "error": "Error message",
  "details": "Detailed error information",
  "type": "glue|server|validation"
}
```

### GET /job-status/{job_run_id}

Gets the status of a Glue job run.

**Response:**
```json
{
  "jobRunId": "jr_1234567890",
  "status": "SUCCEEDED",
  "message": "Job completed successfully",
  "segmentedRows": [...],
  "columns": [...]
}
```

### POST /segmentation

Starts a Glue job for data segmentation.

**Request Body:**
```json
{
  "s3FilePath": "s3://your-bucket/path/to/file.csv"
}
```

## Environment Variables

The following environment variables need to be configured:

- `GLUE_JOB_NAME`: Name of the Glue job for segmentation (default: "data-segmentation-job")
- `CATEGORIZATION_GLUE_JOB`: Name of the Glue job for categorization (default: "data-categorization-job")
- `SEGMENTATION_GLUE_JOB`: Name of the Glue job for segmentation (default: "data-segmentation-job")
- `AWS_REGION`: AWS region (default: "eu-west-1")

## Deployment

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Deploy to AWS:
```bash
chalice deploy
```

## Error Handling

The API includes comprehensive error handling for:

- Missing or invalid S3 file paths
- Lambda function not found
- Lambda function execution errors
- AWS service errors
- Network and server errors

Each error response includes:
- `error`: Main error message
- `details`: Detailed error information
- `type`: Error type for frontend handling

## Frontend Integration

The frontend can call the categorize endpoint like this:

```javascript
const response = await fetch('/api/segmentation/categorize', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
  },
  body: JSON.stringify({ s3FilePath: 's3://bucket/path/to/file.csv' }),
});

const data = await response.json();

if (response.ok) {
  // Handle successful categorization
  console.log(data.segmentedRows, data.columns);
} else {
  // Handle error
  console.error(data.error, data.details);
}
``` 