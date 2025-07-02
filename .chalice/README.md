# Chalice IAM Policy Documentation

This directory contains the IAM policy configuration for the data-segmentation-api Chalice application.

## Files

- `config.json`: Chalice configuration with environment variables and IAM policy reference
- `policy.json`: IAM policy document granting necessary permissions

## IAM Policy Permissions

The `policy.json` file grants the following permissions to the Lambda function:

### 1. CloudWatch Logs
- **Purpose**: Allow Lambda function to write logs
- **Permissions**: `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents`
- **Resource**: All CloudWatch log groups and streams

### 2. AWS Glue
- **Purpose**: Allow Lambda to start and monitor Glue jobs
- **Permissions**: 
  - `glue:StartJobRun` - Start Glue jobs
  - `glue:GetJobRun` - Get job run details
  - `glue:GetJobRuns` - List job runs
  - `glue:BatchGetJobs` - Get multiple jobs
  - `glue:GetJob` - Get job details
- **Resource**: All Glue jobs (`arn:aws:glue:*:*:job/*`)

### 3. Glue Data Catalog
- **Purpose**: Allow access to Glue Data Catalog for metadata
- **Permissions**: 
  - `glue:GetDatabase`, `glue:GetDatabases`
  - `glue:GetTable`, `glue:GetTables`
  - `glue:GetPartition`, `glue:GetPartitions`
- **Resource**: Glue catalog, databases, and tables

### 4. Amazon S3
- **Purpose**: Allow reading/writing data files
- **Permissions**: `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:ListBucket`
- **Resource**: 
  - `arn:aws:s3:::data-segmentation-*` and `arn:aws:s3:::data-segmentation-*/*`
  - `arn:aws:s3:::bojalabs-*` and `arn:aws:s3:::bojalabs-*/*`

### 5. IAM PassRole
- **Purpose**: Allow Lambda to pass IAM roles to Glue jobs
- **Permissions**: `iam:PassRole`
- **Resource**: Glue service roles (`AWSGlueServiceRole*`, `GlueServiceRole*`)
- **Condition**: Only when passing to Glue service

### 6. EC2 (for VPC)
- **Purpose**: Allow Glue jobs to work with VPC resources
- **Permissions**: `ec2:DescribeVpcs`, `ec2:DescribeSubnets`, `ec2:DescribeSecurityGroups`
- **Resource**: All EC2 resources

### 7. CloudWatch Metrics
- **Purpose**: Allow monitoring and metrics collection
- **Permissions**: `cloudwatch:PutMetricData`, `cloudwatch:GetMetricData`, `cloudwatch:GetMetricStatistics`
- **Resource**: All CloudWatch resources

### 8. Lambda (Optional)
- **Purpose**: Allow invoking other Lambda functions if needed
- **Permissions**: `lambda:InvokeFunction`
- **Resource**: Data categorization and segmentation Lambda functions

## Security Notes

1. **Principle of Least Privilege**: The policy grants only the minimum permissions needed
2. **Resource Scoping**: S3 permissions are scoped to specific bucket patterns
3. **Service-Specific**: IAM PassRole is restricted to Glue service only
4. **Monitoring**: CloudWatch permissions allow for proper monitoring and debugging

## Deployment

When you run `chalice deploy`, Chalice will:
1. Read the `policy.json` file
2. Create an IAM role with the specified permissions
3. Attach the role to the Lambda function
4. Deploy the application with the configured permissions

## Customization

To customize the policy for your specific needs:

1. **S3 Buckets**: Update the S3 resource ARNs to match your bucket names
2. **Glue Jobs**: Modify the Glue job resource patterns if needed
3. **Additional Services**: Add permissions for any additional AWS services you need

## Troubleshooting

If you encounter permission errors:

1. Check CloudWatch logs for detailed error messages
2. Verify the IAM role has the correct permissions
3. Ensure resource ARNs match your actual AWS resources
4. Check if any additional permissions are needed for your specific use case 