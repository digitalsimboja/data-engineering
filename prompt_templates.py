"""
Prompt templates for data categorization and Glue script generation using Amazon Bedrock.
"""

def get_categorization_prompt(sample_data_str, schema_str):
    """
    Generate prompt for data categorization using Bedrock.
    
    Args:
        sample_data_str (str): JSON string of sample data
        schema_str (str): JSON string of column schema
    
    Returns:
        str: Formatted prompt for categorization
    """
    return f"""
You are a data analyst expert. Analyze the following sample data and suggest appropriate categories for data segmentation.

Sample Data:
{sample_data_str}

Schema (Column Names):
{schema_str}

Based on the data structure and content, suggest 5-10 meaningful categories that could be used for data segmentation. 
Consider factors like:
- Demographic information (age, location, gender, etc.)
- Behavioral patterns (purchase history, activity level, etc.)
- Value-based segmentation (high-value, medium-value, low-value customers)
- Product preferences or usage patterns
- Geographic or temporal patterns
- Customer lifecycle stages
- Risk profiles or credit scores
- Engagement levels
- Transaction frequency
- Average order values

Return your response as a JSON object with the following structure:
{{
    "suggested_categories": ["Category1", "Category2", "Category3", "Category4", "Category5"],
    "reasoning": "Brief explanation of why these categories make sense for this dataset",
    "segmentation_criteria": {{
        "Category1": {{
            "description": "Detailed description of this category and its business value",
            "potential_filters": [
                ["column_name", "condition", "value"],
                ["column_name", "condition", "value"]
            ],
            "business_use_case": "How this category can be used for business decisions"
        }},
        "Category2": {{
            "description": "Detailed description of this category and its business value",
            "potential_filters": [
                ["column_name", "condition", "value"]
            ],
            "business_use_case": "How this category can be used for business decisions"
        }}
    }}
}}

Focus on practical, actionable categories that would be useful for business decision-making, marketing campaigns, customer service, and strategic planning.
"""


def get_script_generation_prompt(schema, categories, criteria, sample_data_str):
    """
    Generate prompt for creating a Glue segmentation script using Bedrock.
    
    Args:
        schema (list): List of column names
        categories (list): List of suggested categories
        criteria (dict): Segmentation criteria for each category
        sample_data_str (str): JSON string of sample data
    
    Returns:
        str: Formatted prompt for script generation
    """
    return f"""
You are an AWS Glue expert. Create a comprehensive Glue script for data segmentation based on the provided categories and criteria.

Dataset Information:
- Schema: {schema}
- Sample Data: {sample_data_str}
- Categories: {categories}
- Segmentation Criteria: {criteria}

Create a complete AWS Glue script that:
1. Reads CSV data from S3
2. Applies segmentation logic for each category
3. Handles different data types appropriately
4. Includes error handling and logging
5. Saves segmented data to separate S3 locations
6. Generates summary statistics

Requirements:
- Use PySpark DataFrame operations
- Include proper error handling with try-catch blocks
- Add comprehensive logging for debugging
- Handle missing values and data quality issues
- Support both categorical and numerical segmentation
- Include data validation and schema checking
- Generate summary reports for each segment

The script should be production-ready and follow AWS Glue best practices.
Return only the Python code without any markdown formatting or explanations.
"""


def get_script_template():
    """
    Get the base template for Glue segmentation scripts.
    
    Returns:
        str: Base script template
    """
    return '''import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_input_path', 's3_output_path'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Get job parameters
s3_input_path = args['s3_input_path']
s3_output_path = args['s3_output_path']

logger.info(f"Starting segmentation job")
logger.info(f"Input path: {s3_input_path}")
logger.info(f"Output path: {s3_output_path}")

try:
    # Read the input data
    logger.info(f"Reading data from: {s3_input_path}")
    df = spark.read.csv(s3_input_path, header=True, inferSchema=True)
    
    logger.info(f"Data schema: {df.schema}")
    logger.info(f"Data count: {df.count()}")
    
    # Data quality check
    logger.info("Performing data quality checks...")
    total_rows = df.count()
    null_counts = {}
    for column in df.columns:
        null_count = df.filter(col(column).isNull()).count()
        null_counts[column] = null_count
        if null_count > 0:
            logger.warning(f"Column {column} has {null_count} null values")
    
    logger.info(f"Null value summary: {null_counts}")
    
    # Clean data - handle null values
    df_clean = df.na.fill("Unknown")
    logger.info("Data cleaned - null values replaced with 'Unknown'")
    
    # Define segmentation categories and criteria
    categories = {categories}
    segmentation_criteria = {segmentation_criteria}
    
    logger.info(f"Categories to create: {categories}")
    
    # Create segments based on categories
    segment_results = {}
    
    for category in categories:
        logger.info(f"Creating segment: {category}")
        
        # Apply segmentation logic based on criteria
        segment_df = df_clean
        
        if category in segmentation_criteria:
            criteria_config = segmentation_criteria[category]
            filters = criteria_config.get('potential_filters', [])
            
            # Apply filters if available
            for filter_config in filters:
                if len(filter_config) >= 3:
                    column = filter_config[0]
                    condition = filter_config[1]
                    value = filter_config[2]
                    
                    if column in df_clean.columns:
                        if condition == 'equals':
                            segment_df = segment_df.filter(col(column) == value)
                        elif condition == 'greater_than':
                            segment_df = segment_df.filter(col(column) > value)
                        elif condition == 'less_than':
                            segment_df = segment_df.filter(col(column) < value)
                        elif condition == 'contains':
                            segment_df = segment_df.filter(col(column).contains(value))
                        elif condition == 'in':
                            segment_df = segment_df.filter(col(column).isin(value))
                        elif condition == 'not_null':
                            segment_df = segment_df.filter(col(column).isNotNull())
                        elif condition == 'is_null':
                            segment_df = segment_df.filter(col(column).isNull())
                        
                        logger.info(f"Applied filter: {column} {condition} {value}")
                    else:
                        logger.warning(f"Column {column} not found in dataset")
        else:
            # Default segmentation logic based on column names
            if category.lower() in [col.lower() for col in df_clean.columns]:
                # If category name matches a column, use that column for segmentation
                category_col = next(col for col in df_clean.columns if col.lower() == category.lower())
                segment_df = segment_df.groupBy(category_col).count()
                logger.info(f"Using column {category_col} for segmentation")
            else:
                # Create a simple segment with all data
                segment_df = df_clean
                logger.info("No specific criteria found, using all data for segment")
        
        # Save segment to S3
        segment_output_path = f"{s3_output_path}/segments/{category}"
        logger.info(f"Saving segment to: {segment_output_path}")
        
        segment_df.write.mode("overwrite").csv(segment_output_path, header=True)
        
        segment_count = segment_df.count()
        segment_results[category] = segment_count
        logger.info(f"Segment '{category}' created with {segment_count} records")
    
    # Save summary statistics
    summary_stats = {
        "total_records": total_rows,
        "categories_created": categories,
        "segment_counts": segment_results,
        "null_value_summary": null_counts,
        "timestamp": datetime.now().isoformat(),
        "job_id": args['JOB_NAME'],
        "input_path": s3_input_path,
        "output_path": s3_output_path
    }
    
    # Write summary to S3
    summary_df = spark.createDataFrame([summary_stats])
    summary_output_path = f"{s3_output_path}/summary"
    summary_df.write.mode("overwrite").json(summary_output_path)
    
    logger.info("Segmentation job completed successfully!")
    logger.info(f"Summary: {summary_stats}")
    
except Exception as e:
    logger.error(f"Error in segmentation job: {str(e)}")
    raise e

job.commit()
''' 