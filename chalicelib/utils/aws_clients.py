import boto3
import os
from typing import Optional

class AWSClients:
    """Centralized AWS clients configuration"""
    
    def __init__(self, region_name: Optional[str] = None):
        self.region_name = region_name or os.environ.get("AWS_REGION_NAME", "eu-west-1")
        self.session = boto3.Session()
        
        # Initialize AWS clients
        self.glue_client = boto3.client("glue", region_name=self.region_name)
        self.s3_client = boto3.client("s3", region_name=self.region_name)
        self.bedrock_client = boto3.client("bedrock-runtime", region_name=self.region_name)
        self.dynamodb = boto3.resource("dynamodb", region_name=self.region_name)
    
    def get_dynamodb_table(self, table_name: str):
        """Get DynamoDB table instance"""
        return self.dynamodb.Table(table_name)
    
    def get_bedrock_client(self, region_name: Optional[str] = None):
        """Get Bedrock client for specific region"""
        bedrock_region = region_name or self.region_name
        return boto3.client("bedrock-runtime", region_name=bedrock_region)

# Global instance
aws_clients = AWSClients() 