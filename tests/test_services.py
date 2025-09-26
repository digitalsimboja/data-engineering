"""
Example test file for the modular services
This demonstrates how to test each service independently
"""

import pytest
from unittest.mock import Mock, patch
from chalicelib.services.glue_service import GlueService
from chalicelib.services.dynamodb_service import DynamoDBService
from chalicelib.services.s3_service import S3Service
from chalicelib.services.bedrock_service import BedrockService
from chalicelib.utils.validators import ValidationError


class TestGlueService:
    """Test cases for GlueService"""
    
    def setup_method(self):
        """Setup test fixtures"""
        self.glue_service = GlueService()
        self.glue_service.glue_client = Mock()
    
    def test_start_categorization_job_success(self):
        """Test successful categorization job start"""
        # Mock the Glue client response
        mock_response = {"JobRunId": "jr_123456789"}
        self.glue_service.glue_client.start_job_run.return_value = mock_response
        
        # Test the method
        result = self.glue_service.start_categorization_job(
            "s3://bucket/file.csv", 
            "lambda-function"
        )
        
        # Assertions
        assert result["status"] == "STARTED"
        assert result["jobRunId"] == "jr_123456789"
        self.glue_service.glue_client.start_job_run.assert_called_once()
    
    def test_start_categorization_job_not_found(self):
        """Test categorization job when Glue job doesn't exist"""
        # Mock EntityNotFoundException
        self.glue_service.glue_client.start_job_run.side_effect = \
            self.glue_service.glue_client.exceptions.EntityNotFoundException({}, "Job not found")
        
        # Test that exception is raised
        with pytest.raises(Exception) as exc_info:
            self.glue_service.start_categorization_job(
                "s3://bucket/file.csv", 
                "lambda-function"
            )
        
        assert "does not exist" in str(exc_info.value)


class TestDynamoDBService:
    """Test cases for DynamoDBService"""
    
    def setup_method(self):
        """Setup test fixtures"""
        self.dynamodb_service = DynamoDBService("test-table")
        self.dynamodb_service.table = Mock()
    
    def test_store_categorization_results_success(self):
        """Test successful storage of categorization results"""
        # Mock the table put_item method
        self.dynamodb_service.table.put_item.return_value = None
        
        # Test data
        categorization_result = {
            "suggested_categories": ["category1", "category2"],
            "reasoning": "Test reasoning",
            "segmentation_criteria": {"criteria": "test"}
        }
        
        # Test the method
        file_id = self.dynamodb_service.store_categorization_results(
            file_name="test.csv",
            categorization_result=categorization_result,
            glue_script="print('test')",
            script_path="s3://bucket/script.py",
            data_count=100,
            schema=["col1", "col2"],
            job_name="test-job"
        )
        
        # Assertions
        assert file_id is not None
        assert "test.csv" in file_id
        self.dynamodb_service.table.put_item.assert_called_once()
    
    def test_get_latest_categorization_results_success(self):
        """Test successful retrieval of latest categorization results"""
        # Mock scan response
        mock_items = [
            {"timestamp": "2023-01-01", "data": "old"},
            {"timestamp": "2023-01-02", "data": "new"}
        ]
        self.dynamodb_service.table.scan.return_value = {"Items": mock_items}
        
        # Test the method
        result = self.dynamodb_service.get_latest_categorization_results("test-job")
        
        # Assertions
        assert result["data"] == "new"  # Should return the latest item
        self.dynamodb_service.table.scan.assert_called_once()


class TestS3Service:
    """Test cases for S3Service"""
    
    def setup_method(self):
        """Setup test fixtures"""
        self.s3_service = S3Service()
        self.s3_service.s3_client = Mock()
    
    def test_validate_file_exists_success(self):
        """Test successful file validation"""
        # Mock successful head_object call
        self.s3_service.s3_client.head_object.return_value = {}
        
        # Test the method
        self.s3_service.validate_file_exists("s3://bucket/file.csv")
        
        # Assertions
        self.s3_service.s3_client.head_object.assert_called_once_with(
            Bucket="bucket", Key="file.csv"
        )
    
    def test_validate_file_exists_not_found(self):
        """Test file validation when file doesn't exist"""
        # Mock NoSuchKey exception
        self.s3_service.s3_client.head_object.side_effect = \
            self.s3_service.s3_client.exceptions.NoSuchKey({}, "File not found")
        
        # Test that ValidationError is raised
        with pytest.raises(ValidationError) as exc_info:
            self.s3_service.validate_file_exists("s3://bucket/file.csv")
        
        assert "does not exist" in str(exc_info.value)
    
    def test_upload_script_success(self):
        """Test successful script upload"""
        # Mock successful put_object call
        self.s3_service.s3_client.put_object.return_value = {}
        
        # Test the method
        script_path = self.s3_service.upload_script(
            "print('test')", 
            "test-bucket", 
            "scripts/test.py"
        )
        
        # Assertions
        assert script_path == "s3://test-bucket/scripts/test.py"
        self.s3_service.s3_client.put_object.assert_called_once_with(
            Bucket="test-bucket",
            Key="scripts/test.py",
            Body="print('test')",
            ContentType='text/x-python'
        )


class TestBedrockService:
    """Test cases for BedrockService"""
    
    def setup_method(self):
        """Setup test fixtures"""
        self.bedrock_service = BedrockService()
        self.bedrock_service.bedrock_client = Mock()
    
    def test_categorize_data_success(self):
        """Test successful data categorization"""
        # Mock Bedrock response
        mock_response = {
            'body': Mock()
        }
        mock_response['body'].read.return_value = b'{"content": [{"text": "{\\"suggested_categories\\": [\\"cat1\\"]}"}]}'
        
        self.bedrock_service.bedrock_client.invoke_model.return_value = mock_response
        
        # Test data
        sample_data = [{"col1": "value1"}]
        schema = ["col1"]
        
        # Test the method
        result = self.bedrock_service.categorize_data(sample_data, schema, "test.csv")
        
        # Assertions
        assert "suggested_categories" in result
        assert result["suggested_categories"] == ["cat1"]
        self.bedrock_service.bedrock_client.invoke_model.assert_called_once()
    
    def test_generate_glue_script_success(self):
        """Test successful Glue script generation"""
        # Mock Bedrock response
        mock_response = {
            'body': Mock()
        }
        mock_response['body'].read.return_value = b'{"content": [{"text": "```python\\nprint(\\"test\\")\\n```"}]}'
        
        self.bedrock_service.bedrock_client.invoke_model.return_value = mock_response
        
        # Test data
        schema = ["col1"]
        categories = ["cat1"]
        criteria = {"criteria": "test"}
        sample_data = [{"col1": "value1"}]
        
        # Test the method
        script = self.bedrock_service.generate_glue_script(schema, categories, criteria, sample_data)
        
        # Assertions
        assert script == 'print("test")'
        self.bedrock_service.bedrock_client.invoke_model.assert_called_once()


class TestValidators:
    """Test cases for validation utilities"""
    
    def test_validate_s3_path_success(self):
        """Test successful S3 path validation"""
        from chalicelib.utils.validators import validate_s3_path
        
        bucket, key = validate_s3_path("s3://bucket/file.csv")
        
        assert bucket == "bucket"
        assert key == "file.csv"
    
    def test_validate_s3_path_invalid_format(self):
        """Test S3 path validation with invalid format"""
        from chalicelib.utils.validators import validate_s3_path
        
        with pytest.raises(ValidationError) as exc_info:
            validate_s3_path("invalid/path")
        
        assert "must start with 's3://'" in str(exc_info.value)
    
    def test_validate_s3_path_empty(self):
        """Test S3 path validation with empty path"""
        from chalicelib.utils.validators import validate_s3_path
        
        with pytest.raises(ValidationError) as exc_info:
            validate_s3_path("")
        
        assert "required" in str(exc_info.value)


if __name__ == "__main__":
    pytest.main([__file__]) 