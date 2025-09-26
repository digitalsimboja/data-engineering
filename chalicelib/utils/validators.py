import logging
from typing import Dict, Any, Tuple
from chalice import Response

logger = logging.getLogger(__name__)

class ValidationError(Exception):
    """Custom validation error"""
    pass

def validate_s3_path(s3_path: str) -> Tuple[str, str]:
    """
    Validate S3 path and return bucket and key
    
    Args:
        s3_path: S3 path to validate
        
    Returns:
        Tuple of (bucket_name, key)
        
    Raises:
        ValidationError: If path is invalid
    """
    if not s3_path:
        raise ValidationError("S3 file path is required")
    
    if not s3_path.startswith("s3://"):
        raise ValidationError("S3 path must start with 's3://'")
    
    try:
        bucket_name, key = s3_path.replace("s3://", "").split("/", 1)
        return bucket_name, key
    except ValueError:
        raise ValidationError("Invalid S3 path format")

def validate_request_body(body: Dict[str, Any], required_fields: list) -> None:
    """
    Validate request body contains required fields
    
    Args:
        body: Request body dictionary
        required_fields: List of required field names
        
    Raises:
        ValidationError: If required fields are missing
    """
    for field in required_fields:
        if not body.get(field):
            raise ValidationError(f"Missing {field} parameter")

def create_error_response(error: str, details: str = "", error_type: str = "server", status_code: int = 400) -> Response:
    """
    Create standardized error response
    
    Args:
        error: Error message
        details: Additional error details
        error_type: Type of error (server, validation, etc.)
        status_code: HTTP status code
        
    Returns:
        Chalice Response object
    """
    return Response(
        body={
            "error": error,
            "details": details,
            "type": error_type
        },
        status_code=status_code
    )

def create_success_response(data: Dict[str, Any], status_code: int = 200) -> Response:
    """
    Create standardized success response
    
    Args:
        data: Response data
        status_code: HTTP status code
        
    Returns:
        Chalice Response object
    """
    return Response(
        body=data,
        status_code=status_code
    ) 