import logging
import json
from typing import Dict, Any, List

from chalicelib.utils.aws_clients import aws_clients
from chalicelib.utils.prompt_templates import get_categorization_prompt, get_script_generation_prompt

logger = logging.getLogger(__name__)

class BedrockService:
    """Service class for Amazon Bedrock operations"""
    
    def __init__(self, region_name: str = "eu-west-1"):
        self.region_name = region_name
        self.bedrock_client = aws_clients.get_bedrock_client(region_name)
        self.model_id = f"arn:aws:bedrock:{region_name}:475453938538:inference-profile/eu.anthropic.claude-3-5-sonnet-20240620-v1:0"
    
    def _create_bedrock_payload(self, prompt: str) -> Dict[str, Any]:
        """
        Create Bedrock API payload
        
        Args:
            prompt: Prompt to send to Bedrock
            
        Returns:
            Bedrock API payload
        """
        return {
            "anthropic_version": "bedrock-2023-05-31",
            "messages": [
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "max_tokens": 1024,
            "temperature": 0.1,
            "top_p": 0.9
        }
    
    def _parse_bedrock_response(self, response: Dict[str, Any]) -> str:
        """
        Parse Bedrock response to extract completion text
        
        Args:
            response: Bedrock API response
            
        Returns:
            Extracted completion text
        """
        response_str = response['body'].read().decode('utf-8')
        response_body = json.loads(response_str)
        return response_body["content"][0]["text"]
    
    def _extract_json_from_response(self, completion: str) -> Dict[str, Any]:
        """
        Extract JSON from Bedrock response
        
        Args:
            completion: Raw completion text
            
        Returns:
            Parsed JSON object
        """
        try:
            # Find JSON in the response
            start_idx = completion.find('{')
            end_idx = completion.rfind('}') + 1
            
            if start_idx != -1 and end_idx != -1:
                json_str = completion[start_idx:end_idx]
                return json.loads(json_str)
            else:
                raise ValueError("No JSON found in response")
                
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON from Bedrock response: {e}")
            raise Exception(f"Failed to parse JSON from Bedrock response: {e}")
    
    def categorize_data(self, sample_data: List[Dict[str, Any]], schema: List[str], file_name: str) -> Dict[str, Any]:
        """
        Categorize data using Amazon Bedrock
        
        Args:
            sample_data: Sample data for categorization
            schema: Data schema
            file_name: Name of the file being processed
            
        Returns:
            Categorization results
        """
        try:
            logger.info(f"Processing {len(sample_data)} sample records for file: {file_name}")
            
            # Prepare the prompt for categorization
            sample_data_str = json.dumps(sample_data[:5], indent=2)  # Use first 5 records as sample
            schema_str = json.dumps(schema, indent=2)
            
            categorization_prompt = get_categorization_prompt(sample_data_str, schema_str)
            
            # Call Bedrock with Claude model for categorization
            logger.info("Calling Amazon Bedrock for categorization...")
            
            body = self._create_bedrock_payload(categorization_prompt)
            
            response = self.bedrock_client.invoke_model(
                modelId=self.model_id,
                body=json.dumps(body),
                contentType="application/json",
                accept="application/json"
            )
            
            completion = self._parse_bedrock_response(response)
            
            # Parse the response to extract JSON
            try:
                categorization_result = self._extract_json_from_response(completion)
            except Exception as e:
                logger.warning(f"Failed to parse categorization response, using fallback: {e}")
                # Fallback: create basic categories based on schema
                categorization_result = {
                    "suggested_categories": schema[:3] if len(schema) >= 3 else schema,
                    "reasoning": "Fallback categorization based on available columns",
                    "segmentation_criteria": {}
                }
            
            return categorization_result
            
        except Exception as e:
            logger.error(f"Error in categorization: {str(e)}")
            raise Exception(f"Error in categorization: {str(e)}")
    
    def generate_glue_script(self, schema: List[str], categories: List[str], 
                           criteria: Dict[str, Any], sample_data: List[Dict[str, Any]]) -> str:
        """
        Generate Glue script using Amazon Bedrock
        
        Args:
            schema: Data schema
            categories: Suggested categories
            criteria: Segmentation criteria
            sample_data: Sample data
            
        Returns:
            Generated Glue script
        """
        try:
            # Generate a Glue script using Bedrock
            sample_data_str = json.dumps(sample_data[:5], indent=2)
            script_generation_prompt = get_script_generation_prompt(
                schema, categories, criteria, sample_data_str
            )
            
            # Call Bedrock to generate the Glue script
            logger.info("Calling Amazon Bedrock for script generation...")
            
            body = self._create_bedrock_payload(script_generation_prompt)
            
            script_response = self.bedrock_client.invoke_model(
                modelId=self.model_id,
                body=json.dumps(body),
                contentType="application/json",
                accept="application/json"
            )
            
            script_completion = self._parse_bedrock_response(script_response)
            
            # Extract the generated script (remove any markdown formatting)
            glue_script = script_completion.strip()
            if glue_script.startswith('```python'):
                glue_script = glue_script[9:]
            if glue_script.endswith('```'):
                glue_script = glue_script[:-3]
            glue_script = glue_script.strip()
            
            return glue_script
            
        except Exception as e:
            logger.error(f"Error generating Glue script: {str(e)}")
            raise Exception(f"Error generating Glue script: {str(e)}")

# Global instance
bedrock_service = BedrockService() 