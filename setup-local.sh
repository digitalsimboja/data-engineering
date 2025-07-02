#!/bin/bash

echo "Setting up local development environment for Chalice API..."

# Check if .env file exists
if [ ! -f .env ]; then
    echo "Creating .env file..."
    cat > .env << EOF
# AWS Profile
AWS_PROFILE=dev
AWS_REGION=eu-west-1

# Glue Job Names
CATEGORIZATION_GLUE_JOB=data-categorization-job
SEGMENTATION_GLUE_JOB=data-segmentation-job

# Development Settings
MOCK_MODE=false
EOF
    echo "Created .env file. Please update it with your AWS credentials."
else
    echo ".env file already exists."
fi

# Install dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt

echo "Setup complete!"
echo ""
echo "Next steps:"
echo "1. Update .env file with your AWS credentials"
echo "2. Run: chalice local --host 0.0.0.0 --port 8000"
echo "3. Test with: curl http://localhost:8000/" 