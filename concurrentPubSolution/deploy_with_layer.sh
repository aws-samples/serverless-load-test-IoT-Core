#!/bin/bash
set -e

# Configuration
FUNCTION_NAME="IoT_ConcurrentPublisher"
REGION="ap-northeast-1"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
CUSTOM_LAYER_ARN="arn:aws:lambda:ap-northeast-1:123456789012:layer:loadtest0601_awsiotsdk:1"
IOT_ENDPOINT="xxxxxx-ats.iot.ap-northeast-1.amazonaws.com"  # Corrected IoT endpoint

echo "Deploying Lambda function with custom AWS IoT SDK Layer"

# Create deployment package
echo "Creating deployment package..."
zip -r function.zip lambda_function.py

# Update Lambda function
echo "Updating Lambda function code..."
aws lambda update-function-code \
    --function-name $FUNCTION_NAME \
    --zip-file fileb://function.zip \
    --region $REGION

# Wait for the code update to complete
echo "Waiting for code update to complete..."
sleep 10

# Update Lambda configuration to use the custom layer and set environment variables
echo "Updating Lambda configuration to use the custom IoT SDK layer and environment variables..."
aws lambda update-function-configuration \
    --function-name $FUNCTION_NAME \
    --layers $CUSTOM_LAYER_ARN \
    --timeout 300 \
    --memory-size 2048 \
    --environment "Variables={IOT_ENDPOINT=$IOT_ENDPOINT}" \
    --region $REGION

echo "Deployment completed successfully!"
echo "You can now test your Lambda function with the AWS IoT SDK layer."
