#!/bin/bash
# s3_setup.sh - Script to set up AWS S3 buckets for NYC TLC Data Platform

set -e  # Exit on any error

echo "Setting up AWS S3 buckets for NYC TLC Data Platform..."

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
    echo "AWS CLI is not installed. Please install it first."
    echo "Installation instructions: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html"
    exit 1
fi

# Check if AWS credentials are configured
if ! aws sts get-caller-identity &> /dev/null; then
    echo "AWS credentials not found or invalid. Please configure your AWS CLI."
    echo "Run: aws configure"
    exit 1
fi

# Get AWS region from environment or use default
AWS_REGION=${AWS_DEFAULT_REGION:-us-east-1}
PROJECT_NAME="nyc-tlc-data-platform"
TIMESTAMP=$(date +%Y%m%d%H%M%S)

# Generate unique bucket names
RAW_BUCKET_NAME="${PROJECT_NAME}-raw-${AWS_REGION}-${TIMESTAMP}"
PROCESSED_BUCKET_NAME="${PROJECT_NAME}-processed-${AWS_REGION}-${TIMESTAMP}"

echo "Using AWS region: $AWS_REGION"
echo "Creating S3 buckets..."
echo "Raw data bucket: $RAW_BUCKET_NAME"
echo "Processed data bucket: $PROCESSED_BUCKET_NAME"

# Create S3 buckets
aws s3 mb s3://$RAW_BUCKET_NAME --region $AWS_REGION
aws s3 mb s3://$PROCESSED_BUCKET_NAME --region $AWS_REGION

echo "Buckets created successfully!"

# Configure bucket policies for better security and lifecycle management
echo "Configuring bucket policies..."

# Enable versioning on raw bucket
aws s3api put-bucket-versioning --bucket $RAW_BUCKET_NAME --versioning-configuration Status=Enabled

# Enable versioning on processed bucket
aws s3api put-bucket-versioning --bucket $PROCESSED_BUCKET_NAME --versioning-configuration Status=Enabled

# Set up lifecycle policy to archive old objects (after 30 days to IA, 90 days to Glacier, 365 days to delete)
cat > lifecycle_policy.json << EOF
{
    "Rules": [
        {
            "ID": "ArchiveOldVersions",
            "Status": "Enabled",
            "Filter": {
                "Prefix": ""
            },
            "NoncurrentVersionTransitions": [
                {
                    "NoncurrentDays": 30,
                    "StorageClass": "STANDARD_IA"
                },
                {
                    "NoncurrentDays": 90,
                    "StorageClass": "GLACIER"
                }
            ],
            "NoncurrentVersionExpiration": {
                "NoncurrentDays": 365
            }
        }
    ]
}
EOF

aws s3api put-bucket-lifecycle-configuration --bucket $RAW_BUCKET_NAME --lifecycle-configuration file://lifecycle_policy.json
aws s3api put-bucket-lifecycle-configuration --bucket $PROCESSED_BUCKET_NAME --lifecycle-configuration file://lifecycle_policy.json

# Clean up temporary file
rm lifecycle_policy.json

# Create a configuration file with bucket names
cat > s3_config.json << EOF
{
    "raw_bucket": "$RAW_BUCKET_NAME",
    "processed_bucket": "$PROCESSED_BUCKET_NAME",
    "region": "$AWS_REGION",
    "created_at": "$(date -u)"
}
EOF

echo "S3 setup completed!"
echo "Configuration saved to s3_config.json"
echo ""
echo "To update your .env file with these bucket names:"
echo "sed -i \"s/S3_RAW_BUCKET=.*/S3_RAW_BUCKET=$RAW_BUCKET_NAME/\" .env"
echo "sed -i \"s/S3_PROCESSED_BUCKET=.*/S3_PROCESSED_BUCKET=$PROCESSED_BUCKET_NAME/\" .env"