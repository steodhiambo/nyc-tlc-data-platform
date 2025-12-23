#!/bin/bash
# download_tlc_data.sh - Script to download NYC TLC data

set -e  # Exit on any error

echo "NYC TLC Data Downloader"
echo "========================"

# Check if required environment variables are set
if [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
    echo "Error: AWS credentials not set. Please configure your AWS CLI."
    echo "Run: aws configure"
    exit 1
fi

# Get S3 bucket from environment or use default
S3_BUCKET=${S3_RAW_BUCKET:-"nyc-tlc-raw-data-us-east-1"}

echo "Using S3 bucket: $S3_BUCKET"

# Default values
DATA_TYPE="yellow"
START_YEAR=""
START_MONTH=""
END_YEAR=""
END_MONTH=""
LATEST_MONTHS=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --type)
            DATA_TYPE="$2"
            shift 2
            ;;
        --start-year)
            START_YEAR="$2"
            shift 2
            ;;
        --start-month)
            START_MONTH="$2"
            shift 2
            ;;
        --end-year)
            END_YEAR="$2"
            shift 2
            ;;
        --end-month)
            END_MONTH="$2"
            shift 2
            ;;
        --latest)
            LATEST_MONTHS="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --type TYPE          Data type: yellow or green (default: yellow)"
            echo "  --start-year YEAR    Start year for data download"
            echo "  --start-month MONTH  Start month for data download"
            echo "  --end-year YEAR      End year for data download"
            echo "  --end-month MONTH    End month for data download"
            echo "  --latest MONTHS      Download latest N months of data"
            echo "  --help               Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0 --type yellow --latest 3"
            echo "  $0 --type green --start-year 2023 --start-month 1 --end-year 2023 --end-month 12"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Validate data type
if [[ "$DATA_TYPE" != "yellow" && "$DATA_TYPE" != "green" ]]; then
    echo "Error: Invalid data type. Use 'yellow' or 'green'."
    exit 1
fi

# Check if Python script exists
if [ ! -f "scripts/tlc_data_downloader.py" ]; then
    echo "Error: scripts/tlc_data_downloader.py not found."
    exit 1
fi

# Prepare command
CMD="python scripts/tlc_data_downloader.py --type $DATA_TYPE --s3-bucket $S3_BUCKET"

if [ -n "$LATEST_MONTHS" ]; then
    CMD="$CMD --latest $LATEST_MONTHS"
elif [ -n "$START_YEAR" ] && [ -n "$START_MONTH" ] && [ -n "$END_YEAR" ] && [ -n "$END_MONTH" ]; then
    CMD="$CMD --start-year $START_YEAR --start-month $START_MONTH --end-year $END_YEAR --end-month $END_MONTH"
else
    echo "Error: You must specify either --latest or all four date parameters (start-year, start-month, end-year, end-month)."
    exit 1
fi

echo "Running command: $CMD"
echo ""

# Execute the download
eval $CMD

echo ""
echo "Download process completed!"