#!/bin/bash
# Rebuild and push plus-worker image to ECR
# Run this after code changes

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Load saved config
if [ ! -f "$SCRIPT_DIR/.env.deploy" ]; then
    echo "Error: .env.deploy not found. Run setup.sh first."
    exit 1
fi

source "$SCRIPT_DIR/.env.deploy"

echo "Deploying plus-worker to ECR..."
echo "  Region: $REGION"
echo "  ECR URI: $ECR_URI"
echo ""

# Login to ECR
echo "Logging into ECR..."
aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com"

# Build for linux/amd64 (Fargate default)
echo "Building image (linux/amd64)..."
docker build --platform linux/amd64 -t "$REPO_NAME" "$SCRIPT_DIR"

# Tag and push
echo "Pushing to ECR..."
docker tag "$REPO_NAME:latest" "$ECR_URI:latest"
docker push "$ECR_URI:latest"

echo ""
echo "Done! Image pushed to $ECR_URI:latest"
echo ""
echo "To run a test task:"
echo "  ./run-task.sh"
