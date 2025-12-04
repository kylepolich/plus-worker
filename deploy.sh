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

# Get CodeArtifact auth token for pip
echo "Getting CodeArtifact auth token..."
CODEARTIFACT_AUTH_TOKEN=$(aws codeartifact get-authorization-token \
    --domain plus \
    --domain-owner "$ACCOUNT_ID" \
    --region "$REGION" \
    --query authorizationToken \
    --output text)

# Write token to temp file for BuildKit secret (more secure than --build-arg)
TOKEN_FILE=$(mktemp)
echo -n "$CODEARTIFACT_AUTH_TOKEN" > "$TOKEN_FILE"
trap "rm -f $TOKEN_FILE" EXIT

# Build for linux/amd64 (Fargate default) using BuildKit secrets
echo "Building image (linux/amd64)..."
DOCKER_BUILDKIT=1 docker build --platform linux/amd64 \
    --secret id=codeartifact_token,src="$TOKEN_FILE" \
    --build-arg AWS_ACCOUNT_ID="$ACCOUNT_ID" \
    -t "$REPO_NAME" "$SCRIPT_DIR"

# Tag and push
echo "Pushing to ECR..."
docker tag "$REPO_NAME:latest" "$ECR_URI:latest"
docker push "$ECR_URI:latest"

echo ""
echo "Done! Image pushed to $ECR_URI:latest"

# Register actions in DynamoDB
echo ""
echo "Registering actions in DynamoDB..."

# Load runtime env vars
if [ -f "$SCRIPT_DIR/.env" ]; then
    source "$SCRIPT_DIR/.env"
fi

if [ -z "$DYNAMO_TABLE" ]; then
    echo "Warning: DYNAMO_TABLE not set in .env, skipping action registration"
else
    docker run --rm \
        -e RUN_MODE=REGISTER_ACTIONS \
        -e REGION="$REGION" \
        -e ACCESS_KEY="$ACCESS_KEY" \
        -e SECRET_KEY="$SECRET_KEY" \
        -e DYNAMO_TABLE="$DYNAMO_TABLE" \
        "$REPO_NAME:latest"
fi

echo ""
echo "To run a test task:"
echo "  ./run-task.sh"
