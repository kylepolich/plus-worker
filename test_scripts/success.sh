#!/bin/bash
# Build and run plus-worker container with all required env vars
# Expected: runs for 20 seconds then exits 0

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "Building plus-worker container..."
docker build -t plus-worker:test "$PROJECT_DIR"

echo ""
echo "Running container with all required env vars..."
echo "================================================"

docker run --rm \
  -e RUN_MODE=RUN_ACTION \
  -e JOB_ID=test-job-123 \
  -e USERNAME=testuser \
  -e ACCESS_KEY=AKIAIOSFODNN7EXAMPLE \
  -e SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
  -e DYNAMO_TABLE=plus-jobs \
  -e DYNAMO_STREAMS_TABLE=plus-streams \
  -e PRIMARY_BUCKET=plus-data-bucket \
  -e ACTION_ID=test.actions.SampleAction \
  plus-worker:test

echo ""
echo "Container exited with code: $?"
