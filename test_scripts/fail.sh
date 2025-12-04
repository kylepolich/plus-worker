#!/bin/bash
# Build and run plus-worker container with MISSING env vars
# Expected: fails immediately with exit 1 and lists missing vars

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "Building plus-worker container..."
docker build -t plus-worker:test "$PROJECT_DIR"

echo ""
echo "Running container with MISSING env vars..."
echo "================================================"

# Only pass RUN_MODE and JOB_ID - missing everything else
docker run --rm \
  -e RUN_MODE=RUN_ACTION \
  -e JOB_ID=test-job-123 \
  plus-worker:test

echo ""
echo "Container exited with code: $?"
