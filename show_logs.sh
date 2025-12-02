#!/bin/bash
# Tail CloudWatch logs for plus-worker

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Load saved config
if [ ! -f "$SCRIPT_DIR/.env.deploy" ]; then
    echo "Error: .env.deploy not found. Run setup.sh first."
    exit 1
fi

source "$SCRIPT_DIR/.env.deploy"

echo "Tailing logs: $LOG_GROUP"
echo "Press Ctrl+C to stop"
echo ""

aws logs tail "$LOG_GROUP" --follow --region "$REGION"
