#!/bin/bash
# Run a test task on Fargate

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Load saved config
if [ ! -f "$SCRIPT_DIR/.env.deploy" ]; then
    echo "Error: .env.deploy not found. Run setup.sh first."
    exit 1
fi

source "$SCRIPT_DIR/.env.deploy"

echo "Running test task on Fargate..."

TASK_OUTPUT=$(aws ecs run-task \
  --cluster "$CLUSTER_NAME" \
  --task-definition plus-worker \
  --launch-type FARGATE \
  --region "$REGION" \
  --network-configuration "{
    \"awsvpcConfiguration\": {
      \"subnets\": [\"$SUBNET_ID\"],
      \"securityGroups\": [\"$SECURITY_GROUP_ID\"],
      \"assignPublicIp\": \"ENABLED\"
    }
  }" \
  --overrides '{
    "containerOverrides": [{
      "name": "worker",
      "environment": [
        {"name": "RUN_MODE", "value": "RUN_ACTION"},
        {"name": "ACTION_ID", "value": "test.action"},
        {"name": "JOB_ID", "value": "test-'$(date +%s)'"},
        {"name": "USERNAME", "value": "testuser"},
        {"name": "REGION", "value": "'"$REGION"'"},
        {"name": "ACCESS_KEY", "value": "test-access-key"},
        {"name": "SECRET_KEY", "value": "test-secret-key"},
        {"name": "DYNAMO_TABLE", "value": "test-table"},
        {"name": "DYNAMO_STREAMS_TABLE", "value": "test-streams"},
        {"name": "PRIMARY_BUCKET", "value": "test-bucket"}
      ]
    }]
  }')

TASK_ARN=$(echo "$TASK_OUTPUT" | grep -o '"taskArn": "[^"]*"' | head -1 | cut -d'"' -f4)
TASK_ID=$(echo "$TASK_ARN" | rev | cut -d'/' -f1 | rev)

echo "Task started: $TASK_ID"
echo ""
echo "Waiting for task to start..."
sleep 5

# Wait for task to complete
echo "Waiting for task to complete..."
aws ecs wait tasks-stopped --cluster "$CLUSTER_NAME" --tasks "$TASK_ID" --region "$REGION"

# Get final status
STOP_REASON=$(aws ecs describe-tasks --cluster "$CLUSTER_NAME" --tasks "$TASK_ID" --region "$REGION" \
    --query 'tasks[0].stoppedReason' --output text)
EXIT_CODE=$(aws ecs describe-tasks --cluster "$CLUSTER_NAME" --tasks "$TASK_ID" --region "$REGION" \
    --query 'tasks[0].containers[0].exitCode' --output text)

echo ""
echo "Task completed."
echo "  Exit code: $EXIT_CODE"
echo "  Reason: $STOP_REASON"

# Fetch logs for this specific task (filter by task ID in log stream name)
echo ""
echo "=== Task Logs ==="
aws logs filter-log-events \
    --log-group-name "$LOG_GROUP" \
    --log-stream-name-prefix "worker/worker/$TASK_ID" \
    --region "$REGION" \
    --query 'events[*].message' \
    --output text
