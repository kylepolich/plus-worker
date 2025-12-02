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

# Poll for status
for i in {1..30}; do
    STATUS=$(aws ecs describe-tasks --cluster "$CLUSTER_NAME" --tasks "$TASK_ID" --region "$REGION" \
        --query 'tasks[0].lastStatus' --output text)

    echo "  Status: $STATUS"

    if [ "$STATUS" = "STOPPED" ]; then
        STOP_REASON=$(aws ecs describe-tasks --cluster "$CLUSTER_NAME" --tasks "$TASK_ID" --region "$REGION" \
            --query 'tasks[0].stoppedReason' --output text)
        EXIT_CODE=$(aws ecs describe-tasks --cluster "$CLUSTER_NAME" --tasks "$TASK_ID" --region "$REGION" \
            --query 'tasks[0].containers[0].exitCode' --output text)

        echo ""
        echo "Task stopped."
        echo "  Exit code: $EXIT_CODE"
        echo "  Reason: $STOP_REASON"
        break
    fi

    if [ "$STATUS" = "RUNNING" ]; then
        echo ""
        echo "Task is running! Tailing logs..."
        echo "  (Press Ctrl+C to stop watching)"
        echo ""
        aws logs tail "$LOG_GROUP" --follow --region "$REGION"
        break
    fi

    sleep 2
done

echo ""
echo "To view logs manually:"
echo "  aws logs tail $LOG_GROUP --follow --region $REGION"
