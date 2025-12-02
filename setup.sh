#!/bin/bash
# Interactive setup script for plus-worker Fargate deployment
# Guides you through creating all required AWS resources

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_header() {
    echo ""
    echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
}

print_step() {
    echo -e "\n${YELLOW}▶ $1${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_info() {
    echo -e "  $1"
}

confirm() {
    read -p "$1 [y/N] " response
    case "$response" in
        [yY][eE][sS]|[yY]) return 0 ;;
        *) return 1 ;;
    esac
}

# ============================================================
print_header "plus-worker AWS Setup"
# ============================================================

echo "This script will guide you through setting up:"
echo "  1. ECS Cluster"
echo "  2. ECR Repository"
echo "  3. CloudWatch Log Group"
echo "  4. IAM Execution Role"
echo "  5. Task Definition"
echo "  6. Build & Push Docker Image"
echo "  7. Network Configuration"
echo ""

# ============================================================
print_header "Step 0: Prerequisites Check"
# ============================================================

print_step "Checking AWS CLI..."
if ! command -v aws &> /dev/null; then
    print_error "AWS CLI not found. Please install it first."
    exit 1
fi
print_success "AWS CLI found"

print_step "Checking Docker..."
if ! command -v docker &> /dev/null; then
    print_error "Docker not found. Please install it first."
    exit 1
fi
print_success "Docker found"

print_step "Checking AWS credentials..."
if ! aws sts get-caller-identity &> /dev/null; then
    print_error "AWS credentials not configured. Run 'aws configure' first."
    exit 1
fi
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
print_success "AWS credentials valid. Account ID: $ACCOUNT_ID"

# ============================================================
print_header "Step 1: Configuration"
# ============================================================

# Region
print_step "Select AWS region"
DEFAULT_REGION=$(aws configure get region 2>/dev/null || echo "us-east-1")
read -p "Region [$DEFAULT_REGION]: " REGION
REGION=${REGION:-$DEFAULT_REGION}
print_info "Using region: $REGION"

# Cluster name
print_step "ECS Cluster name"
DEFAULT_CLUSTER="plus-worker-cluster"
read -p "Cluster name [$DEFAULT_CLUSTER]: " CLUSTER_NAME
CLUSTER_NAME=${CLUSTER_NAME:-$DEFAULT_CLUSTER}

# ECR repo name
print_step "ECR Repository name"
DEFAULT_REPO="plus-worker"
read -p "Repository name [$DEFAULT_REPO]: " REPO_NAME
REPO_NAME=${REPO_NAME:-$DEFAULT_REPO}

# Log group
print_step "CloudWatch Log Group"
DEFAULT_LOG_GROUP="/ecs/plus-worker"
read -p "Log group [$DEFAULT_LOG_GROUP]: " LOG_GROUP
LOG_GROUP=${LOG_GROUP:-$DEFAULT_LOG_GROUP}

echo ""
echo "Configuration summary:"
echo "  Account ID:   $ACCOUNT_ID"
echo "  Region:       $REGION"
echo "  Cluster:      $CLUSTER_NAME"
echo "  ECR Repo:     $REPO_NAME"
echo "  Log Group:    $LOG_GROUP"
echo ""

if ! confirm "Proceed with these settings?"; then
    echo "Aborted."
    exit 0
fi

# ============================================================
print_header "Step 2: Create ECS Cluster"
# ============================================================

print_step "Checking if cluster exists..."
if aws ecs describe-clusters --clusters "$CLUSTER_NAME" --region "$REGION" --query 'clusters[?status==`ACTIVE`].clusterName' --output text 2>/dev/null | grep -q "$CLUSTER_NAME"; then
    print_success "Cluster '$CLUSTER_NAME' already exists"
else
    print_step "Creating ECS cluster..."
    aws ecs create-cluster --cluster-name "$CLUSTER_NAME" --region "$REGION" > /dev/null
    print_success "Cluster '$CLUSTER_NAME' created"
fi

# ============================================================
print_header "Step 3: Create ECR Repository"
# ============================================================

print_step "Checking if ECR repository exists..."
if aws ecr describe-repositories --repository-names "$REPO_NAME" --region "$REGION" &> /dev/null; then
    print_success "ECR repository '$REPO_NAME' already exists"
else
    print_step "Creating ECR repository..."
    aws ecr create-repository --repository-name "$REPO_NAME" --region "$REGION" > /dev/null
    print_success "ECR repository '$REPO_NAME' created"
fi

ECR_URI="$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$REPO_NAME"
print_info "ECR URI: $ECR_URI"

# ============================================================
print_header "Step 4: Create CloudWatch Log Group"
# ============================================================

print_step "Checking if log group exists..."
if aws logs describe-log-groups --log-group-name-prefix "$LOG_GROUP" --region "$REGION" --query 'logGroups[].logGroupName' --output text 2>/dev/null | grep -q "^${LOG_GROUP}$"; then
    print_success "Log group '$LOG_GROUP' already exists"
else
    print_step "Creating log group..."
    aws logs create-log-group --log-group-name "$LOG_GROUP" --region "$REGION"
    print_success "Log group '$LOG_GROUP' created"
fi

# ============================================================
print_header "Step 5: IAM Execution Role"
# ============================================================

EXECUTION_ROLE_NAME="ecsTaskExecutionRole"

print_step "Checking if execution role exists..."
if aws iam get-role --role-name "$EXECUTION_ROLE_NAME" &> /dev/null; then
    print_success "Role '$EXECUTION_ROLE_NAME' already exists"
else
    print_step "Creating execution role..."

    aws iam create-role --role-name "$EXECUTION_ROLE_NAME" \
        --assume-role-policy-document '{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "ecs-tasks.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }]
        }' > /dev/null

    aws iam attach-role-policy --role-name "$EXECUTION_ROLE_NAME" \
        --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy

    print_success "Role '$EXECUTION_ROLE_NAME' created and policy attached"
fi

EXECUTION_ROLE_ARN="arn:aws:iam::$ACCOUNT_ID:role/$EXECUTION_ROLE_NAME"

# ============================================================
print_header "Step 6: Network Configuration"
# ============================================================

print_step "Fetching available subnets..."
echo ""
aws ec2 describe-subnets --region "$REGION" \
    --query 'Subnets[*].[SubnetId,VpcId,AvailabilityZone,CidrBlock,Tags[?Key==`Name`].Value|[0]]' \
    --output table

echo ""
read -p "Enter subnet ID (e.g., subnet-abc123): " SUBNET_ID

if [ -z "$SUBNET_ID" ]; then
    print_error "Subnet ID is required"
    exit 1
fi

print_step "Fetching security groups for the selected subnet's VPC..."
VPC_ID=$(aws ec2 describe-subnets --subnet-ids "$SUBNET_ID" --region "$REGION" --query 'Subnets[0].VpcId' --output text)
echo ""
aws ec2 describe-security-groups --region "$REGION" \
    --filters "Name=vpc-id,Values=$VPC_ID" \
    --query 'SecurityGroups[*].[GroupId,GroupName,Description]' \
    --output table

echo ""
read -p "Enter security group ID (e.g., sg-abc123): " SECURITY_GROUP_ID

if [ -z "$SECURITY_GROUP_ID" ]; then
    print_error "Security group ID is required"
    exit 1
fi

print_success "Network config: subnet=$SUBNET_ID, sg=$SECURITY_GROUP_ID"

# ============================================================
print_header "Step 7: Create Task Definition"
# ============================================================

print_step "Creating task-definition.json..."

cat > "$SCRIPT_DIR/task-definition.json" << EOF
{
    "family": "plus-worker",
    "networkMode": "awsvpc",
    "requiresCompatibilities": ["FARGATE"],
    "cpu": "256",
    "memory": "512",
    "executionRoleArn": "$EXECUTION_ROLE_ARN",
    "containerDefinitions": [
        {
            "name": "worker",
            "image": "$ECR_URI:latest",
            "essential": true,
            "logConfiguration": {
                "logDriver": "awslogs",
                "options": {
                    "awslogs-group": "$LOG_GROUP",
                    "awslogs-region": "$REGION",
                    "awslogs-stream-prefix": "worker"
                }
            },
            "environment": []
        }
    ]
}
EOF

print_success "task-definition.json created"

print_step "Registering task definition..."
aws ecs register-task-definition --cli-input-json file://"$SCRIPT_DIR/task-definition.json" --region "$REGION" > /dev/null
print_success "Task definition registered"

# ============================================================
print_header "Step 8: Build and Push Docker Image"
# ============================================================

print_step "Logging into ECR..."
aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com"
print_success "ECR login successful"

print_step "Building Docker image (linux/amd64 for Fargate)..."
docker build --platform linux/amd64 -t "$REPO_NAME" "$SCRIPT_DIR"
print_success "Docker image built"

print_step "Tagging image..."
docker tag "$REPO_NAME:latest" "$ECR_URI:latest"
print_success "Image tagged"

print_step "Pushing image to ECR..."
docker push "$ECR_URI:latest"
print_success "Image pushed to ECR"

# ============================================================
print_header "Step 9: Save Configuration"
# ============================================================

print_step "Saving configuration to .env.deploy..."

cat > "$SCRIPT_DIR/.env.deploy" << EOF
# plus-worker deployment configuration
# Generated by setup.sh on $(date)

ACCOUNT_ID=$ACCOUNT_ID
REGION=$REGION
CLUSTER_NAME=$CLUSTER_NAME
REPO_NAME=$REPO_NAME
ECR_URI=$ECR_URI
LOG_GROUP=$LOG_GROUP
SUBNET_ID=$SUBNET_ID
SECURITY_GROUP_ID=$SECURITY_GROUP_ID
EXECUTION_ROLE_ARN=$EXECUTION_ROLE_ARN
EOF

print_success "Configuration saved to .env.deploy"

# ============================================================
print_header "Setup Complete!"
# ============================================================

echo ""
echo "All AWS resources have been created. To run a test task:"
echo ""
cat << EOF
aws ecs run-task \\
  --cluster $CLUSTER_NAME \\
  --task-definition plus-worker \\
  --launch-type FARGATE \\
  --region $REGION \\
  --network-configuration '{
    "awsvpcConfiguration": {
      "subnets": ["$SUBNET_ID"],
      "securityGroups": ["$SECURITY_GROUP_ID"],
      "assignPublicIp": "ENABLED"
    }
  }' \\
  --overrides '{
    "containerOverrides": [{
      "name": "worker",
      "environment": [
        {"name": "RUN_MODE", "value": "RUN_ACTION"},
        {"name": "ACTION_ID", "value": "test.action"},
        {"name": "JOB_ID", "value": "test-123"},
        {"name": "USERNAME", "value": "testuser"},
        {"name": "ACCESS_KEY", "value": "your-access-key"},
        {"name": "SECRET_KEY", "value": "your-secret-key"},
        {"name": "DYNAMO_TABLE", "value": "your-table"},
        {"name": "DYNAMO_STREAMS_TABLE", "value": "your-streams-table"},
        {"name": "PRIMARY_BUCKET", "value": "your-bucket"}
      ]
    }]
  }'
EOF

echo ""
echo "To watch logs:"
echo "  aws logs tail $LOG_GROUP --follow --region $REGION"
echo ""
