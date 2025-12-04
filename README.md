# plus-worker

AWS Fargate worker for executing long-running PlusScript jobs. This service handles compute-intensive tasks that exceed Lambda's 15-minute timeout, such as batch processing, media transcoding, and large dataset operations.


## Architecture

```
┌─────────────────┐         ┌─────────────────┐         ┌─────────────────┐
│   plus-engine   │         │   plus-worker   │         │    DynamoDB     │
│    (Lambda)     │────────▶│    (Fargate)    │────────▶│   (Job State)   │
└─────────────────┘         └─────────────────┘         └─────────────────┘
        │                           │
        │  1. Create job record     │  3. Load job
        │  2. Launch Fargate task   │  4. Update status → RUNNING
        ▼                           │  5. Execute script
┌─────────────────┐                 │  6. Update status → SUCCEEDED
│    DynamoDB     │                 │
│  (Job Record)   │◀────────────────┘
└─────────────────┘
```

### How It Works

1. **plus-engine** (Lambda) receives a request to run a PlusScript
2. For long-running jobs, plus-engine creates a `PlusScriptJob` record in DynamoDB with status `INITIALIZING`
3. plus-engine launches a Fargate task, passing the job ID and credentials via environment variables
4. **plus-worker** (this service) starts in Fargate and:
   - Loads the job record from DynamoDB
   - Updates status to `RUNNING` and sets `started_at`, `fargate_task_arn`
   - Executes the PlusScript (currently test mode: sleeps 20 seconds)
   - Updates status to `SUCCEEDED` and sets `completed_at`, `updated_at`
5. plus-engine or the frontend can poll the job status to track progress

### Run Modes

| Mode | Description | Required Env Vars |
|------|-------------|-------------------|
| `RUN_SCRIPT` | Execute a standalone PlusScript | - |
| `RUN_COLLECTION` | Run a PlusScript over a collection | `COLLECTION_OWNER` |
| `RUN_ACTION` | Execute a single action (not yet implemented) | `ACTION_ID` |

## Environment Variables

### Required (all modes)

| Variable | Description |
|----------|-------------|
| `RUN_MODE` | One of: `RUN_SCRIPT`, `RUN_COLLECTION`, `RUN_ACTION` |
| `JOB_ID` | The DynamoDB object_id of the PlusScriptJob |
| `USERNAME` | User who initiated the job |
| `REGION` | AWS region (e.g., `us-east-1`) |
| `ACCESS_KEY` | AWS access key for DynamoDB/S3 |
| `SECRET_KEY` | AWS secret key |
| `DYNAMO_TABLE` | DynamoDB table for job documents |
| `DYNAMO_STREAMS_TABLE` | DynamoDB table for streams |
| `PRIMARY_BUCKET` | S3 bucket for file storage |

### Mode-specific

| Variable | Mode | Description |
|----------|------|-------------|
| `ACTION_ID` | `RUN_ACTION` | ID of the action to execute |
| `COLLECTION_OWNER` | `RUN_COLLECTION` | Owner path of the collection to process |

## Setup

### Prerequisites

- AWS CLI configured with appropriate permissions
- Docker installed
- Access to an AWS account with ECS, ECR, and CloudWatch permissions

### Initial Setup

Run the interactive setup script to create AWS infrastructure:

```bash
./setup.sh
```

This will guide you through creating:
- ECS Cluster
- ECR Repository
- CloudWatch Log Group
- ECS Task Definition
- Network configuration (VPC, subnets, security groups)

Configuration is saved to `.env.deploy` for use by other scripts.

## Deployment

### Build and Push

```bash
./deploy.sh
```

This builds the Docker image for `linux/amd64` (required for Fargate) and pushes to ECR.

### Run a Test Task

```bash
./run-task.sh
```

Launches a test Fargate task and waits for completion, then displays logs.

### View Logs

```bash
./show_logs.sh
```

Tails CloudWatch logs in real-time.

## Local Development

### Test Scripts

```bash
# Test successful execution
./test_scripts/success.sh

# Test failure handling
./test_scripts/fail.sh
```

### Docker Build

```bash
docker build --platform linux/amd64 -t plus-worker .
```

## Project Structure

```
plus-worker/
├── src/
│   └── worker.py          # Main entry point
├── feaas/                  # Shared library (DAO, protobuf objects)
│   ├── dao/                # Data access layer
│   ├── objects.py          # Generated protobuf classes
│   └── psee/               # PlusScript execution engine
├── proto/
│   └── objs.proto          # Protobuf definitions
├── Dockerfile
├── pyproject.toml
├── setup.sh                # AWS infrastructure setup
├── deploy.sh               # Build and push to ECR
├── run-task.sh             # Launch test task
└── show_logs.sh            # Tail CloudWatch logs
```

## Integration with plus-engine

plus-engine launches Fargate tasks using the `_launch_collection_task` pattern:

```python
def _launch_collection_task(self, job: PlusScriptJob, collection_owner: str):
    ecs = boto3.client('ecs', region_name=REGION)

    ecs.run_task(
        cluster='plus-worker-cluster',
        taskDefinition='plus-worker',
        launchType='FARGATE',
        networkConfiguration={...},
        overrides={
            'containerOverrides': [{
                'name': 'worker',
                'environment': [
                    {'name': 'RUN_MODE', 'value': 'RUN_COLLECTION'},
                    {'name': 'JOB_ID', 'value': job.object_id},
                    {'name': 'USERNAME', 'value': job.username},
                    {'name': 'COLLECTION_OWNER', 'value': collection_owner},
                    # ... credentials and config
                ]
            }]
        }
    )
```
