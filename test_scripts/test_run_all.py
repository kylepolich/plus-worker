#!/usr/bin/env python3
"""
Test script for run_on_collection functionality.

Creates a mock PlusScriptJob, saves it to DynamoDB, launches Fargate,
waits for completion, and cleans up.

Usage:
    python test_scripts/test_run_all.py

Requires environment variables or .env file with:
    - ACCESS_KEY, SECRET_KEY, REGION
    - DYNAMO_TABLE, DYNAMO_STREAMS_TABLE, PRIMARY_BUCKET
    - FARGATE_CLUSTER, FARGATE_TASK_DEFINITION, FARGATE_SUBNETS, FARGATE_SECURITY_GROUPS
"""
import boto3
import json
import os
import sys
import time
import uuid
from pathlib import Path

# Add project root to path for imports
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


def load_env():
    """Load environment from .env and .env.deploy files if present."""
    # Load .env first (AWS credentials, DynamoDB config)
    env_file = PROJECT_ROOT / '.env'
    if env_file.exists():
        print(f"Loading environment from {env_file}")
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ.setdefault(key.strip(), value.strip())

    # Load .env.deploy (Fargate config)
    deploy_file = PROJECT_ROOT / '.env.deploy'
    if deploy_file.exists():
        print(f"Loading Fargate config from {deploy_file}")
        with open(deploy_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ.setdefault(key.strip(), value.strip())


def get_env_or_fail(key):
    """Get environment variable or exit with error."""
    val = os.environ.get(key)
    if not val:
        print(f"ERROR: Missing required environment variable: {key}")
        sys.exit(1)
    return val


def create_no_op_script():
    """Create a PlusScript that does nothing (no action nodes)."""
    return {
        "object_id": f"test/script.{uuid.uuid4()}",
        "owner": "test",
        "label": "No-Op Test Script",
        "nodes": [],  # No action nodes = no-op
        "links": [],
        "inputs": [],
        "outputs": []
    }


def create_mock_job(hostname, username, collection_key):
    """Create a mock PlusScriptJob for testing run_on_collection."""
    job_uuid = str(uuid.uuid4())
    job_object_id = f'{hostname}/{username}/job.{job_uuid}'
    owner = f'{hostname}/{username}/collection.{collection_key}'
    now_ms = int(time.time() * 1000)

    job_dict = {
        'object_id': job_object_id,
        'owner': owner,
        'label': f'Test: Run on collection {collection_key}',
        'username': username,
        'script': create_no_op_script(),
        'status': 'PENDING',
        'created_at': now_ms,
        'updated_at': now_ms,
        'action_count': 0,
        'request_cancel': False,
        'error_message': '',
        # Extra fields for worker
        'hostname': hostname,
        'collection_key': collection_key,
        'job_type': 'run_on_collection',
        'input_data': {}
    }

    return job_object_id, job_dict


def main():
    print("=" * 60)
    print("TEST: run_on_collection via Fargate")
    print("=" * 60)

    # Load environment
    load_env()

    # Configuration
    hostname = 'plus_dataskeptic_com'
    username = 'kyle@dataskeptic.com'
    collection_key = 'arxiv_ratings'
    owner = f'{hostname}/{username}/collection.{collection_key}'

    print(f"\nConfiguration:")
    print(f"  hostname: {hostname}")
    print(f"  username: {username}")
    print(f"  collection_key: {collection_key}")
    print(f"  owner: {owner}")

    # Get required environment variables
    region = get_env_or_fail('REGION')
    access_key = get_env_or_fail('ACCESS_KEY')
    secret_key = get_env_or_fail('SECRET_KEY')
    dynamo_table = get_env_or_fail('DYNAMO_TABLE')
    dynamo_streams_table = get_env_or_fail('DYNAMO_STREAMS_TABLE')
    primary_bucket = get_env_or_fail('PRIMARY_BUCKET')

    # Fargate config (from .env.deploy or environment)
    cluster = os.environ.get('FARGATE_CLUSTER', 'plus-worker-cluster')
    task_definition = os.environ.get('FARGATE_TASK_DEFINITION', 'plus-worker')
    subnets = os.environ.get('FARGATE_SUBNETS', os.environ.get('SUBNET_ID', '')).split(',')
    security_groups = os.environ.get('FARGATE_SECURITY_GROUPS', os.environ.get('SECURITY_GROUP_ID', '')).split(',')
    container_name = os.environ.get('FARGATE_CONTAINER_NAME', 'worker')
    log_group = os.environ.get('LOG_GROUP', '/ecs/plus-worker')

    print(f"\nFargate config:")
    print(f"  cluster: {cluster}")
    print(f"  task_definition: {task_definition}")
    print(f"  subnets: {subnets}")
    print(f"  security_groups: {security_groups}")

    if not subnets[0] or not security_groups[0]:
        print("\nERROR: Missing FARGATE_SUBNETS or FARGATE_SECURITY_GROUPS")
        print("Load .env.deploy with: source .env.deploy")
        sys.exit(1)

    # Initialize boto3 clients
    dynamodb = boto3.resource(
        'dynamodb',
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    ecs = boto3.client(
        'ecs',
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    logs = boto3.client(
        'logs',
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    table = dynamodb.Table(dynamo_table)

    # Step 1: Create mock job
    print("\n" + "=" * 60)
    print("STEP 1: Creating mock job...")
    print("=" * 60)

    job_object_id, job_dict = create_mock_job(hostname, username, collection_key)
    print(f"  job_object_id: {job_object_id}")

    # Step 2: Save job to DynamoDB
    print("\n" + "=" * 60)
    print("STEP 2: Saving job to DynamoDB...")
    print("=" * 60)

    # Convert any remaining non-serializable types
    # Note: feaas uses 'pk' as the partition key name
    job_json = json.loads(json.dumps(job_dict, default=str))
    job_json['pk'] = job_object_id  # Ensure pk is set
    table.put_item(Item=job_json)
    print(f"  Saved to table: {dynamo_table}")

    # Step 3: Launch Fargate task
    print("\n" + "=" * 60)
    print("STEP 3: Launching Fargate task...")
    print("=" * 60)

    try:
        response = ecs.run_task(
            cluster=cluster,
            taskDefinition=task_definition,
            launchType='FARGATE',
            networkConfiguration={
                'awsvpcConfiguration': {
                    'subnets': subnets,
                    'securityGroups': security_groups,
                    'assignPublicIp': 'ENABLED'
                }
            },
            overrides={
                'containerOverrides': [{
                    'name': container_name,
                    'environment': [
                        {'name': 'RUN_MODE', 'value': 'RUN_JOB'},
                        {'name': 'JOB_ID', 'value': job_object_id},
                        {'name': 'USERNAME', 'value': username},
                        {'name': 'REGION', 'value': region},
                        {'name': 'ACCESS_KEY', 'value': access_key},
                        {'name': 'SECRET_KEY', 'value': secret_key},
                        {'name': 'DYNAMO_TABLE', 'value': dynamo_table},
                        {'name': 'DYNAMO_STREAMS_TABLE', 'value': dynamo_streams_table},
                        {'name': 'PRIMARY_BUCKET', 'value': primary_bucket}
                    ]
                }]
            }
        )

        if not response.get('tasks'):
            print(f"  ERROR: Failed to launch task: {response.get('failures')}")
            sys.exit(1)

        task_arn = response['tasks'][0]['taskArn']
        task_id = task_arn.split('/')[-1]
        print(f"  task_arn: {task_arn}")
        print(f"  task_id: {task_id}")

    except Exception as e:
        print(f"  ERROR: {e}")
        # Cleanup job
        table.delete_item(Key={'pk': job_object_id})
        sys.exit(1)

    # Step 4: Wait for task to complete
    print("\n" + "=" * 60)
    print("STEP 4: Waiting for task to complete...")
    print("=" * 60)

    waiter = ecs.get_waiter('tasks_stopped')
    try:
        waiter.wait(
            cluster=cluster,
            tasks=[task_id],
            WaiterConfig={'Delay': 5, 'MaxAttempts': 120}  # 10 minutes max
        )
    except Exception as e:
        print(f"  Waiter error: {e}")

    # Get final task status
    task_info = ecs.describe_tasks(cluster=cluster, tasks=[task_id])
    if task_info.get('tasks'):
        task = task_info['tasks'][0]
        stop_reason = task.get('stoppedReason', 'Unknown')
        exit_code = task.get('containers', [{}])[0].get('exitCode', 'Unknown')
        print(f"  Task stopped")
        print(f"  Exit code: {exit_code}")
        print(f"  Reason: {stop_reason}")

    # Step 5: Fetch and display logs
    print("\n" + "=" * 60)
    print("STEP 5: Fetching logs...")
    print("=" * 60)

    time.sleep(2)  # Give CloudWatch a moment to catch up

    try:
        log_events = logs.filter_log_events(
            logGroupName=log_group,
            logStreamNamePrefix=f'worker/{container_name}/{task_id}',
            limit=200
        )

        print("\n--- Task Logs ---")
        for event in log_events.get('events', []):
            print(event['message'].rstrip())
        print("--- End Logs ---\n")

    except Exception as e:
        print(f"  Could not fetch logs: {e}")

    # Step 6: Check job status in DynamoDB
    print("\n" + "=" * 60)
    print("STEP 6: Checking final job status...")
    print("=" * 60)

    # Try different key schemas - table might use 'pk' or 'object_id'
    final_job = {}
    for key_name in ['pk', 'object_id']:
        try:
            result = table.get_item(Key={key_name: job_object_id})
            if 'Item' in result:
                final_job = result['Item']
                break
        except Exception:
            continue
    print(f"  status: {final_job.get('status', 'Unknown')}")
    print(f"  percent: {final_job.get('percent', 0)}%")
    print(f"  success_count: {final_job.get('success_count', 0)}")
    print(f"  error_count: {final_job.get('error_count', 0)}")
    if final_job.get('error_message'):
        print(f"  error_message: {final_job.get('error_message')}")

    # Step 7: Cleanup
    print("\n" + "=" * 60)
    print("STEP 7: Cleaning up test job...")
    print("=" * 60)

    # Try different key schemas
    for key_name in ['pk', 'object_id']:
        try:
            table.delete_item(Key={key_name: job_object_id})
            print(f"  Deleted job: {job_object_id}")
            break
        except Exception:
            continue

    print("\n" + "=" * 60)
    print("TEST COMPLETE")
    print("=" * 60)


if __name__ == '__main__':
    main()
