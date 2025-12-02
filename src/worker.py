"""
Entry point for plus-worker Fargate tasks.

Reads configuration from environment variables and executes the requested action.
"""
import json
import os
import requests
import sys
import time

import feaas.objects as objs
from feaas.dao.dao import DataAccessObject
from google.protobuf.json_format import Parse, MessageToDict

# Required for all modes
REQUIRED_ENV = [
    'RUN_MODE',
    'JOB_ID',
    'USERNAME',
    'REGION',
    'ACCESS_KEY',
    'SECRET_KEY',
    'DYNAMO_TABLE',
    'DYNAMO_STREAMS_TABLE',
    'PRIMARY_BUCKET',
]

# Additional required vars per mode
MODE_REQUIRED_ENV = {
    'RUN_ACTION': ['ACTION_ID'],
    'RUN_SCRIPT': [],
    'RUN_COLLECTION': ['COLLECTION_OWNER'],
}


def check_env():
    """Validate all required environment variables are set. Exit 1 if any missing."""
    missing = []

    for var in REQUIRED_ENV:
        if not os.environ.get(var):
            missing.append(var)

    run_mode = os.environ.get('RUN_MODE')
    if run_mode in MODE_REQUIRED_ENV:
        for var in MODE_REQUIRED_ENV[run_mode]:
            if not os.environ.get(var):
                missing.append(var)

    if missing:
        print("ERROR: Missing required environment variables:", file=sys.stderr)
        for var in missing:
            print(f"  - {var}", file=sys.stderr)
        sys.exit(1)

    print("Environment check passed. Found all required variables:")
    for var in REQUIRED_ENV:
        val = os.environ.get(var)
        # Mask secrets
        if 'KEY' in var or 'SECRET' in var:
            val = val[:4] + '...' if val else None
        print(f"  {var}={val}")

    if run_mode in MODE_REQUIRED_ENV:
        for var in MODE_REQUIRED_ENV[run_mode]:
            print(f"  {var}={os.environ.get(var)}")


def get_fargate_task_arn():
    """Get the Fargate task ARN from ECS metadata, if available."""
    metadata_uri = os.environ.get('ECS_CONTAINER_METADATA_URI_V4')
    if not metadata_uri:
        return None
    try:
        resp = requests.get(f"{metadata_uri}/task", timeout=2)
        if resp.status_code == 200:
            return resp.json().get('TaskARN')
    except Exception as e:
        print(f"Warning: Could not fetch Fargate task ARN: {e}")
    return None


def get_dao():
    """Initialize DataAccessObject from environment variables."""
    props = {
        'ACCESS_KEY': os.environ.get('ACCESS_KEY'),
        'SECRET_KEY': os.environ.get('SECRET_KEY'),
        'REGION': os.environ.get('REGION'),
        'DYNAMO_TABLE': os.environ.get('DYNAMO_TABLE'),
        'DYNAMO_STREAMS_TABLE': os.environ.get('DYNAMO_STREAMS_TABLE'),
        'PRIMARY_BUCKET': os.environ.get('PRIMARY_BUCKET'),
    }
    return DataAccessObject(props, running_as_worker=True)


def load_job(dao, job_id: str) -> objs.PlusScriptJob:
    """Load PlusScriptJob from DynamoDB and parse as protobuf."""
    docstore = dao.get_docstore()
    doc = docstore.get_document(job_id)

    if doc is None:
        raise ValueError(f"Job not found: {job_id}")

    job = Parse(json.dumps(doc), objs.PlusScriptJob(), ignore_unknown_fields=True)
    return job


def save_job(dao, job: objs.PlusScriptJob):
    """Save PlusScriptJob back to DynamoDB."""
    docstore = dao.get_docstore()
    doc = MessageToDict(job, preserving_proto_field_name=True)
    docstore.save_document(job.object_id, doc)


def run_script_job():
    """Run a PlusScriptJob."""
    job_id = os.environ.get('JOB_ID')
    print(f"\nLoading job: {job_id}")

    # Initialize DAO
    dao = get_dao()

    # Load job from DynamoDB
    try:
        job = load_job(dao, job_id)
    except Exception as e:
        print(f"ERROR: Failed to load job: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Job loaded: {job.label}")
    print(f"  owner: {job.owner}")
    print(f"  username: {job.username}")
    print(f"  current status: {objs.PlusScriptStatus.Name(job.status)}")

    # Update job to RUNNING
    now = int(time.time())
    job.started_at = now
    job.status = objs.PlusScriptStatus.RUNNING

    # Try to get Fargate task ARN
    task_arn = get_fargate_task_arn()
    if task_arn:
        job.fargate_task_arn = task_arn
        print(f"  fargate_task_arn: {task_arn}")

    # Save updated job
    save_job(dao, job)
    print(f"Job status updated to RUNNING")

    # TEST MODE: Sleep for 20 seconds
    print("\n[TEST MODE] Sleeping for 20 seconds...")
    for i in range(20):
        print(f"  {i+1}/20 seconds elapsed")
        time.sleep(1)

    # Update job to SUCCEEDED
    job.completed_at = int(time.time())
    job.status = objs.PlusScriptStatus.SUCCEEDED
    save_job(dao, job)

    print(f"\nJob status updated to SUCCEEDED")


def run_collection_job():
    """Run a PlusScriptJob for collection processing."""
    job_id = os.environ.get('JOB_ID')
    collection_owner = os.environ.get('COLLECTION_OWNER')
    print(f"\nLoading job: {job_id}")
    print(f"Collection owner: {collection_owner}")

    # Initialize DAO
    dao = get_dao()

    # Load job from DynamoDB
    try:
        job = load_job(dao, job_id)
    except Exception as e:
        print(f"ERROR: Failed to load job: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Job loaded: {job.label}")
    print(f"  owner: {job.owner}")
    print(f"  username: {job.username}")
    print(f"  current status: {objs.PlusScriptStatus.Name(job.status)}")

    # Update job to RUNNING
    now = int(time.time())
    job.started_at = now
    job.status = objs.PlusScriptStatus.RUNNING

    # Try to get Fargate task ARN
    task_arn = get_fargate_task_arn()
    if task_arn:
        job.fargate_task_arn = task_arn
        print(f"  fargate_task_arn: {task_arn}")

    # Save updated job
    save_job(dao, job)
    print(f"Job status updated to RUNNING")

    # TEST MODE: Sleep for 20 seconds
    print("\n[TEST MODE] Sleeping for 20 seconds...")
    for i in range(20):
        print(f"  {i+1}/20 seconds elapsed")
        time.sleep(1)

    # Update job to SUCCEEDED
    job.completed_at = int(time.time())
    job.status = objs.PlusScriptStatus.SUCCEEDED
    save_job(dao, job)

    print(f"\nJob status updated to SUCCEEDED")


def main():
    print("=" * 50)
    print("plus-worker starting")
    print("=" * 50)

    check_env()

    run_mode = os.environ.get('RUN_MODE')
    print(f"\nRUN_MODE: {run_mode}")

    if run_mode == 'RUN_SCRIPT':
        run_script_job()
    elif run_mode == 'RUN_COLLECTION':
        run_collection_job()
    elif run_mode == 'RUN_ACTION':
        # TODO: Implement single action execution
        print("RUN_ACTION not yet implemented")
        sys.exit(1)
    else:
        print(f"ERROR: Unknown RUN_MODE: {run_mode}", file=sys.stderr)
        sys.exit(1)

    print("=" * 50)
    print("plus-worker completed successfully")
    print("=" * 50)
    sys.exit(0)


if __name__ == '__main__':
    main()
