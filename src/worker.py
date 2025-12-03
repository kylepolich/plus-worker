"""
Entry point for plus-worker Fargate tasks.

Reads configuration from environment variables and executes the requested action.
"""
import json
import logging
import os
import requests
import sys
import time
import traceback

# Suppress verbose boto3/botocore DEBUG logs
logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

import feaas.objects as objs
from feaas.dao.dao import DataAccessObject
from feaas.psee.psee import PlusScriptExecutionEngine
from feaas.util.common import build_action_class
from google.protobuf.json_format import Parse, MessageToDict

# Search paths for action resolution (first match wins)
ACTION_SEARCH_PATHS = [
    'src.actions.vendor',    # Local worker actions (ffmpeg, etc.)
    'chalicelib.actions',    # plus-engine actions
    'feaas.actions',         # plus-core built-ins
]

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
    'RUN_JOB': [],  # job_type is in the job itself (run_on_collection, run_on_stream)
}


class CancelledException(Exception):
    """Raised when job is cancelled via request_cancel flag."""
    pass


class WorkerActionExecutor:
    """
    Action executor for the worker that uses local search paths.

    Implements the interface expected by PlusScriptExecutionEngine:
    - begin_action_execution(action_id, username, data) -> Receipt

    Also tracks progress and triggers saves.
    """

    def __init__(self, dao, job_runner):
        self.dao = dao
        self.job_runner = job_runner
        self.success_count = 0
        self.error_count = 0

    def begin_action_execution(self, action_id, username, data) -> objs.Receipt:
        """Execute an action and return the receipt."""
        print(f"  Executing action: {action_id}")

        try:
            # Resolve action class using worker search paths
            ActionClass = build_action_class(action_id, search_paths=ACTION_SEARCH_PATHS)
            action = ActionClass(self.dao)

            # Execute the action
            receipt = action.execute_action(**data)

            # Track counts
            if receipt.success:
                self.success_count += 1
                print(f"    -> Success")
            else:
                self.error_count += 1
                print(f"    -> Failed: {receipt.error_message}")

            # Notify job runner of action completion (for progress tracking)
            self.job_runner.on_action_complete()

            return receipt

        except ModuleNotFoundError:
            self.error_count += 1
            msg = f"Action not found: {action_id}. Searched: {ACTION_SEARCH_PATHS}"
            print(f"    -> Error: {msg}")
            return objs.Receipt(success=False, error_message=msg)
        except Exception as e:
            self.error_count += 1
            msg = f"Action execution failed: {str(e)}\n{traceback.format_exc()}"
            print(f"    -> Error: {msg}")
            return objs.Receipt(success=False, error_message=msg)


class JobRunner:
    """
    Manages job execution with progress tracking and cancellation support.

    Progress is saved:
    - Every 60 seconds
    - When percent changes

    Cancellation is checked on each progress save.
    """

    SAVE_INTERVAL_SECONDS = 60

    def __init__(self, dao, job: objs.PlusScriptJob):
        self.dao = dao
        self.docstore = dao.get_docstore()
        self.job = job
        self.last_save_time = time.time()
        self.last_saved_percent = 0
        self.total_actions = self._count_action_nodes()
        self.completed_actions = 0

    def _count_action_nodes(self) -> int:
        """Count total ACTION nodes in the script."""
        count = 0
        for node in self.job.script.nodes:
            if node.ntype == objs.PlusScriptNodeType.ACTION:
                count += 1
        return max(count, 1)  # Avoid division by zero

    def on_action_complete(self):
        """Called after each action completes. May trigger progress save."""
        self.completed_actions += 1
        self._maybe_save_progress()

    def _calculate_percent(self) -> int:
        """Calculate current progress percentage."""
        return int((self.completed_actions / self.total_actions) * 100)

    def _maybe_save_progress(self):
        """Save progress if 60s elapsed or percent changed."""
        now = time.time()
        current_percent = self._calculate_percent()

        time_elapsed = (now - self.last_save_time) >= self.SAVE_INTERVAL_SECONDS
        percent_changed = current_percent != self.last_saved_percent

        if time_elapsed or percent_changed:
            self._save_progress(current_percent)
            self.last_save_time = now
            self.last_saved_percent = current_percent

    def _save_progress(self, percent: int):
        """Save job progress to DynamoDB and check for cancellation."""
        print(f"  Saving progress: {percent}% ({self.completed_actions}/{self.total_actions} actions)")

        # Update job fields
        self.job.updated_at = int(time.time())

        self.job.percent = percent

        # Save to DynamoDB
        doc = MessageToDict(self.job, preserving_proto_field_name=True)
        self.docstore.save_document(self.job.object_id, doc)

        # Check for cancellation by reloading job
        self._check_cancellation()

    def _check_cancellation(self):
        """Check if job has been cancelled. Raises CancelledException if so."""
        doc = self.docstore.get_document(self.job.object_id)
        if doc and doc.get('request_cancel', False):
            print("  Job cancellation requested!")
            raise CancelledException("Job was cancelled by user")

    def run(self) -> objs.PlusScriptJob:
        """Execute the job using PSEE and return the final job state."""
        print(f"\nStarting PSEE execution...")
        print(f"  Total action nodes: {self.total_actions}")

        # Create executor that reports back to us
        executor = WorkerActionExecutor(self.dao, self)

        # Create PSEE with our executor
        psee = PlusScriptExecutionEngine(self.dao, executor)

        try:
            # Run the job - PSEE will call executor.begin_action_execution for each action
            self.job = psee.run_job(self.job)

            # Update final counts
            self.job.success_count = executor.success_count
            self.job.error_count = executor.error_count
            self.job.percent = 100

            print(f"\nPSEE execution complete")
            print(f"  Status: {objs.PlusScriptStatus.Name(self.job.status)}")
            print(f"  Actions: {executor.success_count} succeeded, {executor.error_count} failed")

        except CancelledException:
            self.job.status = objs.PlusScriptStatus.FAILED
            self.job.error_message = "Job was cancelled by user"
            self.job.success_count = executor.success_count
            self.job.error_count = executor.error_count

        return self.job


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


def load_job(dao, job_id: str) -> tuple:
    """Load PlusScriptJob from DynamoDB. Returns (protobuf, raw_doc) tuple.

    The raw_doc contains extra fields not in the protobuf:
    - job_type: 'run_on_collection' or 'run_on_stream'
    - collection_key or stream_key
    - hostname
    - input_data
    """
    docstore = dao.get_docstore()
    doc = docstore.get_document(job_id)

    if doc is None:
        raise ValueError(f"Job not found: {job_id}")

    job = Parse(json.dumps(doc), objs.PlusScriptJob(), ignore_unknown_fields=True)
    return job, doc


def save_job(dao, job: objs.PlusScriptJob):
    """Save PlusScriptJob back to DynamoDB."""
    docstore = dao.get_docstore()
    doc = MessageToDict(job, preserving_proto_field_name=True)
    docstore.save_document(job.object_id, doc)


def run_job():
    """Run a PlusScriptJob using PSEE. Handles both collection and stream jobs."""
    job_id = os.environ.get('JOB_ID')
    print(f"\nLoading job: {job_id}")

    # Initialize DAO
    dao = get_dao()

    # Load job from DynamoDB (get both protobuf and raw doc for extra fields)
    try:
        job, job_doc = load_job(dao, job_id)
    except Exception as e:
        print(f"ERROR: Failed to load job: {e}", file=sys.stderr)
        sys.exit(1)

    # Extract extra fields from raw doc
    job_type = job_doc.get('job_type', 'singleton')
    collection_key = job_doc.get('collection_key')
    stream_key = job_doc.get('stream_key')
    hostname = job_doc.get('hostname')
    input_data = job_doc.get('input_data', {})

    print(f"Job loaded: {job.label}")
    print(f"  owner: {job.owner}")
    print(f"  username: {job.username}")
    print(f"  job_type: {job_type}")
    print(f"  current status: {objs.PlusScriptStatus.Name(job.status)}")

    if collection_key:
        print(f"  collection_key: {collection_key}")
    if stream_key:
        print(f"  stream_key: {stream_key}")

    # Update job to RUNNING
    now = int(time.time())
    job.started_at = now
    job.status = objs.PlusScriptStatus.RUNNING

    # Try to get Fargate task ARN
    task_arn = get_fargate_task_arn()
    if task_arn:
        job.fargate_task_arn = task_arn
        print(f"  fargate_task_arn: {task_arn}")

    # Save initial RUNNING state
    save_job(dao, job)
    print(f"Job status updated to RUNNING")

    # Dispatch based on job_type
    try:
        if job_type == 'run_on_collection':
            job = run_on_collection(dao, job, hostname, collection_key, input_data)
        elif job_type == 'run_on_stream':
            job = run_on_stream(dao, job, hostname, stream_key, input_data)
        else:
            # Singleton job - just run once
            runner = JobRunner(dao, job)
            job = runner.run()
    except Exception as e:
        print(f"ERROR: Job execution failed: {e}", file=sys.stderr)
        traceback.print_exc()
        job.status = objs.PlusScriptStatus.FAILED
        job.error_message = str(e)

    # Set completion timestamp
    job.completed_at = int(time.time())
    job.updated_at = int(time.time())

    # Final save
    save_job(dao, job)

    status_name = objs.PlusScriptStatus.Name(job.status)
    print(f"\nJob completed with status: {status_name}")

    if job.status == objs.PlusScriptStatus.FAILED:
        print(f"  Error: {job.error_message}", file=sys.stderr)
        sys.exit(1)


def search_by_owner(dao, owner: str) -> list:
    """Search for all items with a given owner using DynamoDB.

    Tries GSI query on 'owner' first (efficient), falls back to scan.
    """
    import boto3
    from boto3.dynamodb.conditions import Key, Attr

    table_name = os.environ.get('DYNAMO_TABLE')
    region = os.environ.get('REGION')
    access_key = os.environ.get('ACCESS_KEY')
    secret_key = os.environ.get('SECRET_KEY')

    dynamodb = boto3.resource(
        'dynamodb',
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    table = dynamodb.Table(table_name)

    items = []

    # Try querying GSI on 'owner' field first (most efficient)
    try:
        query_kwargs = {
            'IndexName': 'owner-index',
            'KeyConditionExpression': Key('owner').eq(owner)
        }
        while True:
            response = table.query(**query_kwargs)
            items.extend(response.get('Items', []))
            if 'LastEvaluatedKey' not in response:
                break
            query_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']

        if items:
            print(f"  Found {len(items)} items via owner-index GSI")
            return items
    except Exception as e:
        print(f"  GSI query failed ({e}), falling back to scan")

    # Fallback: scan with filter on owner field
    scan_kwargs = {
        'FilterExpression': Attr('owner').eq(owner)
    }
    while True:
        response = table.scan(**scan_kwargs)
        items.extend(response.get('Items', []))
        if 'LastEvaluatedKey' not in response:
            break
        scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']

    print(f"  Found {len(items)} items via scan")

    # Filter out jobs - only return actual collection/stream items
    # Collection items have pk like: owner.{unique_id}
    # Jobs have pk like: hostname/username/job.{uuid}
    filtered = [item for item in items if '/job.' not in item.get('pk', '')]
    if len(filtered) != len(items):
        print(f"  Filtered to {len(filtered)} items (excluded {len(items) - len(filtered)} jobs)")

    return filtered


def run_on_collection(dao, job: objs.PlusScriptJob, hostname: str,
                      collection_key: str, input_data: dict) -> objs.PlusScriptJob:
    """Run a script on each item in a collection."""
    owner = job.owner  # e.g., 'plus_dataskeptic_com/kyle@dataskeptic.com/collection.arxiv_ratings'

    print(f"\n{'='*60}")
    print(f"RUNNING ON COLLECTION: {collection_key}")
    print(f"{'='*60}")

    # Search for all items in the collection (workaround for docstore.search bug)
    print(f"  Searching for items with owner: {owner}")
    items = search_by_owner(dao, owner)

    if not items:
        print(f"  WARNING: No items found in collection")
        job.status = objs.PlusScriptStatus.SUCCEEDED
        job.error_message = "No items found in collection"
        return job

    print(f"  Found {len(items)} items to process")

    # Create executor for tracking counts
    executor = WorkerActionExecutor(dao, None)
    psee = PlusScriptExecutionEngine(dao, executor)

    success_count = 0
    error_count = 0

    for i, item in enumerate(items):
        item_object_id = item.get('pk', item.get('object_id', 'unknown'))

        print(f"\n  [{i+1}/{len(items)}] Processing: {item_object_id}")
        print(f"  {'='*50}")

        # Merge input_data with item data (ensure object_id is set)
        script_input = dict(input_data) if input_data else {}
        script_input.update(item)
        script_input['object_id'] = item_object_id

        try:
            # Start a fresh job for this item using the same script
            item_job = psee.start_script(hostname, job.username, job.script, script_input)

            # Run the job until completion
            item_job = psee.run_job(item_job)
            while item_job.status == objs.PlusScriptStatus.RUNNING:
                item_job = psee.run_job(item_job)

            if item_job.status == objs.PlusScriptStatus.FAILED:
                print(f"    -> FAILED: {item_job.error_message}")
                error_count += 1
            else:
                print(f"    -> SUCCESS")
                success_count += 1

        except Exception as e:
            print(f"    -> ERROR: {str(e)}")
            error_count += 1

        # Update progress
        job.percent = int(((i + 1) / len(items)) * 100)
        job.success_count = success_count
        job.error_count = error_count
        job.updated_at = int(time.time())
        save_job(dao, job)

    # Final status
    job.percent = 100
    job.success_count = success_count
    job.error_count = error_count

    if error_count == 0:
        job.status = objs.PlusScriptStatus.SUCCEEDED
    elif success_count == 0:
        job.status = objs.PlusScriptStatus.FAILED
        job.error_message = f"All {error_count} items failed"
    else:
        job.status = objs.PlusScriptStatus.SUCCEEDED  # Partial success
        job.error_message = f"{error_count} of {len(items)} items failed"

    print(f"\n{'='*60}")
    print(f"COLLECTION COMPLETE: {success_count} succeeded, {error_count} failed")
    print(f"{'='*60}")

    return job


def run_on_stream(dao, job: objs.PlusScriptJob, hostname: str,
                  stream_key: str, input_data: dict) -> objs.PlusScriptJob:
    """Run a script on each item in a stream."""
    streams = dao.get_streams()

    print(f"\n{'='*60}")
    print(f"RUNNING ON STREAM: {stream_key}")
    print(f"{'='*60}")

    # Stream items are in DYNAMO_STREAMS_TABLE, keyed by stream_id
    stream_id = f'{hostname}/{job.username}/stream.{stream_key}'
    print(f"  Reading stream: {stream_id}")
    items = streams.read_stream(stream_id, after_timestamp=0, limit=10000)
    print(f"  Found {len(items)} items")

    if not items:
        print(f"  WARNING: No items found in stream")
        job.status = objs.PlusScriptStatus.SUCCEEDED
        job.error_message = "No items found in stream"
        return job

    print(f"  Found {len(items)} items to process")

    # Create executor for tracking counts
    executor = WorkerActionExecutor(dao, None)
    psee = PlusScriptExecutionEngine(dao, executor)

    success_count = 0
    error_count = 0

    for i, item in enumerate(items):
        # Stream items use timestamp as identifier
        item_ts = item.get('timestamp', 'unknown')

        print(f"\n  [{i+1}/{len(items)}] Processing: {stream_id}@{item_ts}")
        print(f"  {'='*50}")

        # Merge input_data with item data
        script_input = dict(input_data) if input_data else {}
        script_input.update(item)
        script_input['stream_id'] = stream_id
        script_input['timestamp'] = item_ts

        try:
            # Start a fresh job for this item using the same script
            item_job = psee.start_script(hostname, job.username, job.script, script_input)

            # Run until completion
            item_job = psee.run_job(item_job)
            while item_job.status == objs.PlusScriptStatus.RUNNING:
                item_job = psee.run_job(item_job)

            if item_job.status == objs.PlusScriptStatus.FAILED:
                print(f"    -> FAILED: {item_job.error_message}")
                error_count += 1
            else:
                print(f"    -> SUCCESS")
                success_count += 1

        except Exception as e:
            print(f"    -> ERROR: {str(e)}")
            error_count += 1

        # Update progress
        job.percent = int(((i + 1) / len(items)) * 100)
        job.success_count = success_count
        job.error_count = error_count
        job.updated_at = int(time.time())
        save_job(dao, job)

    # Final status
    job.percent = 100
    job.success_count = success_count
    job.error_count = error_count

    if error_count == 0:
        job.status = objs.PlusScriptStatus.SUCCEEDED
    elif success_count == 0:
        job.status = objs.PlusScriptStatus.FAILED
        job.error_message = f"All {error_count} items failed"
    else:
        job.status = objs.PlusScriptStatus.SUCCEEDED  # Partial success
        job.error_message = f"{error_count} of {len(items)} items failed"

    print(f"\n{'='*60}")
    print(f"STREAM COMPLETE: {success_count} succeeded, {error_count} failed")
    print(f"{'='*60}")

    return job


def run_action():
    """Execute a single action."""
    action_id = os.environ.get('ACTION_ID')
    username = os.environ.get('USERNAME')
    action_input_json = os.environ.get('ACTION_INPUT_JSON', '{}')

    print(f"\nExecuting action: {action_id}")
    print(f"  username: {username}")

    # Parse input data
    try:
        inputs = json.loads(action_input_json)
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid ACTION_INPUT_JSON: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"  inputs: {list(inputs.keys())}")

    # Initialize DAO
    dao = get_dao()

    # Resolve and instantiate action class
    try:
        ActionClass = build_action_class(action_id, search_paths=ACTION_SEARCH_PATHS)
        action = ActionClass(dao)
        print(f"  resolved to: {ActionClass.__module__}.{ActionClass.__name__}")
    except ModuleNotFoundError as e:
        print(f"ERROR: Action not found: {action_id}", file=sys.stderr)
        print(f"  Searched: {ACTION_SEARCH_PATHS}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: Failed to instantiate action: {e}", file=sys.stderr)
        sys.exit(1)

    # Execute action
    print(f"\nExecuting action...")
    try:
        receipt = action.execute_action(**inputs)
    except Exception as e:
        print(f"ERROR: Action execution failed: {e}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)

    # Report results
    if receipt.success:
        print(f"\nAction completed successfully")
        if receipt.outputs:
            print(f"  outputs: {list(receipt.outputs.keys())}")
        if receipt.primary_output:
            print(f"  primary_output: {receipt.primary_output}")
    else:
        print(f"\nAction failed: {receipt.error_message}", file=sys.stderr)
        sys.exit(1)


def main():
    print("=" * 50)
    print("plus-worker starting")
    print("=" * 50)

    check_env()

    run_mode = os.environ.get('RUN_MODE')
    print(f"\nRUN_MODE: {run_mode}")

    if run_mode == 'RUN_JOB':
        run_job()
    elif run_mode == 'RUN_ACTION':
        run_action()
    else:
        print(f"ERROR: Unknown RUN_MODE: {run_mode}", file=sys.stderr)
        print(f"  Valid modes: RUN_JOB, RUN_ACTION", file=sys.stderr)
        sys.exit(1)

    print("=" * 50)
    print("plus-worker completed successfully")
    print("=" * 50)
    sys.exit(0)


if __name__ == '__main__':
    main()
