import glob
import requests
import boto3
import json
import uuid
import time
import re
import os
from decimal import Decimal
from datetime import datetime
from src.feaas.engines.engine import PlusEngine


class DecimalEncoder(json.JSONEncoder):
    """Helper class to encode Decimal objects to JSON"""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)


def convert_decimals_to_float(obj):
    """Recursively convert Decimal objects to floats in nested dictionaries and lists"""
    if isinstance(obj, list):
        return [convert_decimals_to_float(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: convert_decimals_to_float(value) for key, value in obj.items()}
    elif isinstance(obj, Decimal):
        return float(obj)
    return obj


def prepare_item_for_dynamo(item):
    """Prepare item for DynamoDB by converting floats to Decimals"""
    if isinstance(item, list):
        return [prepare_item_for_dynamo(i) for i in item]
    elif isinstance(item, dict):
        return {key: prepare_item_for_dynamo(value) for key, value in item.items()}
    elif isinstance(item, float):
        return Decimal(str(item))
    return item


class AwsLambdaPlusEngine(PlusEngine):
    """
    AWS Lambda implementation of PlusEngine using S3 for files and DynamoDB for collections/streams.
    
    Args:
        bucket_name (str): S3 bucket name for file storage
        collections_table_name (str): DynamoDB table name for collections
        streams_table_name (str): DynamoDB table name for streams
        hostname (str): The hostname identifier for the tenant/environment
        username (str): The username identifier for the authenticated user
    """


    def __init__(self, bucket_name, collections_table_name, streams_table_name, hostname, username):
        self.bucket_name = bucket_name
        self.collections_table_name = collections_table_name
        self.streams_table_name = streams_table_name
        super().__init__(hostname, username)
        
        self.s3_client = boto3.client('s3')
        self.dynamodb = boto3.resource('dynamodb')
        self.collections_table = self.dynamodb.Table(collections_table_name)
        self.streams_table = self.dynamodb.Table(streams_table_name)
        
        self.batch_client = boto3.client('batch', region_name='us-east-1')

    
    def _create_object_id(self, unique_id):
        """Create object_id in the format {hostname}/{username}/collection.{unique_id}"""
        return f"{self.hostname}/{self.username}/collection.{unique_id}"
    
    def _create_stream_id(self, stream_key):
        """Create stream_id in the format {hostname}/{username}/stream.{stream_key}"""
        return f"{self.hostname}/{self.username}/stream.{stream_key}"

    ### Files -----------------------------------------------------------------
    
    def file_exists(self, absolute_location_key):
        """Check if a file exists in S3"""
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=absolute_location_key)
            return True
        except self.s3_client.exceptions.NoSuchKey:
            return False
        except Exception:
            return False

    def file_get(self, absolute_location_key):
        """Retrieve file contents from S3"""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=absolute_location_key)
            return response['Body'].read()
        except self.s3_client.exceptions.NoSuchKey:
            raise FileNotFoundError(f"File not found: {absolute_location_key}")
        except Exception as e:
            raise Exception(f"Error retrieving file: {str(e)}")

    def file_delete(self, absolute_location_key):
        """Delete a file from S3"""
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=absolute_location_key)
            return True
        except Exception as e:
            raise Exception(f"Error deleting file: {str(e)}")

    def file_folder_list(self, location_prefix, limit=None, recursive=False, continuation_token=None):
        """List files in S3 with prefix"""
        try:
            kwargs = {
                'Bucket': self.bucket_name,
                'Prefix': location_prefix
            }
            
            if limit:
                kwargs['MaxKeys'] = limit
            if continuation_token:
                kwargs['ContinuationToken'] = continuation_token
            if not recursive:
                kwargs['Delimiter'] = '/'
            
            response = self.s3_client.list_objects_v2(**kwargs)
            
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    files.append({
                        'key': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'],
                        'etag': obj['ETag']
                    })
            
            return {
                'files': files,
                'continuation_token': response.get('NextContinuationToken'),
                'truncated': response.get('IsTruncated', False)
            }
        except Exception as e:
            raise Exception(f"Error listing files: {str(e)}")

    ### Collections -----------------------------------------------------------
    
    def collections_list(self):
        """Return list of collections from DynamoDB using owner secondary index"""
        try:
            owner_key = f"{self.hostname}/{self.username}/collections"
            
            response = self.collections_table.query(
                IndexName='ownerIndex',
                KeyConditionExpression='#owner = :owner',
                ExpressionAttributeNames={'#owner': 'owner'},
                ExpressionAttributeValues={':owner': owner_key}
            )
            
            collections = []
            for item in response['Items']:
                collections.append(convert_decimals_to_float(item))
            
            return collections
        except Exception as e:
            raise Exception(f"Error listing collections: {str(e)}")

    def collections_items_list(self, collection_key, limit=None, continuation_token=None):
        """List all items in a collection using owner secondary index"""
        try:
            kwargs = {
                'IndexName': 'ownerIndex',
                'KeyConditionExpression': '#owner = :owner',
                'ExpressionAttributeNames': {'#owner': 'owner'},
                'ExpressionAttributeValues': {':owner': collection_key}
            }
            
            if limit:
                kwargs['Limit'] = limit
            if continuation_token:
                kwargs['ExclusiveStartKey'] = json.loads(continuation_token)
            
            response = self.collections_table.query(**kwargs)
            
            items = []
            for item in response['Items']:
                items.append(convert_decimals_to_float(item))
            
            next_token = None
            if 'LastEvaluatedKey' in response:
                next_token = json.dumps(response['LastEvaluatedKey'], cls=DecimalEncoder)
            
            return {
                'items': items,
                'continuation_token': next_token,
                'truncated': 'LastEvaluatedKey' in response
            }
        except Exception as e:
            raise Exception(f"Error listing collection items: {str(e)}")

    def collections_items_run_script(self, collection_key, script_object_id, data):
        """Execute script against collection items - TODO: Implement script execution"""
        # Validate script access
        self._validate_script_object_id(script_object_id)
        
        # TODO: Implement script execution logic
        return self.run_script(script_object_id, data)

    def collections_item_add(self, unique_id, collection_key, item):
        """Add item to collection in DynamoDB"""
        try:
            # Create object_id and ensure required fields are present
            object_id = self._create_object_id(unique_id)
            
            # Copy item to avoid modifying original
            dynamo_item = item.copy()
            dynamo_item['object_id'] = object_id
            dynamo_item['owner'] = collection_key
            
            # Convert floats to Decimals for DynamoDB
            dynamo_item = prepare_item_for_dynamo(dynamo_item)
            
            # Save to DynamoDB (overwrite if exists)
            self.collections_table.put_item(Item=dynamo_item)
            
            return {
                'object_id': object_id,
                'collection_key': collection_key,
                'created_at': datetime.now()
            }
        except Exception as e:
            if 'ValidationException' in str(e):
                raise ValueError(f"Invalid data format for DynamoDB: {str(e)}")
            raise Exception(f"Error adding item to collection: {str(e)}")

    def collections_item_delete(self, unique_id):
        """Delete item from collection"""
        try:
            object_id = self._create_object_id(unique_id)
            
            response = self.collections_table.delete_item(
                Key={'object_id': object_id},
                ReturnValues='ALL_OLD'
            )
            
            return 'Attributes' in response
        except Exception as e:
            raise Exception(f"Error deleting collection item: {str(e)}")

    def collections_item_update(self, unique_id, update):
        """Update item in collection"""
        try:
            object_id = self._create_object_id(unique_id)
            
            # Prepare update for DynamoDB
            update = prepare_item_for_dynamo(update)
            
            # Build update expression
            update_expression = "SET "
            expression_attribute_values = {}
            expression_attribute_names = {}
            
            for key, value in update.items():
                if key not in ['object_id', 'owner']:  # Don't allow updating primary/secondary keys
                    attr_name = f"#{key}"
                    attr_value = f":{key}"
                    update_expression += f"{attr_name} = {attr_value}, "
                    expression_attribute_names[attr_name] = key
                    expression_attribute_values[attr_value] = value
            
            update_expression = update_expression.rstrip(', ')
            
            if not expression_attribute_values:
                raise ValueError("No valid fields to update")
            
            response = self.collections_table.update_item(
                Key={'object_id': object_id},
                UpdateExpression=update_expression,
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=expression_attribute_values,
                ReturnValues='ALL_NEW'
            )
            
            return {
                'object_id': object_id,
                'collection_key': response['Attributes'].get('owner'),
                'modified_at': datetime.now()
            }
        except Exception as e:
            if 'ValidationException' in str(e):
                raise ValueError(f"Invalid update data format: {str(e)}")
            raise Exception(f"Error updating collection item: {str(e)}")

    def collections_item_run_script(self, unique_id, script_object_id, data):
        """Execute script against specific collection item - TODO: Implement script execution"""
        # Validate script access
        self._validate_script_object_id(script_object_id)
        
        # TODO: Implement script execution logic
        return self.run_script(script_object_id, data)


    def collection_item_add_single(self, collection_key, item_data):
        """Add a single new record into a collection"""
        try:
            # Determine unique identifier
            if 'slug' in item_data and item_data['slug']:
                unique_id = str(item_data['slug'])
            elif 'unique_id' in item_data and item_data['unique_id']:
                unique_id = str(item_data['unique_id'])
            else:
                unique_id = str(uuid.uuid4())

            object_id = self._create_object_id(unique_id)

            # Copy input to avoid mutating caller’s dict
            dynamo_item = item_data.copy()
            dynamo_item['object_id'] = object_id
            dynamo_item['owner'] = collection_key
            dynamo_item['created_at'] = datetime.now().isoformat()

            # Prepare for DynamoDB
            dynamo_item = prepare_item_for_dynamo(dynamo_item)

            # Save to DynamoDB
            self.collections_table.put_item(Item=dynamo_item)

            return {
                'item_id': object_id,
                'collection_key': collection_key,
                'created_at': dynamo_item['created_at'],
                'data': item_data
            }
        except Exception as e:
            if 'ValidationException' in str(e):
                raise ValueError(f"Invalid data format for DynamoDB: {str(e)}")
            raise Exception(f"Error adding single record to collection: {str(e)}")


    ### Credentials -----------------------------------------------------------

    def credentials_list(self, limit=None, continuation_token=None):
        # TODO: implement more security
        collection_key = f'{self.hostname}/{self.username}/credential'
        return self.collections_items_list(collection_key, limit, continuation_token)


    ### Streams ---------------------------------------------------------------
    
    def streams_list(self):
        """Return list of streams for current user"""
        try:
            # Query streams table for all streams belonging to this user
            # This would require scanning or a GSI on stream_id prefix
            # For now, implementing a basic scan with filter
            response = self.streams_table.scan(
                FilterExpression='begins_with(stream_id, :prefix)',
                ExpressionAttributeValues={
                    ':prefix': f"{self.hostname}/{self.username}/stream."
                }
            )
            
            # Group by stream_id to get unique streams
            streams = {}
            for item in response['Items']:
                stream_id = item['stream_id']
                if stream_id not in streams:
                    streams[stream_id] = {
                        'stream_key': stream_id.split('stream.')[-1],
                        'stream_id': stream_id,
                        'item_count': 0,
                        'latest_timestamp': 0
                    }
                
                streams[stream_id]['item_count'] += 1
                if item['timestamp'] > streams[stream_id]['latest_timestamp']:
                    streams[stream_id]['latest_timestamp'] = item['timestamp']
            
            return list(streams.values())
        except Exception as e:
            raise Exception(f"Error listing streams: {str(e)}")

    def streams_items_list(self, stream_key, limit=None, continuation_token=None):
        """List recent items from stream"""
        try:
            stream_id = self._create_stream_id(stream_key)
            
            kwargs = {
                'KeyConditionExpression': 'stream_id = :stream_id',
                'ExpressionAttributeValues': {':stream_id': stream_id},
                'ScanIndexForward': False  # Most recent first
            }
            
            if limit:
                kwargs['Limit'] = limit
            if continuation_token:
                kwargs['ExclusiveStartKey'] = json.loads(continuation_token)
            
            response = self.streams_table.query(**kwargs)
            
            items = []
            for item in response['Items']:
                items.append(convert_decimals_to_float(item))
            
            next_token = None
            if 'LastEvaluatedKey' in response:
                next_token = json.dumps(response['LastEvaluatedKey'], cls=DecimalEncoder)
            
            return {
                'items': items,
                'continuation_token': next_token,
                'truncated': 'LastEvaluatedKey' in response
            }
        except Exception as e:
            raise Exception(f"Error listing stream items: {str(e)}")

    def streams_items_run_script(self, stream_key, script_object_id, data):
        """Execute script against stream items - TODO: Implement script execution"""
        # Validate script access
        self._validate_script_object_id(script_object_id)
        
        # TODO: Implement script execution logic
        return self.run_script(script_object_id, data)

    def streams_item_add(self, stream_key, timestamp, item):
        """Add item to stream"""
        try:
            stream_id = self._create_stream_id(stream_key)
            
            # Copy item and add required fields
            dynamo_item = item.copy()
            dynamo_item['stream_id'] = stream_id
            dynamo_item['timestamp'] = timestamp
            
            # Convert floats to Decimals for DynamoDB
            dynamo_item = prepare_item_for_dynamo(dynamo_item)
            
            # Save to DynamoDB
            self.streams_table.put_item(Item=dynamo_item)
            
            return {
                'stream_id': stream_id,
                'stream_key': stream_key,
                'timestamp': timestamp
            }
        except Exception as e:
            if 'ValidationException' in str(e):
                raise ValueError(f"Invalid data format for DynamoDB: {str(e)}")
            raise Exception(f"Error adding item to stream: {str(e)}")

    def streams_item_delete(self, stream_key, timestamp):
        """Delete item from stream"""
        try:
            stream_id = self._create_stream_id(stream_key)
            
            response = self.streams_table.delete_item(
                Key={
                    'stream_id': stream_id,
                    'timestamp': timestamp
                },
                ReturnValues='ALL_OLD'
            )
            
            return 'Attributes' in response
        except Exception as e:
            raise Exception(f"Error deleting stream item: {str(e)}")

    def streams_item_update(self, stream_key, timestamp, update):
        """Update item in stream"""
        try:
            stream_id = self._create_stream_id(stream_key)
            
            # Prepare update for DynamoDB
            update = prepare_item_for_dynamo(update)
            
            # Build update expression
            update_expression = "SET "
            expression_attribute_values = {}
            expression_attribute_names = {}
            
            for key, value in update.items():
                if key not in ['stream_id', 'timestamp']:  # Don't allow updating keys
                    attr_name = f"#{key}"
                    attr_value = f":{key}"
                    update_expression += f"{attr_name} = {attr_value}, "
                    expression_attribute_names[attr_name] = key
                    expression_attribute_values[attr_value] = value
            
            update_expression = update_expression.rstrip(', ')
            
            if not expression_attribute_values:
                raise ValueError("No valid fields to update")
            
            response = self.streams_table.update_item(
                Key={
                    'stream_id': stream_id,
                    'timestamp': timestamp
                },
                UpdateExpression=update_expression,
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=expression_attribute_values,
                ReturnValues='ALL_NEW'
            )
            
            return {
                'stream_id': stream_id,
                'stream_key': stream_key,
                'timestamp': timestamp,
                'modified_at': datetime.now()
            }
        except Exception as e:
            if 'ValidationException' in str(e):
                raise ValueError(f"Invalid update data format: {str(e)}")
            raise Exception(f"Error updating stream item: {str(e)}")

    def streams_item_run_script(self, stream_key, timestamp, script_object_id, data):
        """Execute script against specific stream item - TODO: Implement script execution"""
        # Validate script access
        self._validate_script_object_id(script_object_id)
        
        # TODO: Implement script execution logic
        return self.run_script(script_object_id, data)

    ### Actions ---------------------------------------------------------------
    
    def actions_search(self, q):
        """Search for available actions using synced action files"""
        import glob
        
        if not q:
            q = ""
        
        q_lower = q.lower()
        results = []
        
        # Find all synced action files
        action_files = glob.glob('.plusengine_actions_*')
        
        for action_file in action_files:
            try:
                with open(action_file, 'r') as f:
                    actions = json.load(f)
                
                for action in actions:
                    # Case insensitive search on label and short_desc
                    label = action.get('label', '').lower()
                    short_desc = action.get('short_desc', '').lower()
                    action_id = action.get('sys_action_id', '').lower()
                    
                    if q_lower in label or q_lower in short_desc or q_lower in action_id:
                        results.append({
                            'action_id': action.get('sys_action_id', ''),
                            'name': action.get('label', action.get('sys_name', 'Unknown')),
                            'description': action.get('short_desc', action.get('long_desc', '')),
                            'parameters': [p.get('var_name', '') for p in action.get('params', [])],
                            'tags': [action.get('sys_name', '')],
                            'source_file': action_file
                        })
                        
            except Exception as e:
                print(f"Warning: Could not read action file {action_file}: {e}")
                continue
        
        return results


    def actions_action_run(self, action_id, data):
        """Execute a specific action by finding it in synced actions and calling the server"""
        
        # Search through all synced action files to find the action
        action_files = glob.glob('.plusengine_actions_*')
        
        found_action = None
        server_id = None
        
        for action_file in action_files:
            try:
                with open(action_file, 'r') as f:
                    actions = json.load(f)
                
                for action in actions:
                    if action.get('sys_action_id') == action_id:
                        found_action = action
                        # Extract server_id from filename: .plusengine_actions_{server_id}
                        server_id = action_file.replace('.plusengine_actions_', '')
                        break
                
                if found_action:
                    break
                    
            except Exception as e:
                print(f"Warning: Could not read action file {action_file}: {e}")
                continue
        
        if not found_action:
            raise Exception(f"Action '{action_id}' not found in any synced servers")
        
        # Load server info to get URL
        servers = {}
        if os.path.exists('.plusengine_servers'):
            try:
                with open('.plusengine_servers', 'r') as f:
                    servers = json.load(f)
            except Exception as e:
                raise Exception(f"Could not load servers configuration: {e}")
        
        if server_id not in servers:
            raise Exception(f"Server '{server_id}' not found in configuration")
        
        server_url = servers[server_id]['url']
        
        # Construct execution URL
        execution_url = f"{server_url}/v2/execute/action/{action_id}"
        
        try:
            start_time = datetime.now()
            data['username'] = self.username
            response = requests.post(execution_url, json=data)
            response.raise_for_status()
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            result_data = response.json()
            
            return {
                'job_id': result_data.get('job_id', f"action_{action_id}_{int(start_time.timestamp())}"),
                'status': result_data.get('status', 'success'),
                'result': result_data.get('result', result_data),
                'executed_at': start_time,
                'duration': duration,
                'action_id': action_id,
                'server_id': server_id
            }
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to execute action on server: {e}")
        except json.JSONDecodeError as e:
            raise Exception(f"Invalid JSON response from server: {e}")
        except Exception as e:
            raise Exception(f"Unexpected error executing action: {e}")


    ### Jobs ------------------------------------------------------------------
    
    def jobs_list(self):
        """Return list of jobs - TODO: Implement job storage"""
        # TODO: Implement job storage and retrieval
        return []
    
    def jobs_get(self, job_id):
        """Get detailed job information - TODO: Implement job storage"""
        # TODO: Implement job detail retrieval
        return {
            'job_id': job_id,
            'name': 'Unknown Job',
            'status': 'unknown',
            'progress': 0.0,
            'created_at': datetime.now(),
            'started_at': None,
            'completed_at': None
        }
    
    def jobs_cancel(self, job_id):
        """Cancel a running job - TODO: Implement job management"""
        # TODO: Implement job cancellation
        return True

    ### Dashboards ------------------------------------------------------------
    
    def dashboards_list(self):
        """Return list of dashboards - TODO: Implement dashboard storage"""
        # TODO: Implement dashboard storage and retrieval
        return []
    
    def dashboards_create_session(self, dashboard_id):
        """Create dashboard session - TODO: Implement session management"""
        # TODO: Implement session management
        return {
            'session_id': f"session_{dashboard_id}_{int(datetime.now().timestamp())}",
            'dashboard_id': dashboard_id,
            'expires_at': datetime.now(),
            'websocket_url': 'wss://api.example.com/ws'
        }
    
    def dashboards_end_session(self, session_id):
        """End dashboard session - TODO: Implement session management"""
        # TODO: Implement session cleanup
        return True


    def _get_latest_job_definition(self, job_definition_name):
        """Get the latest active job definition ARN"""
        response = self.batch_client.describe_job_definitions(
            jobDefinitionName=job_definition_name,
            status='ACTIVE'
        )
        
        job_definitions = response.get('jobDefinitions', [])
        if not job_definitions:
            raise ValueError(f"No active job definitions found for {job_definition_name}")
        
        latest_job_definition = max(job_definitions, key=lambda x: x['revision'])
        return latest_job_definition['jobDefinitionArn']


    def run_script(self, script_object_id, data):
        """Execute a script on AWS Batch"""
        try:
            # Generate job details
            job_id = str(uuid.uuid4())
            job_name = script_object_id.split(".")[-1] + f" ___ {self.username}"
            job_name = re.sub(r'[^a-zA-Z0-9_-]', '_', job_name)
            
            # Job configuration
            job_queue_arn = "arn:aws:batch:us-east-1:085318171245:job-queue/batch-job-queue"
            job_definition_name = "batch-job-definition"
            
            # Get latest job definition
            job_definition_arn = self._get_latest_job_definition(job_definition_name)
            
            # Prepare job owner and object_id for tracking
            job_owner = f'{self.hostname}/{self.username}/job'
            job_object_id = f'{job_owner}.{job_id}'
            
            # Submit job to AWS Batch
            response = self.batch_client.submit_job(
                jobName=job_name,
                jobQueue=job_queue_arn,
                jobDefinition=job_definition_arn,
                containerOverrides={
                    "environment": [
                        {"name": "JOB_ID", "value": job_id},
                        {"name": "REGION", "value": "us-east-1"},
                        {"name": "DYNAMO_TABLE", "value": self.collections_table_name},
                        {"name": "DYNAMO_STREAMS_TABLE", "value": self.streams_table_name},
                        {"name": "PRIMARY_BUCKET", "value": self.bucket_name},
                        {"name": "RUN_MODE", "value": "RUN_SCRIPT"},
                        {"name": "USERNAME", "value": self.username},
                        {"name": "SCRIPT_OBJECT_ID", "value": script_object_id},
                        {"name": "SCRIPT_INPUT_JSON", "value": json.dumps(data)},
                    ]
                }
            )
            
            aws_job_id = response['jobId']
            
            # Store job document in DynamoDB for tracking
            job_doc = {
                "object_id": job_object_id,
                "owner": job_owner,
                "aws_job_id": aws_job_id,
                "script_object_id": script_object_id,
                "started_at": int(time.time())
            }
            
            # Convert floats to Decimals for DynamoDB
            job_doc = prepare_item_for_dynamo(job_doc)
            self.collections_table.put_item(Item=job_doc)
            
            return {
                'job_id': job_id,
                'aws_job_id': aws_job_id,
                'script_object_id': script_object_id,
                'status': 'submitted',
                'created_at': datetime.now()
            }
            
        except Exception as e:
            raise Exception(f"Error submitting script to AWS Batch: {str(e)}")

