from datetime import datetime, timezone
import time, io, logging
from typing import Dict

import pyarrow as pa
import pyarrow.parquet as pq
from boto3.dynamodb.conditions import Attr, Key

import feaas.objects as objs
from feaas.abstract import AbstractAction
from .owners import (
    get_global_system_owners,
    get_system_hostname_owners,
    get_user_app_owners,
    get_user_collection_owners,
    get_user_stream_owners,
    get_user_dashboard_owners,
    get_user_api_gateway_owners,
    get_user_file_owners,
    get_user_collection_extra_patterns,
)

# Configurable defaults
S3_PREFIX = "env/mirror"
DYNAMO_PAGE_SIZE = 100
PARQUET_BATCH_SIZE = 500


def _sanitize_owner(owner: str) -> str:
    """Sanitize owner names for use in S3 paths."""
    return owner.replace("/", "_").replace("@", "_at_")


class PerformMirror(AbstractAction):
    """Action that mirrors data from Dynamo/docstore into S3 as Parquet."""

    def __init__(self, dao):
        hostname = objs.Parameter(
            var_name='hostname',
            label='Hostname',
            ptype=objs.ParameterType.HOSTNAME)
        params = [hostname]

        collections_updated = objs.Parameter(
            optional=True,
            var_name='collections_updated',
            label='Collections Updated',
            ptype=objs.ParameterType.INTEGER)
        record_count = objs.Parameter(
            optional=True,
            var_name='record_count',
            label='Total Records',
            ptype=objs.ParameterType.INTEGER)
        outputs = [collections_updated, record_count]
        super().__init__(params, outputs)
        self.primary_output = 'record_count'
        self.docstore = dao.get_docstore()
        self.streams = dao.get_streams()
        self.blobstore = dao.get_blobstore()

    def execute_action(self, hostname: str) -> objs.Receipt:
        """Main entry point for the mirror action."""
        t0 = int(time.time())
        global_resp = self._global_mirror()
        t1 = int(time.time())
        sys_resp = self._system_hostname_mirror(hostname)
        t2 = int(time.time())
        ugc_resp = self._user_content_mirror(hostname)
        t3 = int(time.time())

        outputs = {
            'global_mirror_duration': objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=(t1-t0)),
            'global_mirror_record_count': objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=global_resp['record_count']),
            'global_mirror_collections_updated': objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=global_resp['collections_updated']),
            'sys_mirror_duration': objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=(t2-t1)),
            'sys_mirror_record_count': objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=sys_resp['record_count']),
            'sys_mirror_collections_updated': objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=sys_resp['collections_updated']),
            'ugc_mirror_duration': objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=(t3-t2)),
            'ugc_mirror_record_count': objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=ugc_resp['record_count']),
            'ugc_mirror_collections_updated': objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=ugc_resp['collections_updated']),
            'record_count': objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=global_resp['record_count'] + sys_resp['record_count'] + ugc_resp['record_count']),
            'collections_updated': objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=global_resp['collections_updated'] + sys_resp['collections_updated'] + ugc_resp['collections_updated']),
        }
        return objs.Receipt(success=True, outputs=outputs)

    def _mirror_owner(self, owner: str, yyyymmdd: str) -> int:
        """Mirror all records for a given owner to S3."""
        record_count = 0
        all_ids = []
        sanitized = _sanitize_owner(owner)
        
        # Paginated query to collect all object_ids
        last_evaluated_key = None
        batch_idx = 0
        
        try:
            while True:
                query_args = {
                    'IndexName': 'ownerIndex',
                    'KeyConditionExpression': Key('owner').eq(owner),
                    'FilterExpression': Attr('object_id').exists(),
                    'Limit': DYNAMO_PAGE_SIZE
                }
                if last_evaluated_key:
                    query_args['ExclusiveStartKey'] = last_evaluated_key
                    
                res = self.docstore.table.query(**query_args)
                
                batch = res.get('Items', [])
                batch_ids = [item['object_id'] for item in batch if 'object_id' in item]
                all_ids.extend(batch_ids)
                record_count += len(batch_ids)
                
                last_evaluated_key = res.get('LastEvaluatedKey')
                if not last_evaluated_key:
                    break
                batch_idx += 1
                
        except Exception as e:
            logging.error(f"Error querying owner {owner}: {e}")
            return 0
        
        if not all_ids:
            logging.info(f"No records found for owner: {owner}")
            return 0
            
        try:
            # Get full documents from docstore
            records = self.docstore.get_batch_documents(all_ids)
            if not records:
                logging.warning(f"No documents returned for owner: {owner}")
                return 0
                
            # Convert to Arrow Table
            table = pa.Table.from_pylist(records)
            
            # Write to in-memory Parquet file
            buffer = io.BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)
            
            # Save to blobstore
            key = f"{S3_PREFIX}/{yyyymmdd}/{sanitized}/mirror_{batch_idx}.parquet"
            self.blobstore.save_blob(key, buffer.read())
            logging.info(f"Mirrored {len(records)} records to {key}")
            
        except Exception as e:
            logging.error(f"Error mirroring owner {owner}: {e}")
            return 0
            
        return len(records)

    def _global_mirror(self) -> Dict[str, int]:
        """Mirror global system data."""
        yyyymmdd = datetime.now(timezone.utc).strftime("%Y%m%d")
        total_records = 0
        collections_updated = 0
        
        for owner in get_global_system_owners():
            records = self._mirror_owner(owner, yyyymmdd)
            total_records += records
            collections_updated += 1
            
        return {"record_count": total_records, "collections_updated": collections_updated}

    def _system_hostname_mirror(self, hostname: str) -> Dict[str, int]:
        """Mirror system data specific to a hostname."""
        yyyymmdd = datetime.now(timezone.utc).strftime("%Y%m%d")
        total_records = 0
        collections_updated = 0
        
        for owner in get_system_hostname_owners(hostname):
            records = self._mirror_owner(owner, yyyymmdd)
            total_records += records
            collections_updated += 1
            
        return {"record_count": total_records, "collections_updated": collections_updated}

    def _user_content_mirror(self, hostname: str) -> Dict[str, int]:
        """Mirror all user content for a hostname."""
        yyyymmdd = datetime.now(timezone.utc).strftime("%Y%m%d")
        total_records = 0
        collections_updated = 0
        
        try:
            # Get all users for this hostname
            user_owner = f"sys.user.{hostname}"
            users = self.docstore.get_list(user_owner)
            logging.info(f"Found {len(users)} users for hostname {hostname}")
            
            for user in users:
                username = user['object_id'].split('.')[-1]
                logging.info(f"Processing user: {username}")
                
                # Process simple user owners
                for getter in [
                    get_user_app_owners,
                    get_user_collection_owners,
                    get_user_stream_owners,
                    get_user_dashboard_owners,
                    get_user_api_gateway_owners,
                    get_user_file_owners,
                ]:
                    for owner in getter(hostname, username):
                        records = self._mirror_owner(owner, yyyymmdd)
                        total_records += records
                        collections_updated += 1
                
                # Process collection extras
                resp = self._user_collections_mirror(hostname, username, yyyymmdd)
                total_records += resp['record_count']
                collections_updated += resp['collections_updated']
                
        except Exception as e:
            logging.error(f"Error in user content mirror: {e}")
            
        return {"record_count": total_records, "collections_updated": collections_updated}

    def _user_collections_mirror(self, hostname: str, username: str, yyyymmdd: str) -> Dict[str, int]:
        """Mirror user collections and their extra patterns."""
        total_records = 0
        collections_updated = 0
        extra_patterns = []
        
        try:
            # Get user collections
            collection_list = self.docstore.get_list(f"{hostname}/{username}/collection")
            
            for collection_rec in collection_list:
                collection_id = collection_rec['object_id']
                
                # Mirror the collection itself
                records = self._mirror_owner(collection_id, yyyymmdd)
                total_records += records
                collections_updated += 1
                
                # Collect extra patterns for this collection
                suffix = collection_id.split('.')[-1]
                extra_patterns.extend(get_user_collection_extra_patterns(hostname, username, suffix))
            
            # Process extra patterns in batches
            for i in range(0, len(extra_patterns), PARQUET_BATCH_SIZE):
                batch = extra_patterns[i:i+PARQUET_BATCH_SIZE]
                try:
                    docs = self.docstore.get_batch_documents(batch)
                    if docs:
                        table = pa.Table.from_pylist(docs)
                        buffer = io.BytesIO()
                        pq.write_table(table, buffer)
                        buffer.seek(0)
                        
                        sanitized_username = _sanitize_owner(username)
                        key = f"{S3_PREFIX}/{yyyymmdd}/{hostname}/{sanitized_username}/collections_extra_{i}.parquet"
                        self.blobstore.save_blob(key, buffer.read())
                        total_records += len(docs)
                        logging.info(f"Mirrored {len(docs)} extra collection records to {key}")
                        
                except Exception as e:
                    logging.error(f"Error processing collection extras batch {i}: {e}")
                    
        except Exception as e:
            logging.error(f"Error in collections mirror for user {username}: {e}")
            
        return {"record_count": total_records, "collections_updated": collections_updated}