# TODO: also for copying prod to staging
# TODO: destination of S3 or Dynamo and object_ids
# TODO: Standardize more!  Like all owners are enums from protobuf or something

from datetime import datetime
import feaas.objects as objs
from feaas.util import common
from google.protobuf.json_format import Parse, MessageToDict
import json
import traceback
import time
from feaas.abstract import AbstractAction
import io
import pyarrow as pa
import pyarrow.parquet as pq
import logging
from boto3.dynamodb.conditions import Attr, Key


# TODO: checks all records based on last_updated_at?


class PerformBackup(AbstractAction):


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
        outputs = [record_count]
        super().__init__(params, outputs)
        self.primary_output = 'record_count'
        self.docstore = dao.get_docstore()
        self.streams = dao.get_streams()


    def execute_action(self, hostname) -> objs.Receipt:
        t0 = int(time.time())
        global_resp = self._system_hostname_backup(hostname)
        t1 = int(time.time())
        sys_resp = self._global_backup()
        t2 = int(time.time())
        ugc_resp = self._user_content_backup(hostname)
        t3 = int(time.time())

        # TODO: include count of total users per hostname
        # TODO: include count of streams updated
        # TODO: include count of individual feature usage (e.g. total count CollectionTriggers)

        outputs = {
            'global_backup_duration': objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=(t1-t0)),
            'global_backup_record_count': objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=global_resp['record_count']),
            'global_backup_collections_udpated': objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=global_resp['collections_udpated']),
            'sys_backup_duration': objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=(t2-t1)),
            'sys_backup_record_count': objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=sys_resp['record_count']),
            'sys_backup_collections_udpated': objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=sys_resp['collections_udpated']),
            'ugc_backup_duration': objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=(t3-t2)),
            'ugc_backup_record_count': objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=ugc_resp['record_count']),
            'ugc_backup_collections_udpated': objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=ugc_resp['collections_udpated'])
        }
        return objs.Receipt(success=True, outputs=outputs)


    def _get_global_system_owners(self):
        return [
            'sys.services-providers',
            'sys.env',
            'sys.oauth2.client'
        ]


    def _backup_owner(self, owner, yyyymmdd) -> int:
        record_count = 0
        i = 0
        all_object_ids = []

        # Paginated query to collect object_ids
        last_evaluated_key = None
        while True:
            res = self.table.query(
                IndexName='ownerIndex',
                KeyConditionExpression=Key('owner').eq(owner),
                FilterExpression=Attr('object_id').exists(),
                Limit=100,
                ExclusiveStartKey=last_evaluated_key
            )
            batch = res.get('Items', [])
            all_object_ids.extend([item['object_id'] for item in batch])
            record_count += len(batch)
            last_evaluated_key = res.get('LastEvaluatedKey')
            if not last_evaluated_key:
                break
            i += 1

        if not all_object_ids:
            return 0

        # Get full documents from docstore
        records = self.docstore.get_batch_documents(all_object_ids)
        if not records:
            return 0

        # Convert to Arrow Table
        table = pa.Table.from_pylist(records)

        # Write to in-memory Parquet file
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)
        bytez = buffer.read()

        # Save to blobstore
        dest_key = f'sys/backup/{yyyymmdd}/{owner}/backup_{i}.parquet'
        self.blobstore.save_blob(dest_key, bytez)
        logging.info(f"Saved {dest_key}")

        return record_count


    def _get_common_system_hostname_owners(self, hostname):
        owners = [
            f'sys.user.{hostname}',
            f'sys.{hostname}.app',
            f'sys.{hostname}.users.groups'
            # TODO: sys.{hostname}.toolbar-params
        ]
        if hostname == 'app_neonpixel_co':
            owners.extend([
                'sys.app_neonpixel_co.clients',
                'sys@neonpixel.co/campaign-groups',
                'sys@neonpixel.co/creative-groups'
            ])
        return owners


    def _system_hostname_backup(self, hostname):
        collections_updated = 0
        record_count = 0
        owners = self._get_common_system_hostname_owners(hostname)
        for owner in owners:
            record_count += self._backup_owner(owner)
            collections_updated += 1
        return { "record_count": record_count, "collections_updated": collections_updated }


    def _global_backup(self):
        collections_updated = 0
        record_count = 0
        sys_owners = self._get_global_system_owners()
        for owner in owners:
            record_count += self._backup_owner(owner)
            collections_updated += 1

        return { "record_count": record_count, "collections_updated": collections_updated }


    def _user_content_backup(self, hostname):
        collections_updated = 0
        record_count = 0

        # TODO: do in separate account bucket eventually
        yyyymmdd = str(datetime.now())[0:10]

        # TODO: paginate over all
        user_owner = f'sys.user.{hostname}'
        n = len(user_owner) + 1
        oids = []
        for item in self.docstore.get_document(user_owner):
            individual_object_ids = []
            oid = item['object_id']
            username = oid[n:]
            logging.info(f"Backup username={username}")
            owners = [
                f"{oid}.apps" # sys.user.{hostname}.{username}.apps
            ]
            for owner in owners:
                record_count += self._backup_owner(owner)
                collections_updated += 1

            resp = self._user_collections_backup(hostname, username)
            record_count += resp['record_count']
            collections_updated += resp['collections_updated']

            resp = self._user_streams_backup(hostname, username)
            record_count += resp['record_count']
            collections_updated += resp['collections_updated']

            resp = self._user_dashboard_backup(hostname, username)
            record_count += resp['record_count']
            collections_updated += resp['collections_updated']

            resp = self._user_api_gateway_backup(hostname, username)
            record_count += resp['record_count']
            collections_updated += resp['collections_updated']

            resp = self._user_files_backup(hostname, username)
            record_count += resp['record_count']
            collections_updated += resp['collections_updated']


        return { "record_count": record_count, "collections_updated": collections_updated }


    def _user_collections_backup(self, hostname, username):
        collections_updated = 0
        record_count = 0
        individual_object_ids = []

        user_collections = self.docstore.get_list(f'{hostname}/{username}/collection')  # main records
        for rec in user_collections:
            collection_object_id = rec['object_id']
            record_count += self._backup_owner(collection_object_id)  # all records in this collection
            collectionId = collection_object_id.split(".")[-1]
            individual_object_ids.append(f'{hostname}/{username}/collection-input.{collectionId}')
            individual_object_ids.append(f'{hostname}/{username}/collections-shared.{collectionId}')
            # TODO: collections (collection trigger)
            # TODO: inputs shared
            collections_updated += 1

        # Split into batches of 500 to avoid overloading
        batch_size = 500
        for i in range(0, len(individual_object_ids), batch_size):
            batch_ids = individual_object_ids[i:i + batch_size]
            loose_objects = self.docstore.get_batch_documents(batch_ids)
            record_count += len(loose_objects)
            self.docstore.batch_write_stream(loose_objects)

        return { "record_count": record_count, "collections_updated": collections_updated }


    def _user_streams_backup(self, hostname, username):
        # TODO: streams (main records)
        # TODO: streams (need stream_id backup)
        # TODO: streams (stream trigger)
        collections_updated = 0
        record_count = 0
        return { "record_count": record_count, "collections_updated": collections_updated }
        

    def _user_dashboard_backup(self, hostname, username):
        collections_updated = 0
        record_count = 0
        # TODO: dashboards (*)
        return { "record_count": record_count, "collections_updated": collections_updated }
        

    def _user_api_gateway_backup(self, hostname, username):
        collections_updated = 0
        record_count = 0
        # TODO: api_gateway (*)
        return { "record_count": record_count, "collections_updated": collections_updated }
        

    def _user_files_backup(self, hostname, username):
        collections_updated = 0
        record_count = 0
        # TODO: file_triggers (*)
        return { "record_count": record_count, "collections_updated": collections_updated }
    

