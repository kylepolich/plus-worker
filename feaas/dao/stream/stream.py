# TODO: finish this work.  This is just a copy over from Docstore and cleanup

import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from feaas.dao.docstore.abstract_docstore import AbstractDocstore as Docstore
from feaas.util import common
from decimal import Decimal
import json
import logging
import os
import re
import time


log = logging.getLogger(__name__)
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))


class StreamConnection(object):


    def __init__(self, table_name, access_key=None, secret_key=None):
        super().__init__()
        self.table_name = table_name
        self.deserializer = TypeDeserializer() # To go from low-level format to python
        self.serializer = TypeSerializer() # To go from python to low-level format
        self.pk = 'stream_id'
        self.access_key = access_key
        self.secret_key = secret_key
        if access_key is not None:
            #region = os.getenv("AWS_REGION")
            region = 'us-east-1'
            self.client = boto3.client('dynamodb', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region)
            self.table = boto3.resource('dynamodb', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region).Table(table_name)
            self.has_iam = True
        else:
            self.client = boto3.client('dynamodb')
            self.table = boto3.resource('dynamodb').Table(table_name)
            self.has_iam = False


    def delete_feed(self, stream_id, timestamp):
        key = {"stream_id": stream_id, "timestamp": {"N": str(int(timestamp))} }
        key = {"stream_id": stream_id, "timestamp": int(timestamp) }
        r = self.table.delete_item(Key=key)
        return r


    def update_feed(self, stream_id, timestamp, contents):
        if contents == {}:
            return None
        key = {"stream_id": stream_id, "timestamp": timestamp}
        return self._update_document(key, contents)


    def delete_from_stream(self, stream_id, timestamp):
        key = {"stream_id": stream_id, "timestamp": timestamp}
        r = self.table.delete_item(Key=key)
        return r


    def read_stream_value(self, stream_id, timestamp):
        kce = 'stream_id = :stream_id and #timestamp = :timestamp'
        eav = {
            ':stream_id': {'S': str(stream_id)},
            ':timestamp': {'N': str(timestamp)}
        }
        items = self._query_stream(stream_id, kce, eav, 1)
        if len(items) == 0:
            return None
        else:
            return items[0]


    def read_stream(self, stream_id, after_timestamp=0, limit=10):
        kce = 'stream_id = :stream_id and #timestamp > :after_timestamp'
        eav = {
            ':stream_id': {'S': stream_id},
            ':after_timestamp': {'N': str(after_timestamp)}
        }
        return self._query_stream(stream_id, kce, eav, limit)


    def _query_stream(self, stream_id, kce, eav, limit):
        r = self.client.query(
            TableName=self.table_name,
            KeyConditionExpression=kce,
            ExpressionAttributeNames={
                '#timestamp': 'timestamp'
            },
            ExpressionAttributeValues=eav,
            Limit=limit
        )
        ritems = r['Items']
        items = []
        for ritem in ritems:
            item = {}
            for k in ritem.keys():
                att_val = ritem[k]
                item[k] = self._get_value(att_val)
            items.append(item)
        return items


    def _get_value(self, att_val):
        if 'S' in att_val:
            return att_val['S']
        elif 'N' in att_val:
            return float(att_val['N'])
        elif 'B' in att_val:
            return bool(att_val['B'])
        elif 'BOOL' in att_val:
            return bool(att_val['BOOL'])
        elif 'L' in att_val:
            ilst = att_val['L']
            olst = []
            for iitem in ilst:
                oitem = self._get_value(iitem)
                olst.append(oitem)
            return olst
        elif 'M' in att_val:
            return self.deserializer.deserialize(att_val)
        else:
            for t in att_val.keys():
                log.error(f'Not expecting type of {t}')


    def read_stream_recent(self, stream_id, limit=10):
        now = int(time.time() * 1000)
        return self.read_stream_before(stream_id, now, limit)


    def read_stream_before(self, stream_id, before_timestamp, limit=10):
        r = self.client.query(
            TableName=self.table_name,
            KeyConditionExpression='stream_id = :stream_id and #timestamp < :before_timestamp',
            ExpressionAttributeNames = {
                '#timestamp': 'timestamp'
            },
            ExpressionAttributeValues={
                ':stream_id': {'S': stream_id},
                ':before_timestamp': {'N': str(before_timestamp)}
            },
            ScanIndexForward=False,
            Limit=limit
        )
        ritems = r['Items']
        items = []
        for ritem in ritems:
            item = {}
            for k in ritem.keys():
                att_val = ritem[k]
                item[k] = self._get_value(att_val)
            items.append(item)
        return items


    def _get_params(self, contents):
        ue = "set"
        ean = {}
        eav = {}
        if type(contents) == dict:
            d = contents
        else:
            d = contents.as_dict()
        for k in d.keys():
            if k != self.pk:
                v = d[k]
                if type(v) == float:
                    v = Decimal(str(v))
                    regex = re.compile('[^a-zA-Z0-9_ ]')
                    k = regex.sub('', k)
                    ean[f'#{k}'] = k
                    eav[f':{k}'] = v
                    if len(ue) > 3:
                        ue += ','
                    ue +=  f' #{k} = :{k}'
                elif v is None or str(v).strip() == '':
                    pass
                elif type(v) == list:
                    arr = []
                    for i, item in enumerate(v):
                        if type(item) == float:
                            arr.append(Decimal(str(item)))
                        else:
                            arr.append(item)
                    v = arr
                    regex = re.compile('[^a-zA-Z0-9_ ]')
                    k = regex.sub('', k)
                    ean[f'#{k}'] = k
                    eav[f':{k}'] = v
                    if len(ue) > 3:
                        ue += ','
                    ue +=  f' #{k} = :{k}'
                else:
                    regex = re.compile('[^a-zA-Z0-9_ ]')
                    k = regex.sub('', k)
                    ean[f'#{k}'] = k
                    eav[f':{k}'] = v
                    if len(ue) > 3:
                        ue += ','
                    ue +=  f' #{k} = :{k}'
        return ue, ean, eav


    def _update_document(self, pk, contents):
        ue, ean, eav = self._get_params(contents)
        try:
            response = self.table.update_item(
                Key=pk,
                UpdateExpression=ue,
                ExpressionAttributeNames=ean,
                ExpressionAttributeValues=eav,
                ReturnValues="UPDATED_NEW"
            )
        except Exception as e:
            print("==============================")
            print('PK: ', pk)
            print("------------------------------")
            print('Contents: ', contents)
            print('UE: ', ue)
            print('AN: ', ean)
            print('AV: ', eav)
            print("==============================")
            log.error(e)
            raise e
        return response


    def get(self, idx_name, eav=None, kce=None):
        """
        resp = self.client.query(
           TableName=self.table_name,
           IndexName=idx_name,
           ExpressionAttributeValues=eav,
           KeyConditionExpression=kce,
        )
        """
        response = self.table.scan()
        c = response['Count']
        sc = response['ScannedCount']
        log.debug(f'Count: {c} | Scanned Count: {sc}')
        data = response['Items']
        return data


    def search(self, pattern):
        """ Please override this.  Your persistence layer should do better! """
        items = self.get_all() # TODO: in need of optimization
        results = []
        for item in items:
            k = item[self.pk]
            # ext = Uri2S3key.get_extension(pattern)
            # item_key = Uri2S3key.encode(k, ext)
            if common.match(pattern, k):
                results.append(item)
        log.info(f'====[{len(results)}, {type(results)}]========')
        return results


    def increment_stream_counter(self, stream_id, timestamp, name, amount=1):
        amount = float(amount)
        res = self.client.update_item(
            TableName = self.table_name,
            Key = {
                "stream_id": { "S" : stream_id },
                "timestamp": { "N" : str(timestamp) }
            },
            ExpressionAttributeNames = {
                '#value': name
            },
            ExpressionAttributeValues = {
                ':amount': {
                    'N': str(amount)
                },
                ':start': {
                    'N': '0'
                }
            },
            UpdateExpression = 'SET #value = if_not_exists(#value, :start) + :amount',
            # UpdateExpression = 'ADD #value :amount',
            ReturnValues = 'UPDATED_NEW'
        )
        log.info(f'dynamo.increment_counter resp {res}')
        return float(res['Attributes'][name]['N'])


    def add_to_list(self, object_id, prop, value):
        result = self.table.update_item(
            Key={
                self.pk: object_id
            },
            UpdateExpression="SET #list = list_append(if_not_exists(#list, :empty_list), :i)",
            ExpressionAttributeNames={'#list': prop},
            ExpressionAttributeValues={
                ':i': [value],
                ':empty_list': []
            },
            ReturnValues="UPDATED_NEW"
        )
        log.info(f'dynamo.add_to_list resp {result}')
        if result['ResponseMetadata']['HTTPStatusCode'] == 200 and 'Attributes' in result:
            return result['Attributes'][prop]
        else:
            return None


    def add_to_stream_list(self, stream_id, timestamp, prop, value):
        result = self.table.update_item(
            Key={
                "stream_id": stream_id,
                "timestamp": timestamp
            },
            UpdateExpression="SET #list = list_append(if_not_exists(#list, :empty_list), :i)",
            ExpressionAttributeNames={'#list': prop},
            ExpressionAttributeValues={
                ':i': [value],
                ':empty_list': []
            },
            ReturnValues="UPDATED_NEW"
        )
        log.info(f'dynamo.add_to_stream_list resp {result}')
        if result['ResponseMetadata']['HTTPStatusCode'] == 200 and 'Attributes' in result:
            return result['Attributes'][prop]
        else:
            return None


    def add_to_stream_set(self, stream_id, timestamp, prop, value) -> int:
        # return the new size of the set
        res = self.client.update_item(
            TableName=self.table_name,
            Key={
                "stream_id": {"S": stream_id},
                "timestamp": {"N": str(timestamp)}
            },
            UpdateExpression='ADD #string_set :value',
            ExpressionAttributeNames={'#string_set': prop},
            ExpressionAttributeValues={':value': {'SS':[str(value)]}},
            ReturnValues = 'UPDATED_NEW'
        )
        log.info(f'dynamo.add_to_set resp {res}')
        return len(res['Attributes'][prop]['SS'])


    def batch_write_stream(self, items, dest_stream_id=None):
        with self.table.batch_writer() as batch:
            for item in items:
                if dest_stream_id is not None:
                    item['stream_id'] = dest_stream_id
                batch.put_item(Item=item)
