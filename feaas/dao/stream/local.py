import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from feaas.dao.docstore.abstract_docstore import AbstractDocstore as Docstore
from feaas.util import common
from collections import defaultdict
import bisect
from decimal import Decimal
import json
import logging
import os
import re
import time


log = logging.getLogger(__name__)
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))


class InMemoryStream(object):


    def __init__(self):
        super().__init__()
        self.streams = defaultdict(list)


    def delete_feed(self, stream_id, timestamp):
        records = self.streams[stream_id]
        index = next((i for i, record in enumerate(records) if record[0] == timestamp), None)
        if index is not None:
            del records[index]
        else:
            raise ValueError(f"No record found with timestamp {timestamp} in stream {stream_id}")


    def update_feed(self, stream_id, timestamp, record):
        records = self.streams[stream_id]
        bisect.insort(records, (timestamp, record))


    def delete_from_stream(self, stream_id, timestamp):
        return delete_feed(stream_id, timestamp)


    def read_stream_value(self, stream_id, timestamp):
        records = self.streams[stream_id]
        # Find the record with the given timestamp
        for ts, record in records:
            if ts == timestamp:
                return record
        raise ValueError(f"No record found with timestamp {timestamp} in stream {stream_id}")


    def read_stream(self, stream_id, after_timestamp=0, limit=10):
        records = self.streams[stream_id]
        # Find the starting point using bisect
        start_index = bisect.bisect_right(records, (after_timestamp, None))
        # Slice the list to get the required number of records
        return records[start_index:start_index + limit]


    # def _query_stream(self, stream_id, kce, eav, limit):
    #     r = self.client.query(
    #         TableName=self.table_name,
    #         KeyConditionExpression=kce,
    #         ExpressionAttributeNames={
    #             '#timestamp': 'timestamp'
    #         },
    #         ExpressionAttributeValues=eav,
    #         Limit=limit
    #     )
    #     ritems = r['Items']
    #     items = []
    #     for ritem in ritems:
    #         item = {}
    #         for k in ritem.keys():
    #             att_val = ritem[k]
    #             item[k] = self._get_value(att_val)
    #         items.append(item)
    #     return items


    # def _get_value(self, att_val):
    #     if 'S' in att_val:
    #         return att_val['S']
    #     elif 'N' in att_val:
    #         return float(att_val['N'])
    #     elif 'B' in att_val:
    #         return bool(att_val['B'])
    #     elif 'BOOL' in att_val:
    #         return bool(att_val['BOOL'])
    #     elif 'L' in att_val:
    #         ilst = att_val['L']
    #         olst = []
    #         for iitem in ilst:
    #             oitem = self._get_value(iitem)
    #             olst.append(oitem)
    #         return olst
    #     elif 'M' in att_val:
    #         return self.deserializer.deserialize(att_val)
    #     else:
    #         for t in att_val.keys():
    #             log.error(f'Not expecting type of {t}')


    def read_stream_recent(self, stream_id, limit=10):
        now = int(time.time() * 1000)
        return self.read_stream_before(stream_id, now, limit)


    def read_stream_before(self, stream_id, before_timestamp, limit=10):
        records = self.streams[stream_id]
        # Find the endpoint using bisect
        end_index = bisect.bisect_left(records, (before_timestamp, None))
        # Slice the list to get the required number of records
        start_index = max(0, end_index - limit)
        return records[start_index:end_index]


    def increment_stream_counter(self, stream_id, timestamp, name, amount=1):
        return None
        # amount = float(amount)
        # res = self.client.update_item(
        #     TableName = self.table_name,
        #     Key = {
        #         "stream_id": { "S" : stream_id },
        #         "timestamp": { "N" : str(timestamp) }
        #     },
        #     ExpressionAttributeNames = {
        #         '#value': name
        #     },
        #     ExpressionAttributeValues = {
        #         ':amount': {
        #             'N': str(amount)
        #         },
        #         ':start': {
        #             'N': '0'
        #         }
        #     },
        #     UpdateExpression = 'SET #value = if_not_exists(#value, :start) + :amount',
        #     # UpdateExpression = 'ADD #value :amount',
        #     ReturnValues = 'UPDATED_NEW'
        # )
        # log.info(f'dynamo.increment_counter resp {res}')
        # return float(res['Attributes'][name]['N'])


    def add_to_list(self, object_id, prop, value):
        return None
        # result = self.table.update_item(
        #     Key={
        #         self.pk: object_id
        #     },
        #     UpdateExpression="SET #list = list_append(if_not_exists(#list, :empty_list), :i)",
        #     ExpressionAttributeNames={'#list': prop},
        #     ExpressionAttributeValues={
        #         ':i': [value],
        #         ':empty_list': []
        #     },
        #     ReturnValues="UPDATED_NEW"
        # )
        # log.info(f'dynamo.add_to_list resp {result}')
        # if result['ResponseMetadata']['HTTPStatusCode'] == 200 and 'Attributes' in result:
        #     return result['Attributes'][prop]
        # else:
        #     return None


    def add_to_stream_list(self, stream_id, timestamp, prop, value):
        return None
        # result = self.table.update_item(
        #     Key={
        #         "stream_id": stream_id,
        #         "timestamp": timestamp
        #     },
        #     UpdateExpression="SET #list = list_append(if_not_exists(#list, :empty_list), :i)",
        #     ExpressionAttributeNames={'#list': prop},
        #     ExpressionAttributeValues={
        #         ':i': [value],
        #         ':empty_list': []
        #     },
        #     ReturnValues="UPDATED_NEW"
        # )
        # log.info(f'dynamo.add_to_stream_list resp {result}')
        # if result['ResponseMetadata']['HTTPStatusCode'] == 200 and 'Attributes' in result:
        #     return result['Attributes'][prop]
        # else:
        #     return None


    def add_to_stream_set(self, stream_id, timestamp, prop, value) -> int:
        return None
        # # return the new size of the set
        # res = self.client.update_item(
        #     TableName=self.table_name,
        #     Key={
        #         "stream_id": {"S": stream_id},
        #         "timestamp": {"N": str(timestamp)}
        #     },
        #     UpdateExpression='ADD #string_set :value',
        #     ExpressionAttributeNames={'#string_set': prop},
        #     ExpressionAttributeValues={':value': {'SS':[str(value)]}},
        #     ReturnValues = 'UPDATED_NEW'
        # )
        # log.info(f'dynamo.add_to_set resp {res}')
        # return len(res['Attributes'][prop]['SS'])

