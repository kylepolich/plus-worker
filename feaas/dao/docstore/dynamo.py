import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from feaas.dao.docstore.abstract_docstore import AbstractDocstore as Docstore
import feaas.objects as objs
from feaas.util import common
from decimal import Decimal
from google.protobuf.json_format import Parse, MessageToDict
import json
import logging
import os
import re
import time


log = logging.getLogger(__name__)
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))


class AstractDynamoDao():

    def _get_value(self, att_val):
        if 'S' in att_val:
            return att_val['S']
        elif 'N' in att_val:
            return float(att_val['N'])
        elif 'B' in att_val:
            return bool(att_val['B'])
        elif 'BOOL' in att_val:
            return bool(att_val['BOOL'])
        elif 'NULL' in att_val:
            return None
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
 
    def _update_document(self, pk, contents):
        ocontents = str(contents)
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
            print('Contents: ', ocontents)
            print('UE: ', ue)
            print('AN: ', ean)
            print('AV: ', eav)
            print("==============================")
            log.error(e)
            try:
                x = json.dumps(contents)
            except:
                x = 'Unserializable data'
            raise e
        return response

    def _get_params(self, contents, ue="set"):
        ean = {}
        eav = {}
        regex = re.compile('[^a-zA-Z0-9_\\- ]')
        if type(contents) == dict:
            d = contents
        else:
            d = contents.as_dict()
        for k in d.keys():
            v = d[k]
            if k != self.pk and v is not None and str(v).strip() != '':
                my_key, ean2 = self._prepare_query(k)
                for k in ean2.keys():
                    k2 = '#' + regex.sub('', k)
                    ean[k2] = ean2[k]
                if type(v) == float:
                    v = Decimal(str(v))
                    k = regex.sub('', k)
                    eav[f':{k}'] = v
                    if len(ue) > 3:
                        ue += ','
                    ue +=  f' {my_key} = :{k}'
                elif type(v) == list:
                    arr = []
                    for i, item in enumerate(v):
                        if type(item) == float:
                            arr.append(Decimal(str(item)))
                        else:
                            arr.append(item)
                    v = arr
                    k = regex.sub('', k)
                    eav[f':{k}'] = v
                    if len(ue) > 3:
                        ue += ','
                    ue +=  f' {my_key} = :{k}'
                else:
                    k = regex.sub('', k)
                    eav[f':{k}'] = v
                    if len(ue) > 3:
                        ue += ','
                    ue +=  f' {my_key} = :{k}'
        return ue, ean, eav

    def _prepare_query(self, name):
        arr = name.split('.')
        if len(arr) == 1:
            regex = re.compile('[^a-zA-Z0-9_ ]')
            k = regex.sub('', name)            
            return f'#{k}', { f'#{k}': name }
        ean = {}
        key_parts = []
        for item in arr:
            key_parts.append('#' + item)
            if item[-1] == ']':
                i = item.find('[')
                ean['#' + item[0:i]] = item[0:i]
            else:
                ean['#' + item] = item
        my_key = '.'.join(key_parts)
        return my_key, ean
    
    def _unpack_value(self, attributes):
        k = list(attributes.keys())[0]
        if k == 'N':
            return int(attributes[k]['N'])
        elif k == 'L':
            val = attributes[k]['L'][0]
            return self._unpack_value(val)
        elif k == 'M':
            return self._unpack_value(attributes[k]['M']) 
            
class DynamoStream(AstractDynamoDao):

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
        # key = {"stream_id": stream_id, "timestamp": {"N": str(int(timestamp))} }
        key = {"stream_id": stream_id, "timestamp": int(timestamp) }
        r = self.table.delete_item(Key=key)
        return r

    def erase_stream(self, stream_id):
        items = self.read_stream(stream_id, limit=1000)
        c = 0
        while len(items) > 0:
            with self.table.batch_writer() as batch:
                for item in items:
                    c += 1
                    batch.delete_item(
                        Key={
                            'stream_id': stream_id,
                            'timestamp': int(item['timestamp'])
                        }
                    )
            items = self.read_stream(stream_id, limit=1000)
        return c


    def save_stream_item(self, stream_id, timestamp, contents):
        t = type(contents)
        if t==dict:
            item = contents
        else:
            item = contents.__dict__
        # item[self.pk] = key
        if 'timestamp' in item:
            tt = item['timestamp']
            item['timestamp'] = timestamp
        else:
            tt = -1
        item = common.clean_json_dict(item)
        print("save_stream_item ts=", tt, timestamp)
        r = self.table.put_item(Item=item)
        sc = r['ResponseMetadata']['HTTPStatusCode']
        if sc != 200:
            # TODO: error in rollbar
            return None
        key = {"stream_id": stream_id, "timestamp": timestamp}
        return key


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

    def read_stream_between(self, stream_id, after_timestamp, before_timestamp, limit=10):
        kce = 'stream_id = :stream_id and #timestamp BETWEEN :after_timestamp AND :before_timestamp'
        eav = {
            ':stream_id': {'S': stream_id},
            ':after_timestamp': {'N': str(after_timestamp)},
            ':before_timestamp': {'N': str(before_timestamp)}
        }
        return self._query_stream(stream_id, kce, eav, limit)

    def read_stream_before(self, stream_id, before_timestamp, limit=10):
        kce = 'stream_id = :stream_id and #timestamp < :before_timestamp'
        eav = {
            ':stream_id': {'S': stream_id},
            ':before_timestamp': {'N': str(before_timestamp)}
        }
        return self._query_stream(stream_id, kce, eav, limit)

    def read_stream_most_recent_before(self, stream_id, before_timestamp, limit=10):
        """the `limit` most-recent items before `before_timestamp`"""
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

    def read_stream_recent(self, stream_id, limit=10, project_expression=None):
        now = int(time.time() * 1000)
        eav = {
            ':stream_id': {'S': stream_id},
            ':before_timestamp': {'N': str(now)}
        }
        ean = {
            '#timestamp': 'timestamp'
        }
        if project_expression is None:
            r = self.client.query(
                TableName=self.table_name,
                KeyConditionExpression='stream_id = :stream_id and #timestamp < :before_timestamp',
                ExpressionAttributeNames = {
                    '#timestamp': 'timestamp'
                },
                ExpressionAttributeValues=eav,
                ScanIndexForward=False,
                Limit=limit
            )
        else:
            arr = project_expression.split(',')
            arr2 = []
            for item in arr:
                i = item.strip()
                if i == 'timestamp':
                    att = '#' + i
                    arr2.append(att)
                else:
                    att = ':' + i
                    ean[att] = i
                    arr2.append(att)
            project_expression2 = ",".join(arr2)
            r = self.client.query(
                TableName=self.table_name,
                KeyConditionExpression='stream_id = :stream_id and #timestamp < :before_timestamp',
                ExpressionAttributeNames = ean,
                ProjectionExpression=project_expression2,
                ExpressionAttributeValues=eav,
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

    def batch_write_stream(self, items, dest_stream_id=None):
        with self.table.batch_writer() as batch:
            for item in items:
                if dest_stream_id is not None:
                    item['stream_id'] = dest_stream_id
                batch.put_item(Item=item)

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
        if (result['ResponseMetadata']['HTTPStatusCode'] == 200) and ('Attributes' in result):
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

    def process_and_batch_update(self, updates, field_name):
        if not updates:
            return

        updates2 = []
        for record in updates:
            # Validate and format keys and values
            try:
                ts = int(record['timestamp'])
                value = record[field_name]
                if not isinstance(value, (int, float)):
                    raise ValueError(f"Invalid value for {field_name}: {value}")
                updates2.append({
                    'Update': {
                        'TableName': self.table_name,
                        'Key': {
                            'stream_id': {'S': record['stream_id']},  # Partition key
                            'timestamp': {'N': str(ts)}  # Sort key
                        },
                        'UpdateExpression': f'SET #{field_name} = :{field_name}',
                        'ExpressionAttributeNames': {
                            f'#{field_name}': field_name
                        },
                        'ExpressionAttributeValues': {
                            f':{field_name}': {'N': str(value)}
                        }
                    }
                })
            except (KeyError, ValueError) as e:
                print(f"Skipping record due to error: {e}")

        batch_size = 25
        for i in range(0, len(updates2), batch_size):
            batch = updates2[i:i + batch_size]
            try:
                self.client.transact_write_items(
                    TransactItems=batch,
                    ReturnConsumedCapacity='INDEXES',
                    ReturnItemCollectionMetrics='SIZE'
                )
            except Exception as e:
                print(f"Error in batch {i // batch_size + 1}: {e}")

        # for i in range(0, len(updates2), batch_size):
        #     batch = updates2[i:i+batch_size]
        #     print(batch)
        #     self.client.transact_write_items(TransactItems=batch)


class DynamoDocstore(AstractDynamoDao, Docstore):
   
    def __init__(self, table_name, access_key=None, secret_key=None):
        super().__init__()
        self.table_name = table_name
        self.deserializer = TypeDeserializer() # To go from low-level format to python
        self.serializer = TypeSerializer() # To go from python to low-level format
        self.pk = 'object_id'
        self.access_key = access_key
        self.secret_key = secret_key
        self.cache = {}
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

    def get_document(self, object_id, cache_result=None):
        if cache_result is not None:
            if object_id in self.cache:
                c = self.cache[object_id]
                if c['ttl'] > int(time.time()):
                    return c['item']

        log.debug(f'Docstore get_document {object_id}')
        k = {}
        k[self.pk] = object_id
        try:
            resp = self.table.get_item(Key=k)
            item = resp['Item']
            for k in item.keys():
                v = item[k]
                if type(v) == Decimal:
                    item[k] = float(v)
            if 'ttl' in item:
                ttl = item['ttl']
                now = int(time.time())
                if ttl != 0 and ttl < now:
                    self.delete_document(object_id)
                    return None
            if cache_result is not None:
                c = { "ttl": int(time.time()) + int(cache_result), "item": item }
                self.cache[object_id] = c
            return item
        except:
            return None

    def delete_document(self, key):
        k = {}
        k[self.pk] = key
        r = self.table.delete_item(Key=k)
        return r

    def erase_list(self, owner):
        limit=1000
        items = self.get_list(owner, limit=limit)
        c = 0
        while len(items) > 0:
            with self.table.batch_writer() as batch:
                for item in items:
                    c += 1
                    batch.delete_item(
                        Key={
                            'object_id': item['object_id']
                        }
                    )
            items = self.get_list(owner, limit=limit)
        return c
   
    def save_document(self, key, contents):
        t = type(contents)
        if t==dict:
            item = contents
        else:
            item = contents.__dict__
        item[self.pk] = key
        item = common.clean_json_dict(item)
        r = self.table.put_item(Item=item)
        sc = r['ResponseMetadata']['HTTPStatusCode']
        if sc != 200:
            # TODO: error in rollbar
            return None
        return key

    def update_document(self, key, contents):
        if contents == {}:
            return None
        pk = { self.pk: key }
        return self._update_document(pk, contents)

    def batch_write_objects(self, items):
        with self.table.batch_writer() as batch:
            for item in items:
                batch.put_item(Item=item)

                
    def get_paginated(self, idx_name, last_evaluated_key=None, eav=None, kce=None):
        """
        resp = self.client.query(
           TableName=self.table_name,
           IndexName=idx_name,
           ExpressionAttributeValues=eav,
           KeyConditionExpression=kce,
        )
        """
        if last_evaluated_key:
            response = self.table.scan(ExclusiveStartKey=last_evaluated_key)
        else:
            response = self.table.scan()

        c = response['Count']
        sc = response['ScannedCount']
        log.debug(f'Count: {c} | Scanned Count: {sc}')

        if 'LastEvaluatedKey' in response:
            return response['Items'], response['LastEvaluatedKey']
        else:
            return response['Items'], None

    
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

    def increment_counter(self, object_id, name, amount=1, update=None):
        my_key, ean = self._prepare_query(name)
        eav = {
            ':amount': { 'N': str(amount) },
            ':start': { 'N': '0' }
        }
        ue = f'SET {my_key} = if_not_exists({my_key}, :start) + :amount'
        contents = {}
        if update is not None:
            for k in update.keys():
                v = update[k]
                x = 'S'
                if type(v) == int or type(v) == float:
                    x = 'N'
                contents[k] = { x : str(v) }
        ue, ean2, eav2 = self._get_params(contents, ue)
        for k in ean2.keys():
            ean[k] = ean2[k]
        for k in eav2.keys():
            eav[k] = eav2[k]
        res = self.client.update_item(
            TableName = self.table_name,
            Key = { self.pk: { 'S': object_id } },
            ExpressionAttributeNames = ean,
            ExpressionAttributeValues = eav,
            UpdateExpression = ue,
            # UpdateExpression = 'ADD #value :amount',
            ReturnValues = 'UPDATED_NEW'
        )
        attributes = res['Attributes']
        if name in attributes:
            return attributes[name]['N']
        else:
            return self._unpack_value(attributes)

    def increment_counter2(self, object_id, name, amount=1):
        # TODO: get rid of this now that increment_counter was upgraded for nested stuff
        arr = name.split('.')
        vchain = ""
        ean = {}
        for i, item in enumerate(arr):
            if i == 0:
                vchain += f'#value{i}'
            else:
                vchain += f'.#value{i}'
            ean[f'#value{i}'] = item
        ue = f'SET {vchain} = if_not_exists({vchain}, :start) + :amount'
        res = self.client.update_item(
            TableName = self.table_name,
            Key = {
                self.pk: {
                    'S': object_id
                }
            },
            ExpressionAttributeNames = ean,
            ExpressionAttributeValues = {
                ':amount': {
                    'N': str(amount)
                },
                ':start': {
                    'N': '0'
                }
            },
            UpdateExpression = ue,
            # UpdateExpression = 'ADD #value :amount',
            ReturnValues = 'UPDATED_NEW'
        )
        log.info(f'dynamo.increment_counter resp {res}')
        attribs = res['Attributes']
        for i, item in enumerate(arr):
            if item not in attribs:
                return None
            if i == len(arr) - 1:
                return item
            else:
                attribs = attribs[item]
                # remove dynamo type
                t = list(attribs.keys())[0]
                attribs = attribs[t]
        return None

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

    def add_to_set(self, object_id, prop, value) -> int:
        # return the new size of the set
        res = self.client.update_item(
            TableName=self.table_name,
            Key={
                self.pk: {
                    'S':object_id
                }
            },
            UpdateExpression='ADD #string_set :value',
            ExpressionAttributeNames={'#string_set': prop},
            ExpressionAttributeValues={':value': {'SS':[str(value)]}},
            ReturnValues = 'UPDATED_NEW'
        )
        log.info(f'dynamo.add_to_set resp {res}')
        return len(res['Attributes'][prop]['SS'])

    def delete_from_set(self, object_id, prop, value) -> bool:
        # return boolean if it value present or not
        res = self.client.update_item(
            TableName=self.table_name,
            Key={
                self.pk: {
                    'S':object_id
                }
            },
            UpdateExpression='DELETE #string_set :value',
            ExpressionAttributeNames={'#string_set': prop},
            ExpressionAttributeValues={':value': {'SS':[str(value)]}},
            ReturnValues = 'UPDATED_OLD'
        )
        log.info(f'dynamo.delete_from_set resp {res}')
        return value in res['Attributes'][prop]['SS']

    def remove_attribute(self, object_id, prop):
        res = self.client.update_item(
            TableName=self.table_name,
            Key={
                self.pk: {
                    'S':object_id
                }
            },
            UpdateExpression='REMOVE #prop',
            ExpressionAttributeNames={'#prop': prop}
        )
        log.info(f'dynamo.remove_attribute resp {res}')
        return True


    def get_list_with_filter(self, search_value, filterExpression, limit=None):
        owner_index = 'ownerIndex'
        if limit is None:
            limit = 25
        key_condition = Key('owner').eq(search_value)
        res = self.table.query(
            IndexName=owner_index,
            KeyConditionExpression=key_condition,
            FilterExpression=filterExpression,
            Limit=limit
        )
        items = res['Items']
        return items


    def owner_query(self, owner_index, attribute, search_value, limit=None):
        if limit is None:
            res = self.table.query(
                    IndexName = owner_index,
                    KeyConditionExpression = Key(attribute).eq(search_value)
                )
        else:
            res = self.table.query(
                    IndexName = owner_index,
                    KeyConditionExpression = Key(attribute).eq(search_value),
                    Limit=limit
                )
        items = res['Items']
        return items

    def get_list(self, search_value, limit=None, cache_result=60*15):
        owner_index = 'ownerIndex'
        attribute = 'owner'
        k = f'owner:{search_value}'
        if cache_result is not None:
            if k in self.cache:
                c = self.cache[k]
                if c['ttl'] > int(time.time()):
                    return c['item']
        result = self.owner_query(owner_index, attribute, search_value, limit)
        if cache_result is not None:
            citem = { "ttl": int(time.time()) + cache_result, "item": result }
            self.cache[k] = citem
        return result


    def get_batch_documents(self, object_ids, cache_result=None):
        result = []
        remaining_object_ids = list(object_ids)

        if cache_result is not None:
            for object_id in object_ids:
                if object_id in self.cache:
                    c = self.cache[object_id]
                    if c['ttl'] > int(time.time()):
                        result.append(c['item'])
                        remaining_object_ids.remove(object_id)

        if len(remaining_object_ids) == 0:
            return result

        if len(remaining_object_ids) < 100:
            result2 = self._get_batch_documents_1kmax(remaining_object_ids)
            result.extend(result2)
            if cache_result is not None:
                ttl_value = int(time.time()) + cache_result
                for object_id, doc in zip(remaining_object_ids, result2):
                    self.cache[object_id] = {'item': doc, 'ttl': ttl_value}
        else:
            lst = list(remaining_object_ids)
            while len(lst) > 0:
                n = len(lst)
                if n > 100:
                    n = 100
                arr = lst[0:n]
                lst = lst[n:]
                arr2 = self._get_batch_documents_1kmax(arr)
                result.extend(arr2)
                if cache_result is not None:
                    ttl_value = int(time.time()) + cache_result
                    for object_id, doc in zip(arr, arr2):
                        self.cache[object_id] = {'item': doc, 'ttl': ttl_value}

        return result


    def _get_batch_documents_1kmax(self, object_ids):
        """
        typical access pattern is to collect object_ids from a call to gsi_query with a given owner
        and then pass the object_ids into this function to get their full records
        """
        res = self.client.batch_get_item( # used client b/c didn't see resource method -> deserializer
            RequestItems={
                self.table_name:{'Keys': 
                    [
                        {self.pk:{'S':object_id}} for object_id in object_ids
                    ]
                }
            }
        )
        raw_items = res['Responses'][self.table_name]
        if res['UnprocessedKeys']:
            print("Warning, unprocessed keys left in query results:")
            print(res['UnprocessedKeys'])
        items = [
            {k: self.deserializer.deserialize(v) for k,v in item.items()} for item in raw_items
        ]
        return items


    def save_object(self, protobuf_object):
        object_id = protobuf_object.object_id
        o = MessageToDict(protobuf_object, preserving_proto_field_name=True, including_default_value_fields=True)
        # TODO: fix integer fields in a more general way
        if type(protobuf_object) == objs.PortalScriptJob:
            fix_ints = ['ttl', 'last_updated_at', 'last_compute_ms', 'wait_count', 'success_count', 'running_count', 'failed_count', 'iterations_left', 'last_compute_ms', 'last_updated_at']
        else:
            fix_ints = []
        for prop in fix_ints:
            if prop in o:
                o[prop] = int(o[prop])
        return self.save_document(object_id, o)

