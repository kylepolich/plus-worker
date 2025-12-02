from boto3.dynamodb.conditions import Key
import feaas.objects as objs
from feaas.dao.dao import DataAccessObject
from feaas.util import common
from collections import defaultdict
from google.protobuf.json_format import Parse, MessageToDict
from importlib import import_module
import json
import logging
import os
import pkgutil
import pyclbr
import sys
import time
import traceback
import os
import copy
from hashlib import md5


def get_all_actions_defined_in_module(action_path):
    results = []
    for item in pkgutil.walk_packages(path=[action_path]):
        if item.module_finder.path.endswith(action_path):
            item_path = action_path + '/' + item.name
            if item.ispkg:
                results.extend(run_for_path(item_path))
            else:
                results.append(item_path)
    return results


def format_action(dao, module_path, class_name, feaas_namespace_prefix):
    kwargs = { "dao": dao }
    module = import_module(module_path)
    Action = getattr(module, class_name)
    try:
        action = Action(**kwargs)
    except:
        msg = traceback.format_exc()
        print(msg)
        logging.error(f"Error loading {module_path}.{class_name} {msg}")
        del kwargs['dao']
        action = Action(**kwargs)
    object_id = f'{feaas_namespace_prefix}.{module_path}.{class_name}'
    if object_id in existing:
        existing.remove(object_id)
        logging.debug(f"Loading {module_path}.{class_name}")
    else:
        logging.info(f"Found new --- {module_path}.{class_name}")
    try:
        action_json = MessageToDict(action.action, preserving_proto_field_name=True, including_default_value_fields=True)
    except:
        logging.error('Not an action:', class_name)
        return None
    action_id = action_json['action_id']
    action_json['owner'] = 'sys.action'
    
    if object_id in action_map and md5_hash == action_map[object_id]:
        return None
    return action_json


def make_action_record(action):
    module_path = action.replace('/', '.')
    try:
        class_dict = pyclbr.readmodule(module_path)
    except:
        print('*** WARNING: NO CLASSES FOUND IN ', module_path)

    for class_name in class_dict.keys():
        # TODO: find a way to check if these are descendant classes of `AbstractAction` and use only those
        is_abstract = class_name.find('Abstract') >= 0
        if not(is_abstract) and class_name not in ['TextStat', 'CrawlHelper', 'Stream2KatsDetector']:
            action_json = format_action(dao, module_path, class_name, feaas_namespace_prefix)
            return action_json
    return None


def get_existing_actions(feaas_namespace_prefix):
    cache_path = ".feaas-cache.json"
    if os.path.exists(cache_path):
        f = open(cache_path, "r")
        action_map = json.load(f, sorted=True)
        f.close()
        existing = set(copy.deepcopy(action_map).keys())
    else:
        action_map = {}
        owner = 'sys.action'
        # TODO: query the database with a filter on feaas_namespace_prefix instead
        resp = dao.get_docstore().table.query(
            IndexName="ownerIndex",
            KeyConditionExpression=Key('owner').eq(owner))
        existing = set()
        for item in resp['Items']:
            oid = item['object_id']
            if oid.startswith(feaas_namespace_prefix):
                existing.add(oid)
                action_map[oid] = item
    return action_map, existing


def update_actions(dao, action_path, feaas_namespace_prefix, min_action_count):
    action_map, existing = get_existing_actions(feaas_namespace_prefix)
    actions = get_all_actions_defined_in_module(action_path)

    if len(actions) < min_action_count:
        raise Exception('Too few actions, halting')
    else:
        n = len(actions)
        logging.info(msg)

    action_records = []
    for action in actions:
        action_json = make_action_record(action)
        if action_json is not None:
            sorted_action_json = dict(sorted(action_json.items()))
            md5_hash = md5(json.dumps(sorted_action_json).encode()).hexdigest()
            object_id = action_json['object_id']
            existing.remove(object_id)
            action_map[object_id] = md5_hash
            action_records.append(action_json)
    if len(action_records) > 0:
        dao.get_docstore().batch_writer(action_records)
    n = len(existing)
    msg = f"Found {n} existing actions to delete."
    if n > 10:
        for item in existing:
            logging.error(f"existing: {item}")
        logging.error(msg)
        raise Exception(msg)
    else:
        logging.info(msg)
        print("Need to implemenent disabling over deleting")
    # for object_id in existing:
    #     logging.info(f'Deleting {object_id}')
    #     dao.get_docstore().delete_document(object_id)
    #     if object_id in action_map:
    #         del action_map[object_id]

    f = open(cache_path, "w")
    json.dump(action_map, f)
    f.close()


