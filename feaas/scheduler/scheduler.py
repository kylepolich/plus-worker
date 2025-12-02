"""
The `run_scheduler.py` file contains an set of `Action` classes which act as drivers of the scheduler.
Their purpose is to serve as an operations interface.  For example, they manage database interactions.
Further, they consider `last_updated_at` (at the user/account level) to minimize wasting time.

In contrast, the `scheduler.py` file contains no data access layer.  It's the core of the scheduler.
When called, it does the work it's asked to do without consideration for billing or efficiency.
Logic in this file will consider whether or not to run a ScheduledItem which passed the "first check"
layer in `run_scheduler.py` `Actions`.
"""


# TODO: when calling API, write to queue so its async and all scheduler is just flywheel, even calling itself!
# TODO: add stream to my account to monitor via admin!
# TODO: put on a dashboard!

from boto3.dynamodb.conditions import Key
import feaas.objects as objs
from feaas.util import common
from feaas.psee.psee import PlusScriptExecutionEngine
from feaas.sys.billing.billing import Billing
from feaas.sys.executor import ActionExecutor
from croniter import croniter
from datetime import datetime, timedelta
from google.protobuf.json_format import Parse, MessageToDict
from feaas.stream import util
from typing import List
import json
import logging
import os
import requests
import time
import traceback


log = logging.getLogger(__name__)
logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))


def should_run_fn(last_updated_at, cron_string, now=None) -> bool:
    if now is None:
        now = int(time.time())
    cr = croniter(cron_string, now)
    p = cr.get_prev()
    if last_updated_at >= p:
        return False
    else:
        return True


class ScheduledItem:


    def __init__(self, object_id, owner, action_id, username, cron, last_updated_at, label, params):
        self.object_id = object_id
        self.owner = owner
        self.action_id = action_id
        self.username = username
        self.cron = cron
        self.last_updated_at = last_updated_at
        self.label = label
        self.params = params


    def __dict__(self):
        return {
            'object_id': self.object_id,
            'owner': self.owner,
            'action_id': self.action_id,
            'username': self.username,
            'cron': self.cron,
            'last_updated_at': self.last_updated_at,
            'label': self.label,
            'params': self.params
        }


class Scheduler(object):


    def __init__(self, scheduled_items: List[ScheduledItem], feaas_host, feaas_token):
        self.scheduled_items = scheduled_items
        self.feaas_host = feaas_host
        self.feaas_token = feaas_token
        # self.action_executor = ActionExecutor(dao, sys_name)


    def run(self):
        success_count = 0
        failure_count = 0
        skip_count = 0
        receipts = []
        for scheduled_item in self.scheduled_items:
            last_updated_at = scheduled_item.last_updated_at
            cron_string = scheduled_item.cron
            now = int(time.time())
            should_run = should_run_fn(last_updated_at, cron_string=cron_string, now=now)
            if should_run:
                result = self._process_record(scheduled_item)
                if result['success']:
                    success_count += 1
                else:
                    failure_count += 1
                receipts.append(result)
            else:
                skip_count += 1
        return {
            "receipts": receipts,
            "success_count": success_count,
            "failure_count": failure_count,
            "skip_count": skip_count
        }


    def _process_record(self, scheduled_item):
        action_id = scheduled_item.action_id
        if action_id == 'noop-success':
            return { "success": True }
        elif action_id == 'noop-failure':
            return { "success": False }
        elif action_id.find('feaas-py.') == 0:
            return self._run_via_api(scheduled_item)
        else:
            return { "success": False }


    def _run_via_api(self, scheduled_item):
        action_id = scheduled_item.action_id
        req = scheduled_item.params
        if action_id.find('feaas-py.') == 0:
            username = scheduled_item.username
            url = f'{self.feaas_host}/{username}/work/{action_id}'
            headers = {
                'FEAAS_TOKEN': self.feaas_token
            }
            r = requests.post(url, json=req, headers=headers)
            receipt = r.json()
            if 'success' not in receipt:
                receipt['success'] = r.status_code == 200
            return receipt
        else:
            return { "success": False }



    # def run_apps(self):
    #     # TODO: update
    #     owner = 'sys.scheduler.install'
    #     resp = self.docstore.table.query(
    #         IndexName="ownerIndex",
    #         KeyConditionExpression=Key('owner').eq(owner)
    #     )
    #     n = len(resp['Items'])
    #     log.info(f'Going to process {n} scheduler instances')
    #     # TODO: handle >1k list size
    #     # TODO: order by last_updated
    #     # TODO: save telemetry in a stream
    #     for item in resp['Items']:
    #         ref_object_id = item['object_id']
    #         scheduler_object_id = ref_object_id[4:]
    #         self.handle_scheduler_instance(scheduler_object_id)


    # def run_psee(self):
    #     owner = "sys.psee"
    #     resp = self.docstore.table.query(
    #         IndexName="ownerIndex",
    #         KeyConditionExpression=Key('owner').eq(owner)
    #     )
    #     n = len(resp['Items'])
    #     print(f'Going to process {n} psee jobs')
    #     for item in resp['Items']:
    #         if item['object_id'].startswith('ref_'):
    #             job_object_id = item['object_id'][4:]
    #         elif 'secondary_key' not in item:
    #             print(f'Missing field secondary_key processing owner={owner}', item)
    #             continue
    #             job_object_id = item['secondary_key']
    #         # TODO: don't run it if it's failed or complete so need to store status somewhere in index record for easy reference
    #         o = self.docstore.get_document(job_object_id)
    #         if o is None:
    #             self.docstore.delete_document(item['object_id'])
    #             continue
    #         failed = False
    #         try:
    #             s = json.dumps(o, cls=common.DecimalEncoder)
    #             job = Parse(s, objs.PortalScriptJob(), ignore_unknown_fields=True)
    #         except:
    #             err = traceback.format_exc()
    #             print("run_psee ERROR on", job_object_id, err)
    #             print(s)
    #             # TODO: update job with error
    #             failed = True
    #         if not(failed):
    #             status = job.status
    #             if status != objs.PortalScriptStatus.Failed:
    #                 try:
    #                     receipt = self.psee.run_script(job)
    #                 except:
    #                     err = traceback.format_exc()
    #                     print("ERROR on", job_object_id, err)
    #                     job.status = objs.PortalScriptStatus.Failed
    #                     job.ttl = int((datetime.now() + timedelta(days=1)).timestamp())

    #                     o = MessageToDict(job, preserving_proto_field_name=True)
    #                     o = common.protoBufIntFix(o)
    #                     self.docstore.save_document(job_object_id, o)




    # def handle_scheduler_instance(self, scheduler_object_id):
    #     log.debug(f'Running scheduler for {scheduler_object_id}')
    #     resp = self.docstore.table.query(
    #         IndexName="ownerIndex",
    #         KeyConditionExpression=Key('owner').eq(scheduler_object_id))
    #     items = resp['Items']
    #     n = len(items)
    #     log.info(f'Found {n} scheduler items for {scheduler_object_id}')
    #     receipts = []
    #     for ref in items:
    #         receipt = self.handle_scheduler_item(ref['object_id'])
    #         receipts.append(receipt)
    #     return receipts


    # def handle_scheduler_item(self, object_id, run_now=False):
    #     item = self.docstore.get_document(object_id)
    #     if item is None:
    #         return objs.Receipt(success=False, error_message=f"Could not find {object_id}")
    #     try:
    #         se = Parse(json.dumps(item, cls=common.DecimalEncoder), objs.ScheduledEvent(), ignore_unknown_fields=True)
    #     except:
    #         print("ERROR")
    #         print("ERROR", item)
    #         print("ERROR")
    #         return objs.Receipt(success=False, error_message=f"Could not parse {object_id}")
    #     if 'run_count' in item:
    #         run_count = item['run_count']
    #     else:
    #         run_count = 0
    #     if 'error_count' in item:
    #         error_count = item['error_count']
    #     else:
    #         error_count = 0
    #     cron_action_params = { '_scheduler_object_id': object_id }
    #     for k in item.keys():
    #         if k not in ['object_id', 'owner', 'cron', 'action_id', 'dest_stream_id', 'username', 'last_updated_at', 'last_output', 'in_map']:
    #             cron_action_params[k] = item[k]
    #     if 'paramValues' in item:
    #         rec = item
    #         if 'paramValues' in rec:
    #             for k in rec['paramValues'].keys():
    #                 at = rec['paramValues'][k]
    #                 if 'byval' in at:
    #                     del at['byval']
    #                 anyType = Parse(json.dumps(at, cls=common.DecimalEncoder), objs.AnyType(), ignore_unknown_fields=True)
    #                 cron_action_params[k] = util.any_type_resolved(anyType)
    #             del cron_action_params['paramValues']

    #         # for k in item.keys():
    #         #     if k not in ['object_id', 'owner', 'cron', 'action_id', 'dest_stream_id', 'username', 'last_updated_at', 'last_output', 'in_map']:
    #         #         cron_action_params[k] = item[k]
    #     now = int(time.time())
    #     n = str(datetime.now())
    #     should_run = self._should_run(se, now)
    #     should_run = True # TODO: remove
    #     if not(should_run) and not(run_now):
    #         return objs.Receipt(success=False)
    #     update = {}
    #     print(f"SHOULD RUN {object_id}, cron = {se.cron}, last = {se.last_updated_at}, {should_run} {run_now}")
    #     start_compute = int(time.time() * 1000)
    #     if len(se.username) > 0:
    #         username = se.username
    #     else:
    #         arr = se.object_id.split('/')
    #         if len(arr) == 2:
    #             i = arr[0].find('.') + 1
    #             username = arr[0][i:]
    #         else:
    #             username = arr[1]
    #     receipt = self._handle_scheduler_item(username, se, cron_action_params, run_now)
    #     update['last_receipt'] = MessageToDict(receipt, preserving_proto_field_name=True)
    #     last_compute_ms = int(time.time() * 1000) - start_compute
    #     update["last_compute_ms"] = last_compute_ms
    #     update["last_updated_at"] = now
    #     err_message = ""
    #     is_error = 0
    #     if not(receipt.success):
    #         err_message = receipt.error_message
    #         log.error(err_message)
    #         if err_message is not None and len(err_message) > 0:
    #             is_error = 1
    #             update['last_err_at'] = now
    #             update["last_err_message"] = err_message
    #     outputs = receipt.outputs
    #     if is_error:
    #         sublabel = f'ERROR: {err_message}'
    #     else:
    #         sublabel = f'Last run at {n}'
    #     update["sublabel"] = sublabel
    #     update["run_count"] = run_count + 1
    #     update["error_count"] = error_count + is_error
    #     update = common.clean_json_dict(update)
    #     self.docstore.update_document(se.object_id, update)
    #     o = MessageToDict(receipt, preserving_proto_field_name=True)
    #     stream_id = object_id.replace('/app-v2.scheduler.', '/sch-receipts.')
    #     self.streams.update_feed(stream_id, now, o)
    #     return receipt


    # def _handle_scheduler_item(self, username: str, item: objs.ScheduledEvent, cron_action_params: dict, run_now=False):
    #     action_id = item.action_id
    #     if item.cron == '':
    #         return objs.Receipt(success=False, error_message='Missing cron expression')
    #     cron = item.cron
    #     if action_id.strip() == '':
    #         return objs.Receipt(success=False, error_message=f'Unknown action on {item.object_id}')
    #     print(f"Running {action_id} for {item.object_id}")
    #     adoc = self.docstore.get_document(action_id)
    #     sys_name = adoc['sys_name']
    #     if sys_name == 'ecs-1':
    #         username = username
    #         # ecs_host = 'http://feaas-py-worker.dataskeptic.com'
    #         ecs_host = 'http://127.0.0.1:6001'
    #         app_name = 'work'
    #         url = f'{ecs_host}/api/{username}/{app_name}/{action_id}'
    #         headers = {}
    #         headers['FEAAS_TOKEN'] = os.environ.get('FEAAS_TOKEN')
    #         print('HHHHH', headers)
    #         r = requests.post(url, json=cron_action_params, headers=headers)
    #         print(url)
    #         print(r.status_code)
    #         print(r.content)
    #         return objs.Receipt(success=False, error_message="No support for ecs-1")
    #     elif sys_name == 'lambda-1':
    #         # TODO: handle other prefixes
    #         feaas_action_id = action_id[len('feaas-py.'):]
    #         try:
    #             print('feaas_action_id', feaas_action_id)
    #             Action = common.build_action_class(feaas_action_id)
    #         except (ImportError, AttributeError) as e:
    #             msg = f'Import error in stream_processor servicing {feaas_action_id}'
    #             print(msg)
    #             return objs.Receipt(success=False, error_message=msg)
    #         try:
    #             action = Action(self.dao)
    #         except:
    #             action = Action()
    #         kwargs = {}
    #         # if 'paramValues' in item:
    #         #     d2 = item.paramValues
    #         #     cron_action_params = { **d2, **cron_action_params }
    #         #     print("w22w", cron_action_params)
    #         for param in action.action.params:
    #             v = param.var_name
    #             if v in cron_action_params:
    #                 kwargs[v] = cron_action_params[v]
    #             elif v == 'username':
    #                 kwargs[v] = username
    #             elif v == 'dest_stream_id':
    #                 kwargs[v] = item.dest_stream_id
    #         # in_map is not currently in use
    #         # for pm in item.in_map:
    #         #     print('pm', pm)
    #         #     dest = pm.dest
    #         #     sdef = pm.sdefault
    #         #     if sdef is not None and sdef != '':
    #         #         kwargs[dest] = sdef
    #         #     else:
    #         #         kwargs[dest] = pm.idefault
    #         user_host = 'https://beta.dataskeptic.com'
    #         try:
    #             receipt = self.action_executor.begin_action_execution(action_id, username, kwargs, run_now=run_now)
    #             self.billing.scheduler_billing(username, receipt, item, user_host)
    #         except:
    #             err = traceback.format_exc()
    #             print(err)
    #             receipt = objs.Receipt(success=False, error_message=err)
    #         return receipt
    #     else:
    #         return objs.Receipt(success=False, error_message=f'Unknown system {sys_name}')

