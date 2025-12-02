import boto3
import feaas.objects as objs
from feaas.psee.psee import PlusScriptExecutionEngine
from feaas.psee.util import load_registers
from feaas.actions.noop import NoopSuccess, NoopFailure
from feaas.actions.string import StringUcase, StringLcase, Markdown, StringReplace
from feaas.util import common
from google.protobuf.json_format import Parse, MessageToDict
from feaas.stream import util
from decimal import Decimal, InvalidOperation
from importlib import import_module
from inspect import signature
import boto3
import hashlib
import json
import markdown2
import logging
import requests
import time
import inspect
import traceback
from typing import List, Tuple
import os
import re
import uuid


def assign_unique_id(sys_name, sys_action_id):
    i = f'{sys_name}:{sys_action_id}'.lower().strip()
    return hashlib.md5(i.encode('utf-8')).hexdigest()


def clean_data_for_action(action, data):
    cleaned = data.copy()

    # Build a lookup for param types
    param_ptype = {param['var_name']: param['ptype'] for param in action['params']}

    for key, value in data.items():
        expected_type = param_ptype.get(key)
        if expected_type == 'BOOLEAN':
            if isinstance(value, str):
                v_lower = value.strip().lower()
                if v_lower.startswith('f'):
                    cleaned[key] = False
                elif v_lower.startswith('t'):
                    cleaned[key] = True
                else:
                    raise ValueError(f"Cannot interpret boolean value: {value}")
            elif isinstance(value, bool):
                cleaned[key] = value
            else:
                raise ValueError(f"Unexpected type for BOOLEAN param '{key}': {type(value)}")

    return cleaned


class ActionExecutor(object):


    def __init__(self, dao, sys_name):
        self.dao = dao
        self.blobstore = dao.get_blobstore()
        self.docstore = dao.get_docstore()
        self.streams = dao.get_streams()
        self.psee = PlusScriptExecutionEngine(dao, self)
        # self.notifier = Notifier(dao.notification_secret)
        self.sys_name = sys_name
        assert sys_name != 'feaas-core'
        self.batch_client = boto3.client('batch', aws_access_key_id=dao.access_key, aws_secret_access_key=dao.secret_key, region_name='us-east-1')
        env = os.getenv("FEAAS_ENV", "prod")
        object_id = f'sys.env.{env}'
        if object_id not in self.docstore.cache:
            print("ActionExecutor Loading token from", object_id)
        cache_for_sec = 60 * 15
        doc = self.docstore.get_document(object_id, cache_for_sec)
        self.feaas_py_host = None
        if doc is not None:
            self.token = doc['TOKEN']
            self.feaas_py_host = doc['FEAAS_PY_HOST']
        if self.feaas_py_host is None:
            print("Missing feaas_host")
            self.feaas_py_host = 'https://4nbaxdraua.execute-api.us-east-1.amazonaws.com/api/'
        # TODO: cache Actions by action_id


    def begin_action_execution(self, action_object_id, username, data) -> objs.Receipt:
        # TODO: quietly fix types from string to somethign else where possible,
        # TODO: enforce validations.

        decision = self._run_env_decision(username, action_object_id)
        run_env = decision['run_env']
        if run_env == 'aws-batch':
            logging.info(f"Run {action_object_id} on AWS Batch")
            return self._run_on_aws_batch(action_object_id, username, data)
        elif run_env == 'run-locally':
            local_class_path = decision['local_class_path']
            logging.info(f"Run {action_object_id} -> {local_class_path} on local")
            return self._run_locally(action_object_id, local_class_path, username, data)
        elif run_env == 'run-api':
            url = decision['url']
            logging.info(f"Run {action_object_id} via api call to {url}")
            headers = decision['headers']
            return self._run_via_api(url, data, headers)
        else:
            return objs.Receipt(success=False, error_message=f'Unknown run_env = {run_env}')


    def _run_via_api(self, url, data, headers):
        r = requests.post(url, json=data, headers=headers)
        if r.status_code == 200:
            o = r.json()
            s = json.dumps(o, cls=common.DecimalEncoder)
            receipt =  Parse(s, objs.Receipt(), ignore_unknown_fields=True)
            return receipt
        else:
            logging.error(r)
            em = r.content
            logging.error(f"worker fail {em}")
            return objs.Receipt(success=False, error_message=em)





    def run_inline_action(self, action_id, username, data) -> objs.Receipt:
        action = None
        if action_id == 'noop-success':
            action = NoopSuccess(self.dao)
        elif action_id == 'noop-failure':
            action = NoopFailure(self.dao)
        elif action_id == 'string-ucase':
            action = StringUcase(self.dao)
        elif action_id == 'string-lcase':
            action = StringLcase(self.dao)
        elif action_id == 'string-markdown':
            action = Markdown(self.dao)
        elif action_id == 'string-replace':
            action = StringReplace(self.dao)
        # elif action_id == 'src.feaas.scheduler.run_scheduler.RunScheduler':
        #     Action = common.build_action_class(action_id)
        #     action = Action(self.dao)
        #     receipt = self._run_feaas_py_worker_action(action, username, data)
        #     return receipt
        if action is None:
            return objs.Receipt(success=False, error_message=f"Unknown action {action_id}")
        receipt = action.execute_action(**data)
        return receipt


    def _run_env_decision(self, username, object_id):
        current_sys_name = self.sys_name

        # unique_id = assign_unique_id(sys_name, sys_action_id)
        # owner = 'sys.actions'
        # object_id = f'{owner}.{unique_id}'
        action = self.docstore.get_document(object_id)
        if action is None:
            try:
                Action = common.build_action_class(object_id)
                return { 'run_env': 'run-locally', 'local_class_path': object_id }
            except:
                raise Exception(f"Cannot find action {object_id}")

        target_sys_name = action['sys_name']

        if 'run_mode' in action and action['run_mode'] == 1:
            return { 'run_env' : 'aws-batch' }
        elif current_sys_name == target_sys_name:
            logging.info(f"sys match: {current_sys_name}")
            sys_action_id = action['sys_action_id']
            local_class_path = sys_action_id
            return { 'run_env': 'run-locally', 'local_class_path': local_class_path }
        elif target_sys_name == 'feaas-core':
            logging.info("core run")
            local_class_path = action['sys_action_id'][4:]
            return { 'run_env': 'run-locally', 'local_class_path': local_class_path }
        else:
            api_object_id = f'sys.services-providers.{target_sys_name}'
            doc = self.docstore.get_document(api_object_id)
            if doc is None:
                logging.error(f"No record in {api_object_id}.  Should have url in the record.")
            host = doc['url']
            url = f'{host}/api/{username}/work/{object_id}'
            headers = { 'TOKEN': self.token }
            return { 'run_env': 'run-api', 'url': url, 'headers': headers }


    def _run_now(self, action, actionObj, data, username) -> objs.Receipt:
        data = self._enrich_inputs(data, action, username)
        try:
            receipt = actionObj.execute_action(**data)
            return receipt
        except:
            em = traceback.format_exc()
            logging.error(em)
            # m = f'Could not find {action_id}:{adjusted_action_id}'
            return objs.Receipt(success=False, error_message=em)


    def _enrich_inputs(self, data, action, username):
        if action is None or 'params' not in action:
            return data
        for param in action['params']:
            pt = param['ptype']
            if pt == "USERNAME" or pt == objs.ParameterType.USERNAME:
                vn = param['var_name']
                data[vn] = username
        # if job_id is not None:
        #     for param in action.params:
        #         if param.ptype == objs.ParameterType.JOB_ID:
        #             data[param.var_name] = job_id
        return data


    def _run_locally(self, action_object_id, local_class_path, username, data):
        action = self.docstore.get_document(action_object_id)
        try:
            Action = common.build_action_class(local_class_path)
            actionObj = Action(self.dao)
        except:
            em = traceback.format_exc()
            logging.error(em)
            m = f'Could not find local path {local_class_path}'
            return objs.Receipt(success=False, error_message=m)
        receipt = self._run_now(action, actionObj, data, username)

        return receipt


    def _run_on_aws_batch(self, action_id, username, action_inputs):
        job_name = action_id.split(".")[-1] + f" ___ {username}"
        job_name = re.sub(r'[^a-zA-Z0-9_-]', '_', job_name)

        job_queue_arn = "arn:aws:batch:us-east-1:085318171245:job-queue/batch-job-queue"
        access_key = self.dao.access_key
        secret_key = self.dao.secret_key
        bucket_name = self.blobstore.bucket_name
        region = "us-east-1"
        docstore_table = self.docstore.table_name
        streams_table = self.streams.table_name

        # Dynamically get the latest job definition ARN
        job_definition_name = "batch-job-definition"  # Update this to your job definition name
        job_definition_arn = self._get_latest_job_definition(job_definition_name)

        job_id = str(uuid.uuid4())
        hostname = "plus_dataskeptic_com"
        job_owner = f'{hostname}/{username}/job'
        job_object_id = f'{job_owner}.{job_id}'

        action_inputs['job_id'] = job_id

        response = self.batch_client.submit_job(
            jobName=job_name,
            jobQueue=job_queue_arn,
            jobDefinition=job_definition_arn,
            containerOverrides={
                "environment": [
                    {"name": 'ACCESS_KEY', "value": access_key},
                    {"name": 'SECRET_KEY', "value": secret_key},
                    {"name": "JOB_ID", "value": job_id},
                    {"name": "REGION", "value": region},
                    {"name": 'DYNAMO_TABLE', "value": docstore_table},
                    {"name": 'DYNAMO_STREAMS_TABLE', "value": streams_table},
                    {"name": 'PRIMARY_BUCKET', "value": bucket_name},
                    {"name": "RUN_MODE", "value": "RUN_ACTION"},
                    {"name": "USERNAME", "value": username},
                    {"name": "ACTION_ID", "value": action_id},
                    {"name": "ACTION_INPUT_JSON", "value": json.dumps(action_inputs)},
                ]
            }
        )
        aws_job_id = response['jobId']
        outputs = {
            "job_id": objs.AnyType(ptype=objs.ParameterType.JOB_ID, sval=job_id)
        }
        job_doc = {
            "owner": job_owner,
            'aws_job_id': aws_job_id,
            'started_at': int(time.time())
        }
        self.docstore.save_document(job_object_id, job_doc)
        receipt = objs.Receipt(success=True, outputs=outputs, primary_output='job_id')
        return receipt


    def _get_latest_job_definition(self, job_definition_name):
        # Use AWS Batch client to retrieve the job definitions
        response = self.batch_client.describe_job_definitions(
            jobDefinitionName=job_definition_name,
            status='ACTIVE'
        )
        
        # Sort the job definitions by revision and get the latest
        job_definitions = response.get('jobDefinitions', [])
        if not job_definitions:
            raise ValueError(f"No active job definitions found for {job_definition_name}")
        
        latest_job_definition = max(job_definitions, key=lambda x: x['revision'])
        return latest_job_definition['jobDefinitionArn']


    def batch_file_execution(self, action_id, username, src_prefix, match_pattern, data, stream_id=None) -> objs.Receipt:
        doc = self.docstore.get_document(action_id)
        if doc is None:
            return objs.Receipt(success=False, error_message=f'Could not find {action_id}')
        action = Parse(json.dumps(doc), objs.Action(), ignore_unknown_fields=True)
        var_name = None
        for param in action.params:
            if param.ptype == objs.ParameterType.KEY:
                var_name = param.var_name
        if var_name is None:
            return objs.Receipt(success=False, error_message=f'No KEY param in {action_id}')
        for item in self.blobstore.s3list(src_prefix, recursive=True, list_dirs=False, list_objs=True, limit=None, match_pattern=match_pattern):
            data = { var_name: item.key }
            r = self.begin_action_execution(action_id, username, data, stream_id=stream_id)
            if stream_id is not None:
                d = MessageToDict(r, preserving_proto_field_name=True)
                self.streams.update_feed(stream_id, int(time.time() * 1000), d)


    def _execute_feaas_py_now(self, sys_action_id, username, data, run_now=False):
        if data is None:
            return objs.Receipt(success=False, error_message="No data provided")
        try:
            Action = common.build_action_class(sys_action_id)
        except:
            em = traceback.format_exc()
            logging.error(em)
            m = f'Could not find sys_action_id {sys_action_id}'
            return objs.Receipt(success=False, error_message=m)
        return self.execute_feaas_py(sys_action_id, username, data, run_now)


    # def begin_aligned_stream_alerts(self, action_ids, stream_id, dest_stream_id, reject_p_value_below):
    #     """
    #     https://github.com/kylepolich/uda/issues/68
    #     """
    #     self._are_uda_alert_action_ids(action_ids)
    #     kwargs = {
    #         'stream_id': stream_id,
    #         'dest_stream_id': dest_stream_id,
    #         'reject_p_value_below': reject_p_value_below
    #     }
    #     receipts = []
    #     for action_id in action_ids:
    #         logging.info(f'begin_aligned_stream_alerts {action_id}')
    #         action = self.action_id_to_action_instance(action_id)
    #         action_args = action.handle_stream_alert_args(**kwargs)
    #         receipt = action.execute_action(**action_args)
    #         item = None
    #         is_writeable = receipt.success and receipt.outputs['reject_null'].bval
    #         if not is_writeable:
    #             receipts.append(receipt)
    #             continue
    #         now_ts = int(time.time() * 1000)
    #         items = self._construct_items_from_receipts([receipt])
    #         if len(items) == 1:
    #             item = items[0]   # assume at most one item
    #             item['stream_id'] = dest_stream_id
    #             item['timestamp'] = now_ts
    #             item['action_id'] = action.action.sys_action_id
    #             self.dao.streams.save_document(item['stream_id'], item)
    #         receipts.append(receipt)
    #     return receipts


    # def _are_uda_alert_action_ids(self, action_ids):
    #     uda_alerts = [
    #         'feaas-py.chalicelib.actions.client.uda.ks_2samp.KS2Sample',
    #         'feaas-py.chalicelib.actions.client.uda.z_score_outliers.ZScoreOutlierDetector'
    #     ]
    #     for action_id in action_ids:
    #         if action_id not in uda_alerts:
    #             raise ValueError(f'{action_id} not a valid uda alert action id')
    #     return None


    def begin_batch_execution(self, action_id_and_data_tuples: List[Tuple], stream_id, 
        start_timestamp, end_timestamp, dest_stream_id=None) -> objs.Receipt:
        """
        https://github.com/kylepolich/uda/issues/16
        """
        if not isinstance(action_id_and_data_tuples, list):
            raise TypeError(f'expected {type([])} for action_ids but got {type(action_id_and_data_tuples)}')
        action_and_data_tuples = self._get_action_and_data_tuples(action_id_and_data_tuples)
        current_start_timestamp = start_timestamp
        successful_receipts = 0
        failed_receipts = 0
        while current_start_timestamp < end_timestamp:
            combined_receipts = []
            stream_records = self.dao.streams.read_stream_between(stream_id, current_start_timestamp, end_timestamp, limit=100)
            if len(stream_records) == 0:
                break  # no more records to read
            self._set_db_read_records(action_and_data_tuples, stream_records)
            if dest_stream_id is None:
                dest_stream_id = f'{stream_id}.features'
            for record in stream_records:
                action_and_data_tuples = self._update_action_data_dict(action_and_data_tuples, record, stream_id)
                receipts = self.execute_actions(action_and_data_tuples, record)
                if receipts == []:
                    continue
                combined_receipt = util.combine_receipts(record['timestamp'], receipts)
                combined_receipts.append((record['timestamp'], combined_receipt))
            items = self._construct_items_from_receipts(combined_receipts)
            self.dao.streams.batch_write_stream(items, dest_stream_id)
            current_start_timestamp = stream_records[-1]['timestamp'] + 1
            # also call here so we can gracefully resume
            self.finalize_actions(action_and_data_tuples)
            successful_receipts += len(items)
            failed_receipts += len(combined_receipts) - len(items)
        self.finalize_actions(action_and_data_tuples)
        outputs = { 
            'successful_receipts' : objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=successful_receipts),
            'failed_receipts' : objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=failed_receipts)
        }
        return objs.Receipt(success=True, outputs=outputs)


    def _get_action_and_data_tuples(self, action_id_and_data_tuples):
        action_and_data_tuples = []
        for action_id, data in action_id_and_data_tuples:
            action = self.action_id_to_action_instance(action_id)
            if not hasattr(action, 'batch_mode'):
                raise AttributeError(f"Selected Action {action_id} does not support batching")
            action.batch_mode = True
            action_and_data_tuples.append((action, data))
        return action_and_data_tuples


    def _set_db_read_records(self, action_and_data_tuples, stream_records):
        """
        initialize db_read_records with `window-1` number of records
        so that we don't read from db again
        also note: the first `window-1` stream data points will not have stats
        """
        updated_action_and_data_tuples = []
        for action, data in action_and_data_tuples:
            window = data['window']
            if action.is_first_run:
                action.is_first_run = False
                initial_records = stream_records[:window-1]
                action.db_read_records = initial_records
            ad_tuple = (action, data)
            updated_action_and_data_tuples.append(ad_tuple)
        return updated_action_and_data_tuples


    def execute_actions(self, action_and_data_tuples, record):
        receipts = []
        for action, data in action_and_data_tuples:
            if record in action.db_read_records:
                continue # skip if already part of initial records
            action_kwargs = self._get_action_kwargs(action, data)
            receipt = action.execute_action(**action_kwargs)
            receipts.append(receipt)
        return receipts


    def _get_action_kwargs(self, action, data):
        sig = signature(action.execute_action)
        action_kwargs = {}
        for p in sig.parameters.values():
            if p.kind == p.POSITIONAL_OR_KEYWORD:
                action_kwargs[p.name] = data.get(p.name)
        return action_kwargs


    def _update_action_data_dict(self, action_and_data_tuples, record, stream_id):
        updated_tuples = []
        for action, data in action_and_data_tuples:
            fn = data['field_name']
            if fn in record:
                data['timestamp'] = record['timestamp']
                data['new_value'] = record[fn]
                data['stream_id'] = stream_id
                data['new_record'] = record
                updated_tuples.append((action, data))
        return updated_tuples


    def _construct_items_from_receipts(self, timestamp_receipt_tuples: list):
        items = []
        for timestamp, receipt in timestamp_receipt_tuples:
            if not receipt.success:
                continue
            rdict = MessageToDict(receipt)
            outputs = rdict['outputs']
            item = {}
            for k,v in outputs.items():
                if v.get('sval'):
                    item[k] = str(v['sval'])
                elif v['ptype'] == 'INTEGER':
                    item[k] = int(v['ival'])
                elif v['ptype'] == 'FLOAT':
                    item[k] = Decimal(str(v['dval']))
                elif v['ptype'] == 'BOOLEAN':
                    item[k] = v['bval']
                elif v['ptype'] == 'DATETIME':
                    try:
                        item[k] = v['dval']
                    except KeyError:
                        item[k] = v['ival']
                elif v['ptype'] == 'STRING_MAP':
                    d = v['smap']
                    k = str(list(d.keys())[0])
                    try:
                        v = Decimal(str(list(d.values())[0]))
                    except InvalidOperation:
                        v = str(list(d.values())[0])
                    item[k] = v
                elif v['ptype'] == 'VECTOR':
                    if 'ivals' in v:
                        item[k] = [int(x) for x in v['ivals']]
                    elif 'dvals' in v:
                        item[k] = [Decimal(str(x)) for x in v['dvals']]
                    else:
                        raise NotImplementedError(f'{(k, v)}')
                elif v['ptype'] == 'LIST':
                    if 'ivals' in v:
                        item[k] = [int(x) for x in v['ivals']]
                    elif 'dvals' in v:
                        item[k] = [Decimal(str(x)) for x in v['dvals']]
                    elif 'svals' in v:
                        item[k] = [str(x) for x in v['svals']]
                    else:
                        # no *vals key... assume empty list
                        item[k] = []
                else:
                    raise NotImplementedError(f'{(k, v)}')
            items.append(item)
        return items


    def finalize_actions(self, action_and_data_tuples):
        for action, data in action_and_data_tuples:
            action.finalize()


    def action_id_to_action_instance(self, action_id):
        try:
            Action = common.build_action_class(action_id)
        except ModuleNotFoundError:
            em = traceback.format_exc()
            msg = f'Could not find action_id="{action_id}".  Check docstore.\n\n{em}'
            logging.error(msg)
            return msg
        except (ImportError, AttributeError) as e:
            em = traceback.format_exc()
            msg = f'Import error in route_post servicing {action_id}: {em}'
            return msg
        #If not all params AND key, then check key for params
        try:
            action = Action(self.dao)
            action.action.sys_name = self.sys_name
        except:
            msg = traceback.format_exc()
            print("ERROR:", action_id)
            print(msg)
            action = Action()
        return action


    def _execute_plus_script(self, doc, username, data) -> objs.Receipt:
        # TODO: call psee
        return objs.Receipt(success=False, error_message="PSEE not implemented")


    def execute_feaas_py(self, action_id, username, kwargs, run_now=False) -> objs.Receipt:
        outer_action = self.action_id_to_action_instance(action_id)
        action = outer_action.action
        if type(action) == str:
            return objs.Receipt(success=False, error_message=action)
        return self._execute_feaas_py_inner(action, username, kwargs, run_now)


    def execute_feaas_py2(self, action, username, data, run_on_current_server) -> objs.Receipt:
        return self._execute_feaas_py_inner(action, username, data, run_on_current_server)


    def _execute_feaas_py_inner(self, action, username, kwargs, run_now) -> objs.Receipt:
        kwargs = self._get_kwargs_from_s3_metadata(action.params, kwargs)
        kwargs = self._fill_with_defaults(action.params, kwargs)
        if '_scheduler_object_id' in kwargs:
            scheduler_object_id = kwargs['_scheduler_object_id']
        else:
            scheduler_object_id = None
        if run_now or action.sys_name == self.sys_name:
            return self._execute_now(username, action, kwargs)
        else:
            msg = "Something wrong in _execute_feaas_py_inner"
            logging.error(msg)
            return objs.Receipt(success=False, error_message=msg)
        if receipt.success:
            try:
                receipt.cost = action.cost
            except:
                em = traceback.format_exc()
                logging.error(em)
                logging.error(type(action))
                logging.error(action)
                receipt.cost = 0
        elif receipt.error_message is not None and receipt.error_message.strip() != '':
            logging.error("was not success")
        return receipt


    def _execute_now(self, username, action, data):
        tt = type(action)
        if tt != objs.Action:
            logging.error(f"Bad polymorphism happened, got {tt}")
        x = action.sys_action_id
        logging.debug(f"_execute_now for action: {x}")
        kwargs = {}
        third_party_aws_bucket = False
        using_s3_bucket_selector = False
        arr = list(data.keys())
        datastr = ','.join(arr)
        logging.debug(f"_execute_now with data: {datastr}")
        for param in action.params:
            if '{username}/aws.buckets' in param.src_owners:
                bucket_name = data[param.var_name]
                using_s3_bucket_selector = bucket_name != self.blobstore.bucket_name
        for param in action.params:
            var_name = param.var_name
            ptype = param.ptype
            conditionals = param.conditionals
            if conditionals is not None and len(conditionals) > 0:
                conditional_match_all = param.conditional_match_all
                # TODO: if conditional is not met, set default values for this param
            if ptype == objs.ParameterType.INTEGER and var_name in data:
                try:
                    kwargs[var_name] = int(data[var_name])
                except:
                    return objs.Receipt(success=False, error_message=f'Value {data[var_name]} is not a valid integer')
            elif ptype == objs.ParameterType.FLOAT and var_name in data:
                try:
                    kwargs[var_name] = float(data[var_name])
                except:
                    return objs.Receipt(success=False, error_message=f'Value {data[var_name]} is not a valid float')
            elif ptype == objs.ParameterType.BOOLEAN:
                if var_name in data:
                    b = str(data[var_name]).strip().lower()
                    if b == 'true':
                        val = True
                    elif b == 'false' or b == '':
                        val = False
                    else:
                        return objs.Receipt(success=False, error_message=f'Value {data[var_name]} is not a valid boolean')
                    kwargs[var_name] = val
                elif not(param.optional):
                    kwargs[var_name] = param.bdefault
            elif ptype == objs.ParameterType.USERNAME or var_name == 'username':
                kwargs[var_name] = username
            elif ptype == objs.ParameterType.DISPLAY_ONLY:
                pass
            elif ptype == objs.ParameterType.PREFIX:
                if var_name in data:
                    val = data[var_name]
                else:
                    print("MISSING", var_name, data)
                if val is None or val.strip() == '':
                    msg = f'Got missing {var_name} from data of size ({len(data)}) running action {action.object_id} action_id={action.sys_action_id}'
                    return objs.Receipt(success=False, error_message=msg)
                if str(val).strip() == '' and param.optional:
                    continue
                if val is None:
                    return objs.Receipt(success=False, error_message=f'No value for {var_name} in data for action_id={action.sys_action_id}')
                arr = val.split('/')
                # TODO: check hostname
                hostname = arr[0]
                if hostname != 'app_neonpixel_co':
                    if len(arr) < 2:
                        return objs.Receipt(success=False, error_message=f'Location needs to be in the user area of {username} but got {val}.')
                    xusername = arr[1]
                    if not(username.lower() == xusername.lower()) and username != 'ddd@ddd.ddd':
                        return objs.Receipt(success=False, error_message=f'Locationz needs to be in the user area of {username} but got {val} which has not matching {xusername} account')
                    #val = f'{hostname}/{username}/'
                kwargs[var_name] = val
            elif ptype == (objs.ParameterType.INTEGER or objs.ParameterType.DATETIME) and var_name == 'timestamp':
                if var_name in data:
                    kwargs[var_name] = data[var_name]
                else:
                    kwargs[var_name] = int(time.time() * 1000)
            elif ptype == objs.ParameterType.STREAM_RECORD:
                kwargs[var_name] = data
            # elif ptype == objs.ParameterType.SESSION:
            #     kwargs[var_name] = data['session']
            elif ptype == objs.ParameterType.STRING_MAP:
                d = data[var_name]
                if type(d) == str:
                    d = json.loads(d)
                kwargs[var_name] = d
            else:
                val = data[var_name]
                if val is None:
                    if not(param.optional):
                        #? router_app_object_id
                        return objs.Receipt(success=False, error_message=f'Cannot execute {action.sys_action_id} due to missing value for {var_name}')
                    val = util.get_default_value_from_param(param)
                kwargs[var_name] = val
            # TODO: handle defaults
            # if var_name not in kwargs:
            #     at = util.get_param_default_as_any_type(param)
            #     v = util.any_type_resolved(at)
            #     v2 = util.resolve_default_variables(v, username)
            #     if v2 is not None:
            #         kwargs[var_name] = v2
        try:
            logging.info(f'=[ Begin: {action.sys_action_id} ]=')
            # if ru_trigger and force: ?
            # actionObj = self.action_id_to_action_instance(action.sys_action_id)
            if type(action) == objs.Action:
                action_instance = self.action_id_to_action_instance(action.sys_action_id)
            else:
                print("TODO: make sure this is deprecated")
                action_instance = action
            r = action_instance.execute_action(**kwargs)
        except:
            logging.error(f'--ERROR START {action.sys_action_id}---------------------------')
            s = json.dumps(kwargs)
            logging.error(f'kwargs: {s}')
            msg = traceback.format_exc()
            logging.error(msg)
            logging.error(f'-- ERROR END {action.sys_action_id} ---------------------------')
            r = objs.Receipt(success=False, error_message=msg)
        logging.debug(f'=[ Complete Execute Action: {action.sys_action_id} ]=')
        return r

    
    def _run_feaas_py_worker_action(self, aaction, username, kwargs): #, scheduler_object_id=None, run_now=False):
        action = aaction
        kwargs2 = dict(kwargs)
        data = {}

        try:
            params = action.params
            action_id = action.sys_action_id
        except:
            action = action.action
            params = action.params
            action_id = action.sys_action_id

        for param in params:
            var_name = param.var_name
            ptype = param.ptype
            if ptype != objs.ParameterType.DISPLAY_ONLY:
                if var_name in kwargs2:
                    v2 = kwargs2[var_name]
                    data[var_name] = v2
                if var_name not in data:
                    at = util.get_param_default_as_any_type(param)
                    if at is None and not(param.optional):
                        return objs.Receipt(success=False, error_message=f'Required parameter {param.var_name} not provided.')
                    elif at is None and param.optional:
                        data[var_name] = None
                        kwargs2[var_name] = None
                    elif at is not None and var_name != '_':
                        v = util.any_type_resolved(at)
                        v2 = util.resolve_default_variables(v, username)
                        data[var_name] = v2
                        kwargs2[var_name] = v2

        return self._execute_now(username, action, data)
        # if run_now:
        #     ?
        # else:
        #     print("")
        #     areceipt = action.get_async_receipt(**data)
        # for var_name in areceipt.outputs.keys():
        #     at = areceipt.outputs[var_name]
        #     data[var_name] = util.any_type_resolved(at)
        # data['action_id'] = action.action.sys_action_id
        # data['username'] = username
        # if scheduler_object_id is not None:
        #     data['_scheduler_object_id'] = scheduler_object_id
        # self.queue.add_message(json.dumps(data))
        # # if areceipt.error_message.strip() == '':
        # #     # TODO: wait for https://github.com/kylepolich/portal/issues/3336 to be completed and remove this
        # #     areceipt.success = True
        # return areceipt


    def _get_kwargs_from_s3_metadata(self, params, kwargs):
        missing = set()
        keys = []
        for param in params:
            var_name = param.var_name
            if var_name not in kwargs:
                missing.add(var_name)
            if param.ptype == objs.ParameterType.KEY and var_name in kwargs:
                keys.append(kwargs[var_name])
        for key in keys:
            metas = self.blobstore.get_blob_metadata(key)
            if metas is not None:
                resolved = []
                for elem in missing:
                    if elem in metas:
                        kwargs[elem] = metas[elem]
                        resolved.append(elem)
                for elem in resolved:
                    missing.remove(elem)
        # unexpected?
        return kwargs


    def _fill_with_defaults(self, params, kwargs):
        for param in params:
            var_name = param.var_name
            if var_name not in kwargs:
                val = util.get_default_value_from_param(param)
                kwargs[var_name] = val
        return kwargs


    def execute_psee_job(self, username, pscript, data):
        job = self.psee.start_script(username, pscript, data)
        i = job.object_id.rfind('/job.')
        job_id = job.object_id[i+5:]
        outputs = {
            "job_id": objs.AnyType(ptype=objs.ParameterType.JOB_ID, sval=job_id)
        }
        return objs.Receipt(success=True, outputs=outputs, primary_output='job_id')


    def execute_object_constructor_document(self, username, oc, object_id, owner, data):
        if type(oc) == str:
            doc = self.docstore.get_document(oc)
            if doc is None:
                return objs.Receipt(success=False, error_message=f'Could not find Constructor in object_id={oc}')
            try:
                oc = Parse(json.dumps(doc, cls=common.DecimalEncoder), objs.ObjectConstructor(), ignore_unknown_fields=True)
            except:
                logging.error(traceback.format_exc())
                return objs.Receipt(success=False, error_message=f'Value in object_id={oc} could not be decoded to an ObjectConstructor')
        if object_id is None:
            return objs.Receipt(success=False, error_message='Cannot save.  No object_id provided.')
        all_params = util.get_params_from_all_pages(oc)
        o = {}
        for k in data.keys():
            value = data[k]
            if type(value) == objs.AnyType:
                o[k] = util.any_type_resolved(value)
            else:
                o[k] = value
        for param in all_params:
            if param.var_name not in data:
                if param.sdefault != '':
                    o[param.var_name] = param.sdefault.replace('{username}', username)
                elif param.optional:
                    # TODO: use default if exists
                    val = util.get_default_value_from_param(param)
                    if val is not None:
                        o[param.var_name] = val
                else:
                    msg = f'Missing {param.var_name}'
                    return objs.Receipt(success=False, error_message=msg)
        o['object_id'] = object_id
        o['owner'] = owner
        o['constructor_id'] = oc.object_id
        self.docstore.save_document(object_id, o)
        outputs = {
            'object_id': objs.AnyType(ptype=objs.ParameterType.OBJECT_ID, sval=object_id)
        }
        return objs.Receipt(success=True, outputs=outputs, primary_output='object_id')


    def execute_object_constructor_stream(self, oc, stream_id, timestamp, data):
        data2 = {}
        for k in data.keys():
            v = data[k]
            try:
                v = float(v)
                ptype = objs.ParameterType.FLOAT
            except:
                try:
                    v = int(v)
                    ptype = objs.ParameterType.INTEGER
                except:
                    ptype = objs.ParameterType.STRING
            data2[k] = util.make_any_type(ptype, v)
        event = objs.StreamEvent(
            stream_id=stream_id,
            timestamp=timestamp,
            etype=objs.StreamEvent.EventType.GENERIC,
            data=data2)
        r = self.dao.sns.publish(TopicArn=self.dao.topic_arn,
                    Message=json.dumps(MessageToDict(event, preserving_proto_field_name=True)),
                    Subject=str(event.etype),
                    MessageAttributes={
                        'Author': {
                            'StringValue': str(event.etype),
                            'DataType': 'Number'
                        }
                    })
        return objs.Receipt(
            success=True,
            error_message="",
            primary_output="MessageId",
            outputs={"MessageId": objs.AnyType(ptype=objs.ParameterType.STRING, sval=r['MessageId'])})


    # def handle_job(psee, docstore, username, job_id):
    #     running = True
    #     object_id = f'{username}/job.{job_id}'
    #     job_obj = docstore.get_document(object_id)
    #     job = Parse(json.dumps(job_obj), objs.PortalScriptJob())
    #     while running:
    #         receipt = psee.run_script(job)
    #         job_obj = docstore.get_document(object_id)
    #         job = Parse(json.dumps(job_obj), objs.PortalScriptJob())
    #         print_job(psee, job)
    #         if job.status == objs.PortalScriptStatus.Failed:
    #             running = False
    #         elif job.status == objs.PortalScriptStatus.Succeeded:
    #             running = False
    #         else:
    #             time.sleep(1)
