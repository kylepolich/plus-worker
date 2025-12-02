from boto3.dynamodb.conditions import Key
import feaas.objects as objs
from feaas.dao.search import util as search
from feaas.psee.psee import PlusScriptExecutionEngine
from feaas.dao.search.util import ElasticSearchUtil
from feaas.stream import util
from feaas.stream_processor import StreamProcessor
from feaas.sys.billing.billing import Billing
from feaas.sys.executor import ActionExecutor
from feaas.util import common
from google.protobuf.json_format import MessageToDict, Parse
from importlib import import_module
import hashlib
import json
import logging
import os
import re
import time
import traceback


log = logging.getLogger(__name__)
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))


class FileProcessor(object):


    def __init__(self, dao, portal_secret, sys_name):
        self.dao = dao
        self.action_executor = ActionExecutor(dao, sys_name)
        self.blobstore = dao.get_blobstore()
        self.docstore = dao.get_docstore()
        self.streams = dao.get_streams()
        stream_processor = StreamProcessor(dao, portal_secret, sys_name)
        self.billing = Billing(dao, portal_secret, stream_processor=stream_processor)
        # self.search = ElasticSearchUtil(dao)


    def on_create(self, event):
        # TODO: should write to a stream_id with ttl with initial status
        key = event.key
        arr = key.split("/")
        if len(arr) < 3:
            return
        hostname = arr[0]
        username = arr[1]
        if len(arr) < 2:
            return
        prefix = common.get_prefix(key)
        bucket_name = self.blobstore.bucket_name
        master_receipt = self.trigger_s3(bucket_name, username, key)
        # TODO: should update with Receipt at the end
        if hostname == 'plus_dataskeptic_com':
            self.trigger_es(key, arr)
            # TODO: more here, why have this?
            # stream_id = f'sys.stream.recent_file.{hostname}.{username}'
            # timestamp = int(time.time() * 1000)
            # contents = {
            #     "key": key,
            #     'last_receipt': MessageToDict(master_receipt, preserving_proto_field_name=True)
            # }
            # return self.streams.update_feed(stream_id, timestamp, contents)


    def trigger_s3(self, bucket_name, username, key):
        return self._run_triggers(bucket_name, username, key)


    def trigger_es(self, key, arr):
        if False and self.search.is_indexable(key):
            hostname = arr[0]
            username = arr[1]
            # plus_dataskeptic_com/sys/arxiv/
            if username == 'sys':
                idx = "/".join(arr[0:2]) + '/'
            else:
                idx = f'{hostname}/{username}/'
            # TODO: review this
            # TODO: consider long files, eg PDFs
            text = self.search.get_text(key)
            doc = self.docstore.get_document(key)
            m = self.es.save_document(key, text, doc=doc)


    def on_delete(self, event):
        key = event.key
        print('on_delete', key)

        arr = key.split("/")
        hostname = arr[0]
        print("Deleting", hostname, key)

        hash_object = hashlib.md5(key.encode())
        hash_hex = hash_object.hexdigest()

        ext = key.split('.').pop()
        public_key = f"{hostname}/public/{hash_hex}.{ext}"
        if self.blobstore.exists(public_key):
            print("Removing public version")
            self.blobstore.delete_blob(public_key)


    def _run_triggers(self, bucket_name, username, key) -> objs.Receipt:
        src_prefix = common.get_prefix(key)
        executed = set()
        master_receipt = self._run_triggers_for_prefix(username, bucket_name, src_prefix, key, False, executed)
        arr = key.split('/')
        arr = arr[0:-2]
        while len(arr) >= 2:
            src_prefix2 = '/'.join(arr)
            receipt2 = self._run_triggers_for_prefix(username, bucket_name, src_prefix2, key, True, executed)
            master_receipt = util.combine_receipts(receipt2.timestamp, [master_receipt, receipt2])
            arr = arr[:-1]
        return master_receipt


    def _run_triggers_for_prefix(self, username, bucket_name, src_prefix, key, is_recursive, executed) -> objs.Receipt:
        i = key.rfind('/')
        owner = src_prefix
        if owner[-1] != "/":
            owner += "/"
        resp = self.docstore.table.query(
            IndexName="ownerIndex",
            KeyConditionExpression=Key('owner').eq(owner))
        master_receipt = None
        n = len(resp['Items'])
        if n > 0:
            print(f"Considering {n} triggers from {owner}")
        for item in resp['Items']:
            object_id = item['object_id']
            if object_id in executed:
                continue
            trigger_json = self.docstore.get_document(object_id)
            trigger_json = common.clean_json_dict(trigger_json)
            # workaround for fuckup in plus
            if 'param_values' in trigger_json:
                for k in trigger_json['param_values'].keys():
                    pv = trigger_json['param_values'][k]
                    if 'byval' in pv:
                        del trigger_json['param_values'][k]['byval']
                    if 'byval' in pv and pv['byval'] == {}:
                        trigger_json['param_values'][k]['byval'] = None
            if 'paramValues' in trigger_json:
                for k in trigger_json['paramValues'].keys():
                    pv = trigger_json['paramValues'][k]
                    if 'byval' in pv and pv['byval'] == {}:
                        trigger_json['paramValues'][k]['byval'] = None

            s = json.dumps(trigger_json, cls=common.DecimalEncoder)
            try:
                trigger = Parse(s, objs.BlobTrigger(), ignore_unknown_fields=True)
            except:
                print(traceback.format_exc())
                msg = f'**** Failure to decode trigger {object_id}'
                logging.error(msg)
                continue
            if not(is_recursive) or trigger.recursive:
                executed.add(object_id)
                try:
                    receipt2 = self._consider_trigger(username, key, trigger)
                except:
                    err = traceback.format_exc()
                    print(err)
                    receipt2 = None
                if receipt2 is not None:
                    if master_receipt is None:
                        master_receipt = receipt2
                    else:
                        master_receipt = util.combine_receipts(receipt2.timestamp, [master_receipt, receipt2])
        if master_receipt is None:
            master_receipt = objs.Receipt(success=True)
        return master_receipt


    def _consider_trigger(self, username, key, trigger) -> objs.Receipt:
        kwargs = {}
        populated_key = False
        fn = common.get_filename_from_key(key)
        match_patterns = []
        try:
            match_patterns = trigger.match_patterns
        except:
            try:
                match_patterns = [trigger.match_pattern]
            except:
                match_patterns = []
        if len(match_patterns) == 0:
            match = True
        else:
            match = False
        for mp in match_patterns:
            s = mp.strip()
            if s != '':
                # mp = trigger.match_pattern
                # result = re.match(trigger.match_pattern, fn)
                s = s.replace(".", "\\.").replace("*", ".*") + "$"
                result = re.match(s, fn)
                match = result is not None
            else:
                mp = trigger.match_patterns
                match_patterns = trigger.match_patterns
                match = True
                if len(match_patterns) > 0:
                    matches_key = False
                    prefix = common.get_prefix(key)
                    siblings = self.blobstore.ls(prefix, '')
                    used = set()
                    for k in match_patterns.keys():
                        if match:
                            match = False
                            match_pattern = match_patterns[k]
                            if re.match(match_pattern, fn) and key not in used:
                                matches_key = True
                                match = True
                                kwargs[k] = key
                                used.add(key)
                            else:
                                for sibling in siblings:
                                    if re.match(match_pattern, sibling) and sibling not in used:
                                        match = True
                                        kwargs[k] = sibling
                                        used.add(sibling)
                    if not(matches_key):
                        match = False
        if not(match):
            logging.debug(f'Considering running {trigger.object_id} but {key} did not match patterns')
            return None
        action_id = trigger.action_id
        print(f'Running {action_id} on {key} due to {trigger.object_id}')
        timestamp = int(time.time())
        n = 90 # TODO: free=7, paid=365, paymore = 999999
        ttl = int(time.time() + 60*60*24*n)
        telemetry = {
            "action_id": action_id,
            "key": key,
            "invoked_by": trigger.object_id,
            "ttl": ttl,
            "status": "initializing"
        }
        stream_id = f'{hostname}/{username}/file-trigger-log.{key}'
        self.dao.get_streams().update_feed(stream_id, timestamp, telemetry)
        r = self._starting_trigger(key, username, action_id, trigger, populated_key, kwargs)
        update = {
            "status": "success" if r.success else "failure",
            "end_timestamp": int(time.time()),
            # TODO: David, in particular check the line below to make sure the expected result occurs
            "receipt": MessageToDict(r, preserving_proto_field_name=True)
        }
        self.dao.get_streams().update_feed(stream_id, timestamp, update)

        if r.success:
            stream_id = trigger.object_id + ".success"
            self.dao.get_docstore().increment_counter(trigger.object_id, 'trigger_count', amount=1)
        else:
            stream_id = trigger.object_id + ".fail"
            self.dao.get_docstore().increment_counter(trigger.object_id, 'fail_count', amount=1)
        d = MessageToDict(r, preserving_proto_field_name=True)
        d['ttl'] = ttl
        self.dao.get_streams().update_feed(stream_id, timestamp, d)


    def _starting_trigger(self, key, username, action_id, trigger, populated_key, kwargs):
        if action_id.startswith('sys.actions'):
            doc = self.docstore.get_document(action_id)
            action = Parse(json.dumps(doc), objs.Action(), ignore_unknown_fields=True)
            sys_name = action.sys_name
            sys_action_id = action.sys_action_id
            if sys_name == 'ecs-1':
                data = {} # TODO: from trigger and inject key
                for param in action.params:
                    var_name = param.var_name
                    if param.ptype == objs.ParameterType.KEY:
                        data[var_name] = key
                    elif var_name in trigger.param_values:
                        at = trigger.param_values[var_name]
                        data[var_name] = util.any_type_resolved(at)
                    else:
                        return objs.Receipt(success=False, error_message=f"Failed running {trigger.object_id} due to missing {var_name}")
                r = self.action_executor.begin_action_execution(action_id, username, data)
            elif sys_name == 'lambda-1':
                r, populated_key = self._execute_local_action(key, username, action_id, trigger, populated_key, kwargs)
            else:
                msg = f'Unknown system: {sys_name}'
                logging.error(msg)
                raise Exception(msg)

            # below stuff should be deprecated
            return r
        elif action_id.startswith('feaas-py.'):
            r, populated_key = self._execute_local_action(key, username, action_id, trigger, populated_key, kwargs)
            return r
        elif action_id.find('/ps-file-trigger.') > 0:
            print(f'Running pscript {action_id}')
            # TODO: should it be src_prefix or the prefix of the key?
            return self._execute_psee_job(action_id, key, trigger.src_prefix)
        else:
            msg = f'No implementation for handling Action {action_id}'
            logging.error(msg)
            raise Exception(msg)


    def _execute_local_action(self, key, username, action_id, trigger, populated_key, kwargs = {}):
        action = common.get_Action_from_action_id(action_id, self.dao)
        kwargs, populated_key = common.populate_kwargs(kwargs, trigger, action, populated_key, key)
        print(f"Running {action_id} with {kwargs}")
        try:
            r = self.action_executor.begin_action_execution(action_id, username, kwargs)
            new_tags = {}
            for k in r.outputs.keys():
                new_tags[k] = util.any_type_resolved(r.outputs[k])
            if trigger.object_id != '':
                if r.success:
                    self.dao.get_docstore().increment_counter(trigger.object_id, 'trigger_count', amount=1)
                else:
                    self.dao.get_docstore().increment_counter(trigger.object_id, 'fail_count', amount=1)
            timestamp = r.timestamp
            if timestamp == 0:
                timestamp = int(time.time() * 1000)
            ttl = int(time.time()) + 60 * 24 * 3
            data = { "success": r.success, "cost": r.cost, "kwargs": kwargs, "ttl": ttl }
            unique_id = trigger.object_id
            if not(r.success):
                data['error_message'] = r.error_message
                stream_id = f'{username}/stream.trigger-error.{trigger.src_prefix}.{unique_id}'
            else:
                stream_id = f'{username}/stream.trigger-out.{trigger.src_prefix}.{unique_id}'
            for k in r.outputs.keys():
                output = r.outputs[k]
                data[k] = util.any_type_resolved(output)
            try:
                self.streams.update_feed(stream_id, timestamp, data)
            except:
                logging.error(traceback.format_exc())
                error_key = f'sys/error/{stream_id}/{timestamp}.error.json'
                self.blobstore.save_blob(error_key, json.dumps(data))
            try:
                self.blobstore.update_object_tags(key, new_tags)
            except:
                msg = f'Could not tag {key}'
                print(msg)
                #return objs.Receipt(success=False, error_message=msg)
        except:
            s = traceback.format_exc()
            logging.error(s)
            self.dao.get_docstore().increment_counter(trigger.object_id, 'fail_count', amount=1)
            r = objs.Receipt(success=False, error_message=s)
        user_host = 'https://beta.dataskeptic.com'
        self.billing.file_billing(username, r, key, trigger, user_host)
        return r, populated_key


    def _remove_field(self, o, val):
        if isinstance(o, dict):
            return {key: self._remove_field(value, val) for key, value in o.items() if key != val}
        elif isinstance(o, list):
            return [self._remove_field(item, val) for item in o]
        else:
            return o


    def _execute_psee_job(self, script_object_id, key, src_prefix):
        psee = PlusScriptExecutionEngine(self.dao, self.action_executor)
        arr = key.split('/')
        # TODO: do some security
        # if arr[0] != 'user':
        #     return objs.Receipt(success=False, error_message='Inappropriate source file')
        username = arr[1] # TODO: get it from the trigger object
        script_dict = self.dao.get_docstore().get_document(script_object_id)
        script_dict = self._remove_field(script_dict, "byval")
        s = json.dumps(script_dict, cls=common.DecimalEncoder)
        script = Parse(s, objs.PlusScript(), ignore_unknown_fields=True)
        data = {}
        for param in script.inputs:
            var_name = param.var_name
            if param.ptype == objs.ParameterType.KEY:
                data[var_name] = key
            elif param.ptype == objs.ParameterType.PREFIX:
                data[var_name] = src_prefix
            else:
                if param.ptype == objs.ParameterType.BOOLEAN:
                    data[var_name] = param.bdefault
                elif param.ptype == objs.ParameterType.INTEGER:
                    data[var_name] = param.idefault
                elif param.ptype == objs.ParameterType.FLOAT:
                    data[var_name] = param.ddefault
                else:
                    data[var_name] = param.sdefault
        # TODO: generalize
        hostname = "plus_dataskeptic_com"
        job = psee.start_script(hostname, username, script, data)
        
        # TODO: run it
        job = psee.run_job(job)
        status = job.status
        while status == objs.PlusScriptStatus.RUNNING:
            print("----- ITERATE ---------", status)
            job = psee.run_job(job)
            status = job.status

        jobject_id = job.object_id
        i = jobject_id.find('/') + 1 + len('job.')
        outputs = {
            "job_id": objs.AnyType(ptype=objs.ParameterType.JOB_ID, sval=jobject_id[i:])
        }
        return objs.Receipt(
            success=job.status != objs.PlusScriptStatus.FAILED,
            error_message=job.err_message,
            outputs=outputs,
            primary_output='job_id')


