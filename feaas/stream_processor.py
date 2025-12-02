import boto3
from boto3.dynamodb.conditions import Key
import copy
from feaas.objects import StreamEvent
import feaas.objects as objs
from feaas.stream import util
from feaas.util import common
from feaas.sys.billing.billing import Billing
from feaas.sys.executor import ActionExecutor
from feaas.psee.psee import PlusScriptExecutionEngine
from feaas.router.router import Router
from datetime import datetime
from decimal import Decimal
from google.protobuf.json_format import MessageToDict, Parse
from importlib import import_module
import json
import logging
import os
import requests
import traceback
import time
from typing import Tuple
import uuid


log = logging.getLogger(__name__)
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))


def remove_byval(d):
    if not isinstance(d, dict):
        return d

    # Create a copy of the dictionary to avoid modifying the original one
    d = dict(d)

    # Use a list to avoid 'RuntimeError: dictionary changed size during iteration'
    keys_to_check = list(d.keys())

    for key in keys_to_check:
        if key == 'byval':
            del d[key]
        elif isinstance(d[key], dict):
            d[key] = remove_byval(d[key])
        elif isinstance(d[key], list):
            # Handle lists of dictionaries
            d[key] = [remove_byval(item) if isinstance(item, dict) else item for item in d[key]]

    return d


class StreamProcessor(object):


    def __init__(self, dao, secret, sys_name):
        self.dao = dao
        self.streams = dao.get_streams()
        self.blobstore = dao.get_blobstore()
        self.docstore = dao.get_docstore()
        self.action_executor = ActionExecutor(dao, sys_name)
        self.trigger_cache = {}
        self.trigger_cache_ttl = {}
        self.billing = Billing(dao, secret, stream_processor=self)
        self.router = Router(dao, sys_name)
        self.psee = PlusScriptExecutionEngine(dao, self.action_executor)


    def _load_triggers(self, stream_id):
        resp = self.docstore.table.query(
            IndexName="ownerIndex",
            KeyConditionExpression=Key('owner').eq(stream_id))
        triggers = {}
        triggers_ttl = {}
        expire_at = int(time.time()) + 30
        for item in resp['Items']:
            object_id = item['object_id']
            # TODO: don't get if not expired
            titem = self.docstore.get_document(object_id)
            try:
                trigger = Parse(json.dumps(titem, cls=common.DecimalEncoder), objs.StreamTrigger(), ignore_unknown_fields=True)
            except:
                # TODO: remove this after Ilya has the fix in place
                for im in titem["in_map"]:
                    if 'static_val' in im:
                        if 'byval' in im['static_val']:
                            del im['static_val']['byval']
                for im in titem["out_map"]:
                    if 'static_val' in im:
                        if 'byval' in im['static_val']:
                            del im['static_val']['byval']
                trigger = Parse(json.dumps(titem, cls=common.DecimalEncoder), objs.StreamTrigger(), ignore_unknown_fields=True)
            k = trigger.object_id
            triggers[k] = trigger
            triggers_ttl[k] = expire_at
        self.trigger_cache[stream_id] = triggers
        self.trigger_cache_ttl[stream_id] = triggers_ttl
        return len(resp['Items'])


    def _cache_trigger(self, trigger) -> objs.Receipt:
        # TODO: which ones match? run them
        # TODO: get their outputs and compile the results
        stream_id = trigger.feed_id
        if stream_id not in self.trigger_cache:
            self.trigger_cache[stream_id] = {}
            self.trigger_cache_ttl[stream_id] = {}
        k = trigger.object_id
        self.trigger_cache[stream_id][k] = trigger
        self.trigger_cache_ttl[stream_id][k] = int(time.time()) + 30


    def handle_stream_event(self, stream_id, timestamp, data, user_host) -> objs.Receipt:
        if stream_id == 'sys':
            username = 'sys'
        else:
            # TODO: generalize
            hostname = 'plus_dataskeptic_com'
            arr = stream_id.split("/")
            if len(arr) > 2:
                username = arr[1]
            elif len(arr) == 2:
                username = arr[0]
            else:
                username = ""
            user_json = self.docstore.get_document(f"sys.user.{hostname}.{username}")
            i = stream_id.find('/')
            if i == -1:
                return objs.Receipt(success=False, error_message=f'Stream {stream_id} has no username.'), {}
            if user_json is not None:
                if 'param_values' in user_json:
                    # Fix plus fuck up
                    for pv in user_json['param_values'].keys():
                        values = user_json['param_values'][pv]
                        if 'byval' in values:
                            del user_json['param_values'][pv]['byval']
                user = Parse(json.dumps(user_json, cls=common.DecimalEncoder), objs.UserAccount(), ignore_unknown_fields=True)
            else:
                if username == 'test' or username == 'test@dataskeptic.com':
                    user = objs.UserAccount(username='test', active=True, credits_remaining=99, max_storage_space_mb=1)
                else:
                    msg = f"Cannot find username {username} for handling {stream_id}"
                    log.error(msg)
                    # TODO: increment a counter
                    # TODO: run constructor_object_id="sys.app.stream" which should have been run during first signup
                    return objs.Receipt(success=False, error_message=msg), {}
            if not(user.active):
                msg = f"Dropping event for inactive user {username} on {stream_id}"
                log.error(msg)
                return objs.Receipt(success=False, error_message=msg), {}
        receipt = self._handle_validated_stream_event(username, stream_id, timestamp, data, user_host)
        if receipt.success:
            s = '/streams.webhook.'
            i = stream_id.find(s)
            if i > 0:
                i += len(s)
                uid = stream_id[i:]
                self._run_webhooks(username, uid, data, timestamp)
        return receipt, data


    def handle_stream_event_v2(self, stream_id, timestamp, data, user_host, username) -> objs.Receipt:
        # TODO: throttle inactive accounts
        # ??
        n = self._load_triggers(stream_id)
        # TODO: Run routers before triggers
        triggers = self._get_triggers(stream_id, True)
        n2 = len(triggers)
        print(f"Considering {n}:{n2} triggers for {stream_id}")
        orig_item = {}

        # run triggers
        hostname = 'plus_dataskeptic_com'
        master_receipt, defers = self._run_triggers(triggers, hostname, username, data)
        # append new actions
        for k in master_receipt.outputs.keys():
            any_type = master_receipt.outputs[k]
            data[k] = util.any_type_resolved(any_type)
        # add to feed
        if 'timestamp' in data:
            del data['timestamp']
        if len(data) > 0:
            self.streams.update_feed(stream_id, timestamp, data)
        else:
            print(f"Surprised that write to {stream_id} has no data")
        # TODO: run post-trigger actions
        try:
            self._run_post_triggers(username, stream_id, timestamp, data, defers)
        except:
            err = traceback.format_exc()
            print(err)
            log.error(err)
        # Save to blobstore for logging purposes
        # dts = str(datetime.fromtimestamp(int(timestamp / 1000)))[0:10]
        # uid = str(uuid.uuid4())[0:8]
        # key = f'sys/sns/{dts}/{stream_id}/{timestamp}-{uid}.sns.event'
        # self.blobstore.save_blob(key, json.dumps(data))
        key = str(uuid.uuid4())[0:8]
        self.billing.stream_billing(username, master_receipt, key, timestamp, user_host)
        master_receipt.timestamp = timestamp
        data['timestamp'] = timestamp
        if master_receipt is None: #No router
            if 'timestamp' in data:
                del data['timestamp']
                self.streams.update_feed(stream_id, timestamp, data)
        return master_receipt, data


    def _run_webhooks(self, username, uid, saved_record, timestamp):
        object_id = f'{username}/app-v2.webhook.{uid}'
        doc = self.docstore.get_document(object_id)
        if doc is None:
            return
        app = Parse(json.dumps(doc), objs.PortalApplication(), ignore_unknown_fields=True)
        url = app.param_values['payload_url'].sval
        try:
            r = requests.post(url, json=saved_record)
        except:
            msg = traceback.format_exc()
            r = objs.Receipt(success=False, error_message=msg)
        stream_id=f'{username}/streams.webhook.{uid}'
        data = MessageToDict(r, preserving_proto_field_name=True)
        self.streams.update_feed(stream_id, timestamp, data)


    def _handle_validated_stream_event(self, username, stream_id, timestamp, data, user_host) -> objs.Receipt:
        # TODO: handle routers?
        master_receipt = self._handle_triggers(username, stream_id, timestamp, data, user_host)
        if master_receipt is None: #No router
            if 'timestamp' in data:
                del data['timestamp']
                self.streams.update_feed(stream_id, timestamp, data)
        return master_receipt


    def handle_router_and_response(self, username, session_id, primary_input, input_dict, r, session_data, timestamp1, app_object_id) -> objs.Receipt:
        resp = self.router.route(username, r, session_data, input_dict, primary_input, app_object_id)
        ###### TEMP START ############################################
        text = input_dict[primary_input]
        if text.startswith('set '):
            arr = text[4:].split('=')
            v = arr[0]
            s = arr[1]
            outputs[v] = objs.AnyType(ptype=objs.ParameterType.STRING, sval=s)
        ###### TEMP END ##############################################
        reply = resp['text']
        timestamp2 = int(time.time() * 1000)
        if timestamp2 == timestamp1:
            timestamp2 = timestamp1 + 1
        self.streams.update_feed(stream_id, timestamp1, resp)
        outputs = {
            'reply': objs.AnyType(ptype=objs.ParameterType.STRING, sval=reply),
            'timestamp': objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=timestamp2)
        }
        return objs.Receipt(success=True, outputs=outputs, primary_output='reply')


    def _handle_triggers(self, username, stream_id, timestamp, data, user_host):
        n = self._load_triggers(stream_id)
        # TODO: Run routers before triggers
        print(f"Considering {n} triggers for {stream_id}!")
        orig_item = {}
        # run triggers
        hostname = 'plus_dataskeptic_com'
        master_receipt, defers = self.run_triggers(hostname, username, stream_id, timestamp, data)
        # append new actions
        for k in master_receipt.outputs.keys():
            any_type = master_receipt.outputs[k]
            data[k] = util.any_type_resolved(any_type)
        # add to feed
        if 'timestamp' in data:
            del data['timestamp']
        self.streams.update_feed(stream_id, timestamp, data)
        # TODO: run post-trigger actions
        try:
            self._run_post_triggers(username, stream_id, timestamp, data, defers)
        except:
            err = traceback.format_exc()
            print(err)
            log.error(err)
        # Save to blobstore for logging purposes
        dts = str(datetime.fromtimestamp(int(timestamp / 1000)))[0:10]
        uid = str(uuid.uuid4())[0:8]
        key = f'sys/sns/{dts}/{stream_id}/{timestamp}-{uid}.sns.event'
        # o = common.clean_json_dict(data)
        # self.blobstore.save_blob(key, json.dumps(o))
        self.billing.stream_billing(username, master_receipt, key, timestamp, user_host)
        master_receipt.timestamp = timestamp
        return master_receipt


    def _run_post_triggers(self, username, stream_id, timestamp, processed_event: dict, defers):
        receipts = []
        for item in defers:
            action_id, kwargs = item
            if 'timestamp' in kwargs:
                kwargs['timestamp'] = timestamp
                receipt = self.action_executor.begin_action_execution(action_id, username, kwargs)
                receipts.append(receipt)
        if stream_id == 'sys':
            if 'event' in processed_event and processed_event['event']=='signup':
                action_id = 'feaas-py.chalicelib.actions.stream.email_immediate.EmailImmediate'
                username = 'kyle@dataskeptic.com'
                kwargs = {
                    "from_address": username,
                    "to_address": username,
                    "data": processed_event
                }
                receipt = self.action_executor.begin_action_execution(action_id, username, kwargs)
                receipts.append(receipt)
        master_receipt = util.combine_receipts(timestamp, receipts)
        update = {}
        for k in master_receipt.outputs.keys():
            out = master_receipt.outputs[k]
            update[k] = util.any_type_resolved(out)
        self.streams.update_feed(stream_id, timestamp, update)


    def _get_triggers(self, stream_id, before):
        triggers = []
        if stream_id in self.trigger_cache:
            inner = self.trigger_cache[stream_id]
            for item in inner.values():
                if before:
                    triggers.append(item)
        return triggers


    def run_triggers(self, hostname, username, stream_id, timestamp, data) -> objs.Receipt:
        triggers = self._get_triggers(stream_id, True)
        if len(triggers) == 0:
            return objs.Receipt(success=True, error_message="", timestamp=timestamp), []
        return self._run_triggers(triggers, hostname, username, data)


    def _run_triggers(self, triggers, hostname: str, username: str, result: dict) -> objs.Receipt:
        skipped = []
        master_receipt = objs.Receipt(success=True)
        defers = []
        for trigger in triggers:
            for item in trigger.in_map:
                # TODO: Plus is saving wrong.  Statics are going to src not sdefault :(
                sdefault = item.src
                item.src = item.dest
                dest = item.dest
                svalue = sdefault
                if item.ptype == objs.ParameterType.TIMESTAMP and item.dest not in result:
                    result[item.dest] = int(time.time() * 1000)
                else:
                    result[item.src] = svalue
            run_check = self._can_run(trigger, result)
            if run_check:
                try:
                    # TODO: feaas vs chalice
                    # TODO: not defering action
                    receipt2, defer = self._run_trigger(trigger, hostname, username, result)
                    if defer is not None:
                        defers.append(defer)
                    elif master_receipt is None:
                        master_receipt = receipt2
                    else:
                        master_receipt = util.combine_receipts(receipt2.timestamp, [master_receipt, receipt2])
                except:
                    err = traceback.format_exc()
                    print(err)
                    log.error(err)
            else:
                print("Skipping", trigger)
                skipped.append(trigger)
        if len(skipped) > 0 and len(skipped) == len(triggers):
            log.error("Blocked!")
            for item in skipped:
                print("skipped:", item)
            return objs.Receipt(success=False, error_message="Blocked"), []
        elif len(skipped) > 0:
            result = {}
            for k in master_receipt.outputs.keys():
                value = util.any_type_resolved(master_receipt.outputs[k])
                result[k] = value
            receipt2, defers2 = self._run_triggers(skipped, hostname, username, result)
            if defers2 is not None:
                defers.extend(defers2)
            else:
                master_receipt = util.combine_receipts(master_receipt.timestamp, [master_receipt, receipt2])
        return master_receipt, defers


    def _can_run(self, trigger, result_so_far):
        for mitem in trigger.in_map:
            src = mitem.src
            dest = mitem.dest
            if src is None or src == '':
                pass # going to use the default value
            elif src not in result_so_far:
                if src == 'timestamp':
                    return True
                else:
                    print("Can't run without", src)
                    return False
        return True


    def _run_trigger(self, trigger, hostname, username, result_so_far: dict):# -> objs.Receipt, []:
        if trigger.action_id is None or trigger.action_id.strip() == '':
            msg = f"invalid trigger, missing action_id on object_id={trigger.object_id}"
            print(msg)
            return objs.Receipt(success=True, error_message=msg), None
        assert type(result_so_far) == dict
        if trigger.action_id.find("/ps-stream-trigger.") > 0:
            doc = self.docstore.get_document(trigger.action_id)
            doc = remove_byval(doc)
            s = json.dumps(doc, cls=common.DecimalEncoder)
            pscript = Parse(s, objs.PlusScript(), ignore_unknown_fields=True)
            job = self.psee.start_script(hostname, username, pscript, data=result_so_far)
            while job.status != objs.PlusScriptStatus.FAILED and job.status != objs.PlusScriptStatus.SUCCEEDED:
                job = self.psee.run_job(job)
                print("job:", job.status, job.iteration)
            my_receipt = None
            for k in job.receipts.keys():
                r = job.receipts.get(k)
                if my_receipt == None:
                    my_receipt = r
                else:
                    my_receipt = common.extend_receipt(my_receipt, dict(r.outputs))

            # TODO: better error handling feedback loop
            if my_receipt is None:
                my_receipt = objs.Receipt(success=False, error_message="No receipts found")
            return my_receipt, None

        result = {}
        # kwargs = {}
        kwargs = dict(result_so_far)
        # TODO: if has timestamp with now, needs to run after write
        defer = []
        action_id = trigger.action_id
        should_defer = False
        timestamp = None
        stream_id = trigger.owner
        for mitem in trigger.in_map:
            src = mitem.src
            dest = mitem.dest
            action_id = trigger.action_id

            for parameter_map in trigger.in_map:
                # TODO: file yet another bug for this Plus fuck up
                dest = parameter_map.dest
                sval = parameter_map.static_val.sval
                if sval.strip() != '':
                    kwargs[dest] = sval
            
            # TODO: need a proper way of doing this
            if action_id.find('EmailStreamItem') > 0:
                should_defer = True
            if src is None or src == '':
                sdef = mitem.sdefault
                # TODO: for Gaussian noise, it should go in .ddefault, although Portal currently puts it here
                if sdef in result_so_far:
                    kwargs[dest] = result_so_far[sdef]
                else:
                    kwargs[dest] = sdef
            elif src == 'timestamp':
                if 'timestamp' in result_so_far:
                    timestamp = int(result_so_far['timestamp'])
                else:
                    timestamp = int(time.time() * 1000)
                if dest in kwargs:
                    del kwargs[dest]
                kwargs[dest] = None
                should_defer = False
            else:
                if result_so_far[src] != "":
                    kwargs[dest] = result_so_far[src]
        # timestamp not in in_map, get from action
        if should_defer:
            return None, (action_id, kwargs)
        i = trigger.owner.find('/')
        if timestamp is None:
            timestamp = int(time.time() * 1000)
        ss = common.clean_json_dict(kwargs)
        # self.streams.save_stream_item(stream_id, timestamp, ss)
        kwargs['stream_id'] = stream_id
        kwargs['timestamp'] = timestamp
        try:
            receipt = self.action_executor.begin_action_execution(action_id, username, kwargs)
        except:
            err = traceback.format_exc()
            print(err)
            receipt = objs.Receipt(success=False, error_message=err, cost=0.0, timestamp=int(time.time() * 1000))
        unmapped_outputs = receipt.outputs
        mapped_outputs = {}
        for param_map in trigger.out_map:
            src = param_map.src
            dest = param_map.dest
            mapped_outputs[dest] = unmapped_outputs[src]
        r = objs.Receipt(
            success=receipt.success,
            error_message=receipt.error_message,
            outputs=mapped_outputs,
            primary_output=receipt.primary_output,
            timestamp=receipt.timestamp,
            cost=receipt.cost
        )
        return r, None

