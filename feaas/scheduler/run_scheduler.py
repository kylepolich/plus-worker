from feaas.abstract import AbstractAction
from boto3.dynamodb.conditions import Key, Attr
from feaas.stream_processor import StreamProcessor
from feaas.sys.executor import ActionExecutor
from feaas.scheduler.scheduler import Scheduler
from feaas.stream.util import combine_receipts
from feaas.util import common
from feaas.stream import util
from datetime import datetime
import feaas.objects as objs
from google.protobuf.json_format import Parse, MessageToDict
import json
import logging
import time
from croniter import croniter
import traceback
import copy


#
# In feaas-py, called from app.py every minute
#
class RunScheduler(AbstractAction):


    def __init__(self, dao):
        hostname = objs.Parameter(
            var_name='hostname',
            label='Hostname',
            ptype=objs.ParameterType.STRING)
        t = objs.Parameter(
            optional=True,
            var_name='t',
            label='Time',
            ptype=objs.ParameterType.DATETIME)
        params = [hostname, t]
        outputs = []
        super().__init__(params, outputs)
        self.dao = dao
        self.docstore = dao.get_docstore()
        self.streams = dao.get_streams()
        sys_name = 'feaas-core'
        self.action_executor = ActionExecutor(dao, dao.sys_name)
        # self.billing = Billing(dao, portal_secret, stream_processor=stream_processor)


    def execute_action(self, hostname, t=None) -> objs.Receipt:
        if t is None:
            t = int(time.time())
        else:
            t = int(t / 1000)
        user_owner = f'sys.cron-user.{hostname}'

        # TODO: param to read from some particular owner
        telemetry = {
            "error_count": 0,
            "success_count": 0,
            "skip_count": 0,
            'ttl': int(time.time()) + 60*60*24*7
        }
        t=int(time.time()*1000)
        filterExpression = Attr('last_updated_at').lt(t)
        arr = self.docstore.get_list_with_filter(user_owner, filterExpression)
        for item in arr:
            oid = item['object_id']
            username = oid[len(user_owner)+1:]
            logging.info(f"-----[Beginning scheduler for {username}]----------")

            # scheduled_user_records_owner = f'{hostname}.{username}/scheduler'
            # doc = self.docstore.get_document(item['object_id'])
            # j = len('sys.cron-user.')
            # i = oid.find('.', j) + 1
            # username = oid[i:]
            success_count, error_count, skip_count = self._run_scheduler_for_user(hostname, username, t)
            print(f"Run result for {username}: success={success_count}, error={error_count}, skip={skip_count}")
            telemetry['success_count'] += success_count
            telemetry['error_count'] += error_count
            telemetry['skip_count'] += skip_count
            update = {
                "last_updated_at": t + 60 * 10,
                "last_run": telemetry
            }
            self.docstore.update_document(item['object_id'], update)

        n = time.time()
        telemetry['run_time_sec'] = int(n - t/1000)
        stream_id = f'sys.{hostname}.telemetry.run_cron'
        self.streams.update_feed(stream_id, t*1000, telemetry)
        outputs = {
            'timestamp': objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=t),
            'success_count': objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=telemetry['success_count']),
            'skip_count': objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=telemetry['skip_count']),
            'error_count': objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=telemetry['error_count']),
            'run_time_sec': objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=telemetry['run_time_sec'])
        }
        return objs.Receipt(success=True, primary_output='run_time_sec', outputs=outputs)


    def _run_scheduler_for_user(self, hostname, username, t):
        print("_run_scheduler_for_user", username, t)
        error_count = 0
        success_count = 0
        skip_count = 0
        # TODO: fix this here and in plus at the same time
        owner = f'{hostname}.{username}/scheduler'
        for item in self.docstore.get_list(owner):
            oid = item['object_id']
            doc = self.docstore.get_document(oid)
            try:
                sc, ec, skip_c = self._process_record(hostname, username, t, doc)
                success_count += sc
                error_count += ec
                skip_count += skip_c
            except:
                print(traceback.format_exc())
                error_count += 1
            update = {
                "last_run_at": t / 1000
            }
            self.docstore.update_document(oid, update)

        # something here
        return success_count, error_count, skip_count


    def _process_record(self, hostname, username, t, rec):
        # TODO: check this record itself to see if it needs to run
        update = {}
        error_count = 0
        success_count = 0
        skip_count = 0
        action_id = rec['action_id']
        cron = rec['cron']
        should_run = True
        reason = None
        # check last run
        if 'last_run_at' in rec:
            last_run_at = rec['last_run_at']
        else:
            last_run_at = 0

        if last_run_at == 0:
            should_run = True
        else:
            # Initialize croniter object with your cron string and start time

            # Get the next scheduled run time after the last_run_at
            dt = datetime.fromtimestamp(last_run_at)
            iter = croniter(cron, dt)
            next_run_at_dt = iter.get_next(datetime)
            print("last_run_at", last_run_at)
            print("dt", dt)
            print("cron", cron)

            # Check if the job should run
            should_run_in_n_sec = (next_run_at_dt - datetime.now()).total_seconds()
            if should_run_in_n_sec < 0:
                should_run = True
            else:
                print(rec['title'], '=======================================', rec['object_id'])
                print(f'last_run_at:       {dt} | cron: {cron}')
                print(f'next_run_at_dt:    {next_run_at_dt}')
                print(f'should_run_in_min: {should_run_in_n_sec/60}')
                should_run = False

        oid = rec['object_id']
        if not(should_run):
            logging.info(f"Skipping {oid}")
            skip_count += 1
        else:
            # TODO: eventually enqueue instead
            # TODO: populate kwargs
            logging.info(f"Running {oid}")

            data = {}
            if 'paramValues' in rec:
                for k in rec['paramValues'].keys():
                    at = rec['paramValues'][k]
                    if 'byval' in at:
                        del at['byval']
                    sss = json.dumps(at, cls=common.DecimalEncoder)
                    anyType = Parse(sss, objs.AnyType(), ignore_unknown_fields=True)
                    data[k] = util.any_type_resolved(anyType)

            # Determine if this is a PlusScript by checking multiple indicators
            plus_script_id = rec.get("script_object_id", "")
            is_plus_script = bool(plus_script_id and plus_script_id.strip())
            
            if is_plus_script:
                logging.info(f"Processing as PlusScript: {oid}")
                # Handle PlusScript scheduled item
                from feaas.psee.psee import PlusScriptExecutionEngine
                script_doc = self.docstore.get_document(plus_script_id)

                # prepare PlusScript proto
                script_dict = copy.deepcopy(script_doc)
                common.strip_bytes_fields(script_dict)
                plus_script = Parse(
                    json.dumps(script_dict, default=common.decimal_default),
                    objs.PlusScript(),
                    ignore_unknown_fields=True
                )

                psee = PlusScriptExecutionEngine(self.dao, self.action_executor)

                job = psee.start_script(hostname, username, plus_script, data)
                job = psee.run_job(job)

                # Convert job result to receipt
                if job.status == objs.PlusScriptStatus.SUCCEEDED:
                    receipt = objs.Receipt(success=True, outputs=job.output)
                    success_count += 1 
                else:
                    receipt = objs.Receipt(
                        success=False,
                        error_message=job.err_message if job.err_message else 'PlusScript failed'
                    )
                    error_count += 1
            else:
                # Regular action handling
                action_doc = self.docstore.get_document(action_id)
                # Action = common.build_action_class(action_id)
                # TODO: capture more metadata, also global on the action
                # baction_id = action_doc['sys_action_id'] if action_doc is not None and 'sys_action_id' in action_doc else action_id
                baction_id = action_doc["object_id"]
                # TODO: resolve it here to DB name to avoid headaches
                receipt = self.action_executor.begin_action_execution(baction_id, username, data)
            
            if receipt.success:
                success_count += 1
                self.docstore.increment_counter(rec['object_id'], 'run_count', amount=1)
            else:
                error_count += 1
                self.docstore.increment_counter(rec['object_id'], 'error_count', amount=1)
                msg = receipt.error_message
                logging.error(msg)
                update['last_err_at'] = t
                update['last_err_message'] = msg
            stream_id = rec['object_id'].replace('/scheduler.', '/scheduler-receipts.')
            o = MessageToDict(receipt, preserving_proto_field_name=True)
            o = common.clean_json_dict(o)
            # # TODO: convert to batch?
            o['ttl'] = t/1000 + 60*60*24*90
            print("receipt to stream_id =", stream_id)
            _ = self.streams.update_feed(stream_id, t, o)
            e = int(time.time())
            update["last_compute_sec"] = e - t/1000
            update["last_run_at"] = e
            _ = self.docstore.update_document(rec['object_id'], update)

        return success_count, error_count, skip_count


#------------------------------------------------------------------------------------------------------------------------------#


# #
# # Called from app.py every minute (deprecated)
# #
# class RunScheduleForAllUsers(AbstractAction):


#     def __init__(self, dao):
#         params = []
#         outputs = []
#         cost = 0.0
#         label = "Run Master Scheduler"
#         short_desc = label
#         long_desc = short_desc
#         fa_icon='fa-play'
#         super().__init__(params, outputs)
#         self.docstore = dao.get_docstore()
#         portal_secret   = 'deprecated'
#         sys_name = 'lambda-1'
#         stream_processor = StreamProcessor(dao, portal_secret, sys_name)
#         self.dao = dao
#         self.docstore = dao.get_docstore()
#         self.work_queue = dao.get_async_queue()
#         self.lambda_queue = dao.get_work_queue()


#     def execute_action(self) -> objs.Receipt:
#         run_count = 0
#         # TODO: get list by last_updated_at
#         items = self.docstore.get_list('sys.user')
#         max_since = -99999
#         for item in items:
#             if 'last_updated_at' in item:
#                 last_updated_at = int(item['last_updated_at'])
#                 since = int(time.time()) - last_updated_at
#                 if since > max_since:
#                     max_since = since
#                 if since > 60:
#                     username2 = item['object_id'][9:]
#                     data = {
#                         "action_id": 'feaas-py.chalicelib.actions.sys.run_scheduler.RunScheduleForUser',
#                         "username": username2
#                     }
#                     msg = json.dumps(data)
#                     self.work_queue.add_message(msg)
#                     run_count += 1
#                     up = { 'last_updated_at': int(time.time())}
#                     self.docstore.update_document(item['object_id'], up)
#         outputs = {
#             'run_count': objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=run_count),
#             'max_since': objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=max_since)
#         }
#         return objs.Receipt(success=True, outputs=outputs, primary_output='run_count')


# #------------------------------------------------------------------------------------------------------------------------------#


# class RunScheduleForUser(AbstractAction):


#     def __init__(self, dao):
#         username = objs.Parameter(
#             var_name='username',
#             label='Username',
#             ptype=objs.ParameterType.USERNAME)
#         params = [username]
#         outputs = []
#         cost = 0.0
#         label = "Run Scheduler For User"
#         short_desc = label
#         long_desc = short_desc
#         fa_icon='fa-play'
#         super().__init__(params, outputs)
#         self.docstore = dao.get_docstore()
#         portal_secret   = 'deprecated'
#         sys_name = 'lambda-1'
#         stream_processor = StreamProcessor(dao, portal_secret, sys_name)
#         self.dao = dao
#         self.work_queue = dao.get_async_queue()
#         self.lambda_queue = dao.get_work_queue()


#     def execute_action(self, username) -> objs.Receipt:
#         object_id = f'sys.user.{username}'
#         doc = self.docstore.get_document(object_id)
#         if doc is None:
#             hostname = 'plus_dataskeptic_com'
#             object_id = f'sys.user.{hostname}.{username}'
#             doc = self.docstore.get_document(object_id)
#             if doc is None:
#                 return objs.Receipt(success=False, error_message=f'No record found in {username}')
#         folder = f'{username}/app-v2.scheduler.singleton'
#         run_count = self._check_folder(folder)
#         po = 'run_count'
#         outputs = {
#             'run_count': objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=run_count)
#         }
#         last_updated_at = int(time.time())
#         self.docstore.update_document(object_id, {'last_updated_at': last_updated_at})
#         return objs.Receipt(success=True, outputs=outputs, primary_output=po)


#     def _should_run_cron(self, doc):
#         cron = doc['cron']
#         if 'last_updated_at' not in doc:
#             return True
#         last_run_at = int(doc['last_run_at'])
#         now = int(time.time())
#         cr = croniter(cron, now)
#         p = cr.get_prev()
#         if last_run_at >= p:
#             return False
#         else:
#             return True


#     def _check_folder(self, folder):
#         runs = 0
#         items = self.docstore.get_list(folder)
#         for item in items:
#             doc = self.docstore.get_document(item['object_id'])
#             if 'root' in doc:
#                 runs += self._check_folder(doc['object_id'])
#             elif 'title' in doc:
#                 i = folder.find('/')
#                 if i > 0:
#                     username = folder[0:i]
#                     r = self._should_run_cron(doc)
#                     if r:
#                         data = {
#                             "username": username,
#                             "action_id": 'feaas-py.chalicelib.actions.sys.run_scheduler.RunScheduledItemImmediately',
#                             "sch_object_id": doc['object_id']
#                         }
#                         msg = json.dumps(data)
#                         try:
#                             action = common.get_Action_from_action_id(doc['action_id'], self.dao)
#                         except:
#                             # TODO: rollbar
#                             print("Could not find", doc['action_id'])
#                             continue
#                         if action.action.runtime_id == 3:
#                             self.work_queue.add_message(msg)
#                         else:
#                             self.lambda_queue.add_message(msg)
#                         # from app import _process_sqs_record
#                         # _process_sqs_record(data)
#                         runs += 1
#                 else:
#                     # TODO rollbar
#                     pass
#         return runs


# #------------------------------------------------------------------------------------------------------------------------------#


# class RunScheduledItemImmediately(AbstractAction):


#     def __init__(self, dao):
#         sch_object_id = objs.Parameter(
#             var_name='sch_object_id',
#             label='Scheduled Item',
#             ptype=objs.ParameterType.OBJECT_ID,
#             src_owners=['kyle@dataskeptic.com/scheduler.singleton'])
#         params = [sch_object_id]
#         outputs = []
#         super().__init__(params, outputs)
#         self.docstore = dao.get_docstore()
#         portal_secret   = 'deprecated'
#         sys_name = 'lambda-1'
#         stream_processor = StreamProcessor(dao, portal_secret, sys_name)
#         self.scheduler = Scheduler(dao, portal_secret, stream_processor, sys_name)


#     def execute_action(self, sch_object_id) -> objs.Receipt:
#         doc = self.docstore.get_document(sch_object_id)
#         if doc is None:
#             return objs.Receipt(success=False, error_message=f'No record found in {sch_object_id}')
#         receipt = self.scheduler.handle_scheduler_item(sch_object_id, run_now=False)
#         return receipt


# #------------------------------------------------------------------------------------------------------------------------------#
