import feaas.objects as objs
from feaas.router.dialogs.decision_tree import DecisionTreeDialog
from feaas.router.dialogs.faq import FaqDialog
# from feaas.router.dialogs.oc import ObjectConstructorDialog
from feaas.stream import util
from feaas.sys.executor import ActionExecutor
from feaas.util import common
from google.protobuf.json_format import Parse, MessageToDict
import json
import time
import traceback
import uuid


class Router(object):


    def __init__(self, dao, sys_name):
        self.decision_tree = DecisionTreeDialog()
        self.faq = FaqDialog(dao)
        # self.oc = ObjectConstructorDialog(dao)
        self.docstore = dao.get_docstore()
        self.executor = ActionExecutor(dao, sys_name)


    def _get_or_create_session(self, username, session_id, session_data):
        object_id = f'{username}/router-session.{session_id}'
        owner = f'{username}/router-session'
        if 'label' in session_data:
            label = session_data['label']
        else:
            label = str(uuid.uuid4())
        doc = self.docstore.get_document(object_id)
        if doc is None:
            now = int(time.time() * 1000)
            #     repeated Parameter params = 9;
            #     map<string, AnyType> param_values = 10;
            param_values = {}
            for k in session_data.keys():
                v = session_data[k]
                param_values[k] = util.any_type_ifier(v)
            session = objs.PortalSession(
                object_id=object_id,
                owner=owner,
                label=label,
                created_at=now,
                last_updated_at=now,
                param_values=param_values)
            self.docstore.save_document(session.object_id, MessageToDict(session, preserving_proto_field_name=True))
            return session
        else:
            return Parse(json.dumps(doc, cls=common.DecimalEncoder), objs.PortalSession(), ignore_unknown_fields=True)


    def route(self, username: str, router: objs.Router, session_data: dict, input_dict: dict, primary_input: str='text', app_object_id=None) -> dict:
        # app_object_id used for analytics update since router is nested in this app object
        # if 'stream_id' not in session_data:
        #     return {
        #         'error': 'No `stream_id` found in session'
        #     }
        print('zzz', input_dict, primary_input)
        session_data['last_message'] = input_dict[primary_input]
        # TODO: `session_data` - for slack should be user_id, etc.  How to make this custom to Router?
        now = int(time.time() * 1000)
        # robject_id = router.object_id
        # i = robject_id.find('/')
        # if i == -1:
        #     return None, objs.Receipt(success=False, error_message=f'Could not determine owner of {robject_id}')
        # username = robject_id[0:i]
        # session = self._get_or_create_session(username, session_id, session_data)
        # TODO: _bookmark
        # if '_bookmark' in session.param_values:
        #     bookmark = session.param_values['_bookmark'].sval
        dialogs = router.ordered_match_conditions
        receipts = []
        if len(dialogs) == 0:
            return objs.Receipt(success=False, error_message=f"No dialogs in router {router.object_id}")
        else:
            dialog, actions, dialog_num = self._route2(username, dialogs, session_data, input_dict, primary_input)
            if actions is not None:
                if app_object_id is not None:
                    try:
                        self._update_analytics(router, dialog_num, app_object_id)
                    except:
                        print('ERROR:', traceback.format_exc())
                # data = dict(input_dict)
                data = {}
                action_params = dialog.action_params
                m = {}
                for item in dialog.in_map:
                    src = item.src
                    dest = item.dest
                    m[src] = dest
                for unique_var_name in action_params.keys():
                    v = action_params[unique_var_name]
                    v2 = util.any_type_resolved(v)
                    value = self._resolve_session_vars(v2, session_data)
                    var_name = m[unique_var_name]
                    data[var_name] = value
                # data['session'] = session_data
                # data['text'] = "You said {session.last_message} ..."
                for action in actions:
                    receipt = self.executor.begin_action_execution(action.action_id, username, data)
                    receipts.append(receipt)
        if len(receipts) == 0:
            return objs.Receipt(success=True, error_message='Router has no action to run.')
        master_receipt = util.combine_receipts(now, receipts)
        # object_id = f'{username}/session.{session_owner}.{session_id}'
        output = {}
        for k in master_receipt.outputs.keys():
            v = master_receipt.outputs[k]
            output[k] = util.any_type_resolved(v)
        if not(master_receipt.success):
            output['error'] = master_receipt.error_message
        return output


    def _resolve_session_vars(self, tpl, session_data) -> str:
        d = {}
        i = tpl.find('{')
        arr = []
        s = 0
        while i != -1:
            arr.append(tpl[s:i])
            s = i
            j = tpl.find('}', i)
            if j != -1:
                s = j+1
                x = tpl[i+1:j]
                if x.startswith('session.') or x.startswith('session:'):
                    x = x[8:]
                if x.startswith('app:'):
                    x = 'app.' + x[4:]
                if x in session_data:
                    arr.append(session_data[x])
                elif x.find('|') > 0:
                    arr2 = x.split('|')
                    if arr[0] in session_data:
                        arr.append(session_data[arr[0]])
                    else:
                        arr.append(arr2[1])
                i=j+1
            else:
                arr.append(tpl[s:i])
            i = tpl.find('{', i+1)
        arr.append(tpl[s:])
        return ''.join(arr)


    def _route2(self, username: str, dialogs, session_data: dict, input_dict: dict, primary_input): # returns dialog, action, idx
        now = int(time.time() * 1000)
        for i, dialog in enumerate(dialogs):
            rtype = dialog.rtype
            actions = None
            if rtype == objs.RouterConditional.RouterType.CATCH_ALL:
                actions = dialog.actions
                return dialog, actions, i
            elif rtype == objs.RouterConditional.RouterType.DEFAULT:
                actions = self.decision_tree.handle_event(session_data, dialog, primary_input, input_dict, now)
            elif rtype == objs.RouterConditional.RouterType.FAQ:
                actions = self.faq.handle_event(session_data, dialog, primary_input, input_dict, now)
            elif rtype == objs.RouterConditional.RouterType.OBJECT_CONSTRUCTOR:
                actions = self.oc.handle_event(session_data, dialog, primary_input, input_dict, now)
            # elif rtype == objs.RouterConditional.RouterType.HITL:
            #     actions = 
            # elif rtype == objs.RouterConditional.RouterType.ACTION_RUNNER:
            #     actions = 
            else:
                raise Exception(f"Unsupported RouteType {objs.RouterConditional.RouterType.Name(rtype)}")
            if actions is not None:
                return dialog, actions, i
        # This is kind of like an error. Router didn't explicitly ignore the record (empty list), just failed to react at all.
        return None, None, -1


    def _update_analytics(self, router, i, app_object_id):
        if app_object_id is None or app_object_id.strip() == '':
            return
        dialog = router.ordered_match_conditions[i]
        now = int(time.time() * 1000)
        pe = f'router.ordered_match_conditions[{i}].use_count'
        try:
            print("attempting", app_object_id, pe)
            _ = self.docstore.increment_counter(app_object_id, pe, amount=1)
        except:
            msg = traceback.format_exc()
            print('ERROR:', msg)
        if dialog.first_used == 0:
            pe = f'router.ordered_match_conditions[{i}].first_used'
            print('dialog first use', pe, app_object_id)
            self.docstore.update_document(app_object_id, { pe: now })
            print("@@@@@")
        pe = f'router.ordered_match_conditions[{i}].last_used'
        self.docstore.update_document(app_object_id, { pe: now })
        print('updated analytics on', app_object_id)


    def handle_router_and_response(self, username, session_id, primary_input, input_dict, r, session_data, timestamp) -> objs.Receipt:
        resp = self.router.route(username, session_id, primary_input, input_dict, r, session_data)
        return resp
        #
        #
        #
        #
        #
        #
        if matching_dialog is None or actions is None:
            return objs.Receipt(success=False, error_message='Router could not handle event')
        if len(actions) == 0:
            return objs.Receipt(success=True)

        # TODO: save session (_bookmark and such).  Actions might make independent updates

        receipts = []
        session_stream_id = session_data['stream_id']
        input_dict['stream_id'] = session_stream_id
        input_dict['session'] = session_data
        for action in actions:
            # TODO: confirm extra input_dict are ignored, not thrown as exceptions
            # TODO: only if ParameterType.CURRENT_FEED_ID
            input_dict['router_app_object_id'] = r.object_id
            receipt = self.action_executor.begin_action_execution(action.action_id, username, input_dict)
            receipts.append(receipt)
            # TODO: execute action, update receipt
        master_receipt = util.combine_receipts(None, receipts)
        router_conditional_stream_id = matching_dialog.recent_stream_id
        session_username = session_data['session_username']
        record = {
            'primary_input': primary_input,
            'input_dict': input_dict,
            'session_username': session_username,
            'response': MessageToDict(master_receipt, preserving_proto_field_name=True)
        }
        print('writing to', router_conditional_stream_id)
        self.streams.update_feed(router_conditional_stream_id, timestamp, record)
        # print('writing to', session_stream_id)
        # self.streams.update_feed(session_stream_id, timestamp, record)
        return master_receipt
