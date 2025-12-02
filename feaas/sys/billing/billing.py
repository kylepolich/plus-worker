import feaas.objects as objs
from feaas.sns_processor import handle_json
# from feaas.portal.notify import Notifier
from feaas.util import common
from google.protobuf.json_format import Parse, MessageToDict
import json
import requests
import time
import traceback


class Billing(object):


    def __init__(self, dao, secret, portal_api_url='https://beta.dataskeptic.com', stream_processor=None):
        self.docstore = dao.get_docstore()
        self.streams = dao.get_streams()
        self.blobstore = dao.get_blobstore()
        self.secret = secret
        # TODO: update to beta.dataskeptic.com
        # TODO: try spoofing another username
        self.portal = portal_api_url
        # self.notifier = Notifier(dao.notification_secret)
        self.stream_processor = stream_processor


    def adjust_storage(self, event_type, key):
        arr = key.split('/')
        if arr[0] != 'user':
            if key.startswith('sys/'):
                return
            else:
                print('huh?', arr)
                return
        if event_type == objs.StreamEvent.EventType.BLOB_CREATED:
            o = self.blobstore.get_blob_metadata(key)
            if o is None:
                print(f'ERROR: Cannot find {key} in {self.blobstore.bucket_name}')
                return
            o['owner'] = 'sys.storage'
            self.docstore.save_document(key, o)
        elif event_type == objs.StreamEvent.EventType.BLOB_DELETED:
            o = self.docstore.get_document(key)
            if o is None:
                print(f'ERROR: No dynamodb record for {key}, assuming storage = 0')
            self.docstore.delete_document(key)
        else:
            print(f"ERROR: unknown event_type: {event_type}")
        if 'content_length' in o:
            size = o['content_length'] # in bytes
        else:
            print('No content length in', o)
            size = 0
        if event_type == objs.StreamEvent.EventType.BLOB_DELETED:
            size *= -1
        username = arr[1]
        object_id = f'sys.user.{username}'
        o = self.docstore.get_document(object_id)
        x = self.docstore.increment_counter(object_id, 'current_storage_space', size)
        o = self.docstore.get_document(object_id)
        try:
            user = Parse(json.dumps(o, cls=common.DecimalEncoder), objs.UserAccount(), ignore_unknown_fields=True)
        except:
            print('=*=*=*===============================')
            print(username, object_id, o)
            msg = traceback.format_exc()
            print(msg)
            print('=*=*=*===============================')
        # TODO: consider capping abd more importantly UN-capping


    def _billing(self, username, receipt, details, user_host, timestamp=None):
        cost = receipt.cost
        details['cost'] = cost
        if timestamp is None:
            timestamp = int(time.time() * 1000)
        if username != 'test' and receipt.cost != 0:
            object_id = f'sys.user.{username}'
            if cost < 0:
                print('why is cost < 0?', cost, receipt)
                cost *= -1
            self.docstore.increment_counter(object_id, 'credits_remaining', -1 * cost)
            billing_stream_id = f'sys.stream.billing.{username}'
            self.streams.update_feed(billing_stream_id, timestamp, details)
            details['stream_id'] = billing_stream_id
            # event = handle_json(details)
            print("@@@details", details)
            receipt, _ = self.stream_processor.handle_stream_event(billing_stream_id, timestamp, details, user_host)
            # if cost > 0.001:
            #     self.notifier.send_user_update_notification(user_host, username)
        else:
            print("free!")


    def stream_billing(self, username, receipt, key, timestamp, user_host):
        details = {
            "cost": receipt.cost,
            "key": key,
            "src": "stream"
        }
        # TODO: get `action_id` in here via master_receipt
        self._billing(username, receipt, details, user_host, timestamp)


    def file_billing(self, username, receipt, key, trigger, user_host):
        try:
            label = trigger.label
        except:
            label = 'unknown'
        details = {
            "key": key,
            "trigger_object_id": trigger.object_id,
            "action_id": trigger.action_id,
            "label": label,
            "src": "file"
        }
        token = common.md5(f"{self.secret}/{username}")
        headers = {
            'Content-Type': 'application/json',
            'Authorization': token,
        }
        data = {
            "ntype": "NEW_FILE_CREATED",
            "key": key
        }
        data = json.dumps(data)
        r = requests.post(f'{self.portal}/api/notify/user/{username}', headers=headers, data=data)
        print('_billing -> portal notification reply:', r.content.decode('utf-8'))
        self._billing(username, receipt, details, user_host)


    def queue_billing(self, username, receipt, action_id, user_host):
        details = {
            "action_id": action_id,
            "src": "queue"
        }
        self._billing(username, receipt, details, user_host)


    def api_billing(self, username, receipt, user_host):
        details = {
            "src": "api"
        }
        self._billing(username, receipt, details, user_host)


    def scheduler_billing(self, username, receipt, item, user_host):
        details = {
            "src": "scheduler"
        }
        self._billing(username, receipt, details, user_host)

