# TODO: put this back in feaas-py.  This doesn't belong here.

from feaas.util import common
import json
import requests


class Notifier(object):


    def __init__(self, secret):
        self.secret = secret


    def _get_headers(self, username):
        token = common.md5(f"{self.secret}/{username}")
        headers = {
            'Content-Type': 'application/json',
            'Authorization': token,
        }
        return headers


    def send_file_notification(self, user_host, username, dest_key) -> bool:
        headers = self._get_headers(username)
        data = '{ "ntype": "REFRESH_KEY", "key": "' + dest_key + '" }'
        #data = '{ "ntype": "REFRESH_PREFIX", "prefix": "' + common.get_prefix(dest_key) + '" }'
        url = f'{user_host}/api/notify/user/{username}'
        return True
        # r = requests.post(url, headers=headers, data=data)
        # s = r.status_code == 200
        # if not(s):
        #     print("ERROR WITH NOTIFICATION API", url, data, r)
        # return s


    def send_folder_notification(self, user_host, username, dest_prefix) -> bool:
        headers = self._get_headers(username)
        data = '{ "ntype": "REFRESH_PREFIX", "prefix": "' + dest_prefix + '" }'
        url = f'{user_host}/api/notify/user/{username}'
        return True
        # r = requests.post(url, headers=headers, data=data)
        # s = r.status_code == 200
        # if not(s):
        #     print("ERROR WITH NOTIFICATION API", url, data, r)
        # return s


    def send_user_update_notification(self, user_host, username) -> bool:
        headers = self._get_headers(username)
        data = '{"ntype":"USER_UPDATE"}'
        # r = requests.post(f'{user_host}/api/notify/user/{username}', headers=headers, data=data)


    def send_metadata_notification(self, user_host, username, dest_key) -> bool:
        headers = self._get_headers(username)
        data = { "ntype": "OPEN_FILE_METADATA", "key": dest_key }
        url = f'{user_host}/api/notify/user/{username}'
        # r = requests.post(url, headers=headers, data=json.dumps(data))
        # return r.status_code
        return 200


    def send_stream_constructor_updated_notification(self, user_host, username, object_id) -> bool:
        headers = self._get_headers(username)
        data = { "ntype": "STREAM_CONSTRUCTOR_UPDATED", "object_id": object_id }
        url = f'{user_host}/api/notify/stream_constructor/{username}'
        # r = requests.post(url, headers=headers, data=json.dumps(data))
        # return r.status_code == 200
        return True

