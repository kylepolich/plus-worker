from feaas.dao.blobstore.abstract_blobstore import AbstractBlobstore as Blobstore
from feaas.util import common
from datetime import datetime
import glob
import logging
import json
import os


log = logging.getLogger(__name__)
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))


class LocalBlobstore(Blobstore):


    def __init__(self, logger, root_dir):
        super().__init__(None) # TODO: why does it want bucket_name?
        self.root_dir = root_dir
        if not(self.root_dir.endswith('/')):
            self.root_dir += '/'
        self.logger = logger
        self.mock_dict = {}


    def save_blob(self, key, contents, public=False, metadata={}, content_type=None):
        fn = self._get_filename(key, ext=None)
        i = fn.rfind('/')
        try:
            os.makedirs(fn[0:i])
        except FileExistsError:
            pass
        if type(contents) == str:
            f = open(fn, 'w')
        elif type(contents) == dict:
            d = common.clean_json_dict(contents)
            contents = json.dumps(d)
            f = open(fn, 'w')
        else:
            f = open(fn, 'wb')
        f.write(contents)
        f.close()
        if metadata is not None:
            for k in metadata.keys():
                v = metadata[k]
                self.testing_set_blob_metadata(key, k, v)
        return fn


    def move(self, src_key: str, dest_key: str):
        pass


    def get_url_from_key(self, key):
        i = key.rfind('/') + 1
        return "./test/extracted_images/{}/".format(key[i:])


    def get_key_from_url(self, url):
        return url


    def delete_blob(self, key):
        fn = self._get_filename(key)
        if os.path.exists(fn):
            os.remove(fn)


    def _get_filename(self, key, ext=None):
        if len(key) == 0:
            return None
        if ext is None:
            ext = common.get_extension(key)
        # if key.find('://') != -1:
        #     key = Uri2S3key.convert(key, ext)
        if not(key.startswith(self.root_dir)):
            key = self.root_dir + key
        if key.endswith('/'):
            key = key[:-1]
        return key


    def get_blob(self, key):
        i = key.rfind('.')
        ext = key[i:]
        log.info(f'===[get_blob]===, {self.root_dir}, {key}, {ext}')
        fn = self._get_filename(key, ext)
        log.info(f'===[get_blob]===, {self.root_dir}, {key}, {fn}, {os.path.exists(fn)}')
        if os.path.exists(fn):
            f = open(fn, 'rb')
            content = f.read()
            f.close()
            # TODO: 200
        else:
            content = None
            # TODO: 404
            # TODO: msgs.SystemMessages.unknown_failure
        return content


    def testing_set_blob_metadata(self, key, attribute, value):
        if key not in self.mock_dict:
            self.mock_dict[key] = {}
        self.mock_dict[key][attribute] = value


    def get_blob_metadata(self, key):
        fn = self._get_filename(key)
        if os.path.exists(fn):
            last_modified = datetime.fromtimestamp(int(os.stat(fn).st_mtime))
            f = open(fn, 'rb')
            content = f.read()
            f.close()
            metadata = {
                'status_code': 200,
                'last_modified': last_modified,
                'content_length': len(content)
            }
        else:
            content = None
            metadata = {
                'status_code': 404,
                'msg': "Unknown failure"
            }
        if key in self.mock_dict:
            mocks = self.mock_dict[key]
            if mocks is not None:
                for k in mocks.keys():
                    v = mocks[k]
                    metadata[k] = v
        return metadata


    def get_keys_matching_pattern(self, pattern='*.crawl'):
        p = self.root_dir + pattern
        if p.find('*') == p.rfind('*'):
            p = p.replace('*', '**/*')
        imatches = glob.glob(p, recursive=True)
        matches = []
        n = len(self.root_dir)
        for match in imatches:
            matches.append(match[n:])
        return matches


    def exists(self, key):
        return os.path.exists(self.root_dir + key)


