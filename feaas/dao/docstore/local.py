from abc import  abstractmethod
from feaas.util import common
from feaas.dao.docstore.abstract_docstore import AbstractDocstore as Docstore
from feaas.util import common
import os
import pickle



class LocalDocstore(Docstore):


    def __init__(self):
        super().__init__()
        self._database = {}


    def _generate_initial_document(self, key):
        metadata = { "TODO:": "fix the /models/" } #Metadata(key)
        return metadata


    def get_document(self, key, initialize=False):
        if key in self._database:
            return self._database[key]
        elif initialize:
            doc = self._generate_initial_document(key)
            self.save_document(key, doc)
            return doc
        else:
            return None


    def save_document(self, key, contents):
        self._database[key] = contents
        return key


    def update_document(self, key, contents):
        if key in contents:
            for k in contents.keys():
                self._database[key][k] = contents[k]
        else:
            self.save_document(key, contents)


    def get_all(self):
        return self._database


    def delete_document(self, key):
        if key in self._database:
            del self._database[key]
            return True
        return False


    def search(self, pattern):
        """ Please override this.  Your persistence layer should do better! """
        items = self.get_all()
        results = []
        for item_key in items.keys():
            item = items[item_key]
            if common.match(pattern, item_key):
                results.append(item)
        return results


    def increment_counter(self, object_id, name, amount=1):
        raise Exception("Not impl")


    def update_feed(self, stream_id, timestamp, record):
        record['timestamp'] = timestamp
        if stream_id not in self._database:
            self._database[stream_id] = []
        self._database[stream_id].append(record)
        self._database[stream_id] = sorted(self._database[stream_id], key=lambda x: x['timestamp'])
