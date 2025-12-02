from abc import  abstractmethod
from feaas.dao.queue.abstract_queue import AbstractQueue as Queue
import hashlib


class LocalQueue(Queue):


    def __init__(self, logger):
        super().__init__()
        self.queue = []
        self.logger = logger


    def _add_message(self, msg) -> str:
        self.queue.append(msg)
        print(msg)
        print(type(msg))
        if type(msg) is not dict:
            msg = msg.to_dict()
        hash_object = hashlib.md5(json.dumps(msg).encode('utf-8'))
        return hash_object.hexdigest()


    def _delete_message(self, receipt_handle):
        pass


    def _get_message(self):
        return self.queue.pop()


    def get_size(self):
        return len(self.queue)

