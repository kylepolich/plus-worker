from abc import abstractmethod
from feaas.dao import abstract_wiring as wiring
from feaas.util import common
from datetime import datetime


class Search(wiring.Wiring):
    def __init__(self, es_url='', es_user='', es_password=''):
        self.es_url = es_url
        self.es_user = es_user
        self.es_password = es_password
        self.doc_type = '_doc'
        pass


    """
    -> Reset functions. It will be enabled on only development mode
    """
    @abstractmethod
    def delete_index(self, index):
        pass

    """
    <- Reset functions end
    """
    @abstractmethod
    def create_index(self, index):
        pass

    """
    Create, Save, Update, Delete, Get document in ES, { index: index, doc_type: extension, id: key }'
    """
    def set_index(self, index):
        self.index = index
        self.create_index(self.index)


    def set_doc_type(self, doc_type):
        self.doc_type = doc_type

    """
    Utils
    """

    @abstractmethod
    def save_document(self, id, contents):
        pass


    @abstractmethod
    def delete_document(self, id):
        pass


    @abstractmethod
    def get_document(self, id):
        pass


    @abstractmethod
    def search(self, q, limit=100, offset=0):
        pass
