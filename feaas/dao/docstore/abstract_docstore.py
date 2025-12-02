from abc import  abstractmethod
from feaas.dao import abstract_wiring as wiring


class AbstractDocstore(wiring.Wiring):


    def __init__(self):
        super().__init__()


    @abstractmethod
    def get_document(self, key, initialize=False):
        pass


    @abstractmethod
    def save_document(self, key, contents):
        pass


    @abstractmethod
    def delete_document(self, key):
        pass


    @abstractmethod
    def update_document(self, key, contents):
        pass


    @abstractmethod
    def search(self, pattern):
        pass


    @abstractmethod
    def increment_counter(self, name, amount=1):
        pass


    # @abstractmethod
    # def add_to_set(self, name, item: str):
    #     pass


    # @abstractmethod
    # def remove_from_set(self, name, item: str):
    #     pass


    def get_dockey(self, application_name, item_type, key):
        return f'{application_name}__{item_type}__{key}'


