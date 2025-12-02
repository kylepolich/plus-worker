from abc import  abstractmethod
from feaas.dao import abstract_wiring as wiring


class AbstractQueue(wiring.Wiring):


    def __init__(self):
        super().__init__()


    def add_message(self, msg, delay_seconds=0) -> str:
        return self._add_message(msg, delay_seconds)


    def delete_message(self, receipt_handle):
        return self._delete_message(receipt_handle)


    def get_message(self):
        return self._get_message()


    @abstractmethod
    def _add_message(self, msg, delay_seconds):
        pass


    @abstractmethod
    def _delete_message(self, receipt_handle):
        pass


    @abstractmethod
    def _get_message(self):
        pass


    @abstractmethod
    def get_size(self):
        pass
