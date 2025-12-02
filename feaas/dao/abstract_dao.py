from abc import ABC, abstractmethod
import furl
import hashlib


class AbstractDao(ABC):


    def __init__(self):
        super().__init__()

