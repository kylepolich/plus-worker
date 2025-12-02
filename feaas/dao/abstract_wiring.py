from abc import  abstractmethod
from feaas.dao import abstract_dao


"""A collection of services which are likely to be provided by a variety
of service providers
"""
class Wiring(abstract_dao.AbstractDao):

    def __init__(self):
        super().__init__()
