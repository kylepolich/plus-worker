from abc import ABC, abstractmethod
import feaas.objects as objs


class AbstractMatcher(ABC):


    def __init__(self):
        pass


    @abstractmethod
    def matches(self, primary_input: str, input_dict: dict) -> bool:
        pass
