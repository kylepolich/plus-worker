from abc import ABC, abstractmethod
import feaas.objects as objs


class AbstractAction(ABC):


    def __init__(self, params, outputs):
        sys_action_id = self.__module__ + '.' + self.__class__.__name__
        self.action = objs.Action(
            sys_action_id=sys_action_id,
            params=params,
            outputs=outputs,
        )


    @abstractmethod
    def execute_action(self, **kwargs) -> objs.Receipt:
        pass
