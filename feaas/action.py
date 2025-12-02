# from abc import ABC, abstractmethod
# import feaas.objects as objs


# class AbstractAction(ABC):


#     def __init__(self, sys_name, params, outputs):
#         action_id = self.__module__ + '.' + self.__class__.__name__
#         self.action = objs.Action(
#             sys_name=sys_name,
#             src_action_id = action_id,
#             params=params,
#             outputs=outputs)


#     @abstractmethod
#     def execute_action(self, **kwargs) -> objs.Receipt:
#         pass
