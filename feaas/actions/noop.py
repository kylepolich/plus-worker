import feaas.objects as objs
from feaas.abstract import AbstractAction


class NoopSuccess(AbstractAction):

    def __init__(self, dao):
        params = []
        outputs = []
        super().__init__(params, outputs)


    def execute_action(self) -> objs.Receipt:
        return objs.Receipt(success=True)


class NoopFailure(AbstractAction):

    def __init__(self, dao):
        params = []
        outputs = []
        super().__init__(params, outputs)


    def execute_action(self) -> objs.Receipt:
        return objs.Receipt(success=False, error_message='This NOOP Action failed')
