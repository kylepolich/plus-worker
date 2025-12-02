import feaas.objects as objs
from feaas.abstract import AbstractAction

class StringComparison(AbstractAction):

    def __init__(self, dao):
        needle = objs.Parameter(
            var_name='needle',
            label='needle',
            ptype=objs.ParameterType.STRING)        
        haystack = objs.Parameter(
            var_name='haystack',
            label='haystack',
            ptype=objs.ParameterType.STRING)        
        params = [needle, haystack]
        
        found = objs.Parameter(
            var_name='found',
            label='found',
            ptype=objs.ParameterType.BOOLEAN)
        first_index = objs.Parameter(
            var_name='first_index',
            label='first_index',
            ptype=objs.ParameterType.INTEGER)
        outputs = [found, first_index]
        super().__init__(params, outputs)

    def execute_action(self, needle, haystack) -> objs.Receipt:
        index = haystack.find(needle)
        found = index != -1

        outputs = {
            "found": objs.AnyType(ptype=objs.ParameterType.BOOLEAN, bval=found),
            "first_index": objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=index)
            }
        
        return objs.Receipt(success=True, primary_output='found', outputs=outputs)