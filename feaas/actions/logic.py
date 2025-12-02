import feaas.objects as objs
from feaas.abstract import AbstractAction

# This makes for some re-use of code, making the overall script smaller.

s_param = objs.Parameter(
    var_name='s',
    label='A String',
    ptype=objs.ParameterType.STRING)

i_param = objs.Parameter(
    var_name='val',
    label='An Integer',
    ptype=objs.ParameterType.INTEGER)

f_param = objs.Parameter(
    var_name='val',
    label='A Float',
    ptype=objs.ParameterType.FLOAT)

b_param = objs.Parameter(
    var_name='b',
    label='A Boolean',
    ptype=objs.ParameterType.BOOLEAN)

# TODO: OrAction - two BOOLEAN inputs, one BOOLEAN output
class OrAction(AbstractAction):
    def __init__(self, dao):
        a = objs.Parameter(
            var_name='a',
            label='a',
            ptype=objs.ParameterType.BOOLEAN)
        b = objs.Parameter(
            var_name='b',
            label='b',
            ptype=objs.ParameterType.BOOLEAN)
        params = [a, b]
        outputs = [b_param]
        super().__init__(params, outputs)

    def execute_action(self, a, b) -> objs.Receipt:
        result = a or b
        output = objs.AnyType(ptype=objs.ParameterType.BOOLEAN, bval=result)
        return objs.Receipt(success=True, primary_output='result', outputs={'result': output})


# TODO: AndAction - two BOOLEAN inputs, one BOOLEAN output
class AndAction(AbstractAction):
    def __init__(self, dao):
        a = objs.Parameter(
            var_name='a',
            label='a',
            ptype=objs.ParameterType.BOOLEAN)
        b = objs.Parameter(
            var_name='b',
            label='b',
            ptype=objs.ParameterType.BOOLEAN)
        params = [a, b]
        outputs = [b_param]
        super().__init__(params, outputs)

    def execute_action(self, a, b) -> objs.Receipt:
        result = a and b
        output = objs.AnyType(ptype=objs.ParameterType.BOOLEAN, bval=result)
        return objs.Receipt(success=True, primary_output='result', outputs={'result': output})



# TODO: XorAction - two BOOLEAN inputs, one BOOLEAN output
class XorAction(AbstractAction):
    def __init__(self, dao):
        a = objs.Parameter(
            var_name='a',
            label='a',
            ptype=objs.ParameterType.BOOLEAN)
        b = objs.Parameter(
            var_name='b',
            label='b',
            ptype=objs.ParameterType.BOOLEAN)
        params = [a, b]
        outputs = [b_param]
        super().__init__(params, outputs)

    def execute_action(self, a, b) -> objs.Receipt:
        result = a != b  # XOR operation
        output = objs.AnyType(ptype=objs.ParameterType.BOOLEAN, bval=result)
        return objs.Receipt(success=True, primary_output='result', outputs={'result': output})


# TODO: NotAction -  one BOOLEAN input, one BOOLEAN output
class NotAction(AbstractAction):
    def __init__(self, dao):
        a = objs.Parameter(
            var_name='a',
            label='a',
            ptype=objs.ParameterType.BOOLEAN)
        params = [a]
        outputs = [b_param]
        super().__init__(params, outputs)

    def execute_action(self, a) -> objs.Receipt:
        result = not a
        output = objs.AnyType(ptype=objs.ParameterType.BOOLEAN, bval=result)
        return objs.Receipt(success=True, primary_output='result', outputs={'result': output})

