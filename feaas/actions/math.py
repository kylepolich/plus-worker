import feaas.objects as objs
from feaas.abstract import AbstractAction
import math

s_param = objs.Parameter(
    var_name='s',
    label='A String',
    ptype=objs.ParameterType.STRING)

i_param = objs.Parameter(
    var_name='val',
    label='An Integer',
    ptype=objs.ParameterType.INTEGER)

b_param = objs.Parameter(
    var_name='b',
    label='A Boolean',
    ptype=objs.ParameterType.BOOLEAN)

# TODO: Add - two FLOATS input `a` and `b`, returns a+b
class Add(AbstractAction):
    def __init__(self, dao):
        a = objs.Parameter(
            var_name='a',
            label='a',
            ptype=objs.ParameterType.FLOAT)
        b = objs.Parameter(
            var_name='b',
            label='b',
            ptype=objs.ParameterType.FLOAT)
        params = [a, b]
        sum = objs.Parameter(
            var_name='sum',
            label='Sum',
            ptype=objs.ParameterType.FLOAT)
        outputs = [sum]
        super().__init__(params, outputs)

    def execute_action(self, a, b) -> objs.Receipt:
        result = a + b
        output = objs.AnyType(ptype=objs.ParameterType.FLOAT, dval=result)
        return objs.Receipt(success=True, primary_output='sum', outputs={'sum': output})


class Subtract(AbstractAction):
    def __init__(self, dao):
        a = objs.Parameter(
            var_name='a',
            label='a',
            ptype=objs.ParameterType.FLOAT)
        b = objs.Parameter(
            var_name='b',
            label='b',
            ptype=objs.ParameterType.FLOAT)
        params = [a, b]
        difference = objs.Parameter(
            var_name='diff',
            label='Difference',
            ptype=objs.ParameterType.FLOAT)
        outputs = [difference]
        super().__init__(params, outputs)

    def execute_action(self, a, b) -> objs.Receipt:
        result = a - b
        output = objs.AnyType(ptype=objs.ParameterType.FLOAT, dval=result)
        return objs.Receipt(success=True, primary_output='difference', outputs={'difference': output})


# TODO: Multiply - two FLOATS input `a` and `b`, returns a*b

class Multiply(AbstractAction):
    def __init__(self, dao):
        a = objs.Parameter(
            var_name='a',
            label='a',
            ptype=objs.ParameterType.FLOAT)
        b = objs.Parameter(
            var_name='b',
            label='b',
            ptype=objs.ParameterType.FLOAT)
        params = [a, b]
        product = objs.Parameter(
            var_name='product',
            label='Product',
            ptype=objs.ParameterType.FLOAT)
        outputs = [product]
        super().__init__(params, outputs)

    def execute_action(self, a, b) -> objs.Receipt:
        result = a * b
        output = objs.AnyType(ptype=objs.ParameterType.FLOAT, dval=result)
        return objs.Receipt(success=True, primary_output='product', outputs={'product': output})


class Divide(AbstractAction):
    def __init__(self, dao):
        a = objs.Parameter(
            var_name='a',
            label='a',
            ptype=objs.ParameterType.FLOAT)
        b = objs.Parameter(
            var_name='b',
            label='b',
            ptype=objs.ParameterType.FLOAT)
        params = [a, b]
        quotient = objs.Parameter(
            var_name='quotient',
            label='Quotient',
            ptype=objs.ParameterType.FLOAT)
        outputs = [quotient]
        super().__init__(params, outputs)

    def execute_action(self, a, b) -> objs.Receipt:
        if b == 0:
            return objs.Receipt(success=False, error_message="Cannot divide by 0")
        result = a / b
        output = objs.AnyType(ptype=objs.ParameterType.FLOAT, dval=result)
        return objs.Receipt(success=True, primary_output='quotient', outputs={'quotient': output})




class Power(AbstractAction):
    def __init__(self, dao):
        a = objs.Parameter(
            var_name='a',
            label='a',
            ptype=objs.ParameterType.FLOAT)
        b = objs.Parameter(
            var_name='b',
            label='b',
            ptype=objs.ParameterType.FLOAT)
        params = [a, b]
        power = objs.Parameter(
            var_name='power',
            label='Power',
            ptype=objs.ParameterType.FLOAT)
        outputs = [power]
        super().__init__(params, outputs)

    def execute_action(self, a, b) -> objs.Receipt:
        result = a ** b
        output = objs.AnyType(ptype=objs.ParameterType.FLOAT, dval=result)
        return objs.Receipt(success=True, primary_output='power', outputs={'power': output})


class GreaterThan(AbstractAction):
    def __init__(self, dao):
        a = objs.Parameter(
            var_name='a',
            label='a',
            ptype=objs.ParameterType.FLOAT)
        b = objs.Parameter(
            var_name='b',
            label='b',
            ptype=objs.ParameterType.FLOAT)
        params = [a, b]
        outputs = [b_param]
        super().__init__(params, outputs)

    def execute_action(self, a, b) -> objs.Receipt:
        result = a > b
        output = objs.AnyType(ptype=objs.ParameterType.BOOLEAN, bval=result)
        return objs.Receipt(success=True, primary_output='result', outputs={'result': output})


class LessThan(AbstractAction):
    def __init__(self, dao):
        a = objs.Parameter(
            var_name='a',
            label='a',
            ptype=objs.ParameterType.FLOAT)
        b = objs.Parameter(
            var_name='b',
            label='b',
            ptype=objs.ParameterType.FLOAT)
        params = [a, b]
        outputs = [b_param]
        super().__init__(params, outputs)

    def execute_action(self, a, b) -> objs.Receipt:
        result = a < b
        output = objs.AnyType(ptype=objs.ParameterType.BOOLEAN, bval=result)
        return objs.Receipt(success=True, primary_output='result', outputs={'result': output})


class GreaterThanOrEqual(AbstractAction):
    def __init__(self, dao):
        a = objs.Parameter(
            var_name='a',
            label='a',
            ptype=objs.ParameterType.FLOAT)
        b = objs.Parameter(
            var_name='b',
            label='b',
            ptype=objs.ParameterType.FLOAT)
        params = [a, b]
        outputs = [b_param]
        super().__init__(params, outputs)

    def execute_action(self, a, b) -> objs.Receipt:
        result = a >= b
        output = objs.AnyType(ptype=objs.ParameterType.BOOLEAN, bval=result)
        return objs.Receipt(success=True, primary_output='result', outputs={'result': output})


class LessThanOrEqual(AbstractAction):
    def __init__(self, dao):
        a = objs.Parameter(
            var_name='a',
            label='a',
            ptype=objs.ParameterType.FLOAT)
        b = objs.Parameter(
            var_name='b',
            label='b',
            ptype=objs.ParameterType.FLOAT)
        params = [a, b]
        outputs = [b_param]
        super().__init__(params, outputs)

    def execute_action(self, a, b) -> objs.Receipt:
        result = a <= b
        output = objs.AnyType(ptype=objs.ParameterType.BOOLEAN, bval=result)
        return objs.Receipt(success=True, primary_output='result', outputs={'result': output})


class Equals(AbstractAction):
    def __init__(self, dao):
        a = objs.Parameter(
            var_name='a',
            label='a',
            ptype=objs.ParameterType.FLOAT)
        b = objs.Parameter(
            var_name='b',
            label='b',
            ptype=objs.ParameterType.FLOAT)
        params = [a, b]
        outputs = [b_param]
        super().__init__(params, outputs)

    def execute_action(self, a, b) -> objs.Receipt:
        result = a == b
        output = objs.AnyType(ptype=objs.ParameterType.BOOLEAN, bval=result)
        return objs.Receipt(success=True, primary_output='result', outputs={'result': output})


class Floor(AbstractAction):
    def __init__(self, dao):
        a = objs.Parameter(
            var_name='a',
            label='a',
            ptype=objs.ParameterType.FLOAT)
        params = [a]
        outputs = [i_param]
        super().__init__(params, outputs)

    
    def execute_action(self, a) -> objs.Receipt:
        result = math.floor(a)
        output = objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=result)
        return objs.Receipt(success=True, primary_output='floor', outputs={'floor': output})



class Ceiling(AbstractAction):
    def __init__(self, dao):
        a = objs.Parameter(
            var_name='a',
            label='a',
            ptype=objs.ParameterType.FLOAT)
        params = [a]
        outputs = [i_param]
        super().__init__(params, outputs)

    def execute_action(self, a) -> objs.Receipt:
        result = math.ceil(a)
        output = objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=result)
        return objs.Receipt(success=True, primary_output='ceiling', outputs={'ceiling': output})


class Round(AbstractAction):
    def __init__(self, dao):
        a = objs.Parameter(
            var_name='a',
            label='a',
            ptype=objs.ParameterType.FLOAT)
        params = [a]
        outputs = [i_param]
        super().__init__(params, outputs)

    def execute_action(self, a) -> objs.Receipt:
        result = round(a)
        output = objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=result)
        return objs.Receipt(success=True, primary_output='result', outputs={'result': output})
