import feaas.objects as objs
from feaas.abstract import AbstractAction
import random
import string
import math


def poisson_sample(lam):
    L = math.exp(-lam)
    k = 0
    p = 1

    while p > L:
        k += 1
        p *= random.random()

    return k - 1


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
    label='An Integer',
    ptype=objs.ParameterType.FLOAT)

b_param = objs.Parameter(
    var_name='b',
    label='A Boolean',
    ptype=objs.ParameterType.BOOLEAN)


class RandomInt(AbstractAction):

    def __init__(self, dao):
        val_1 = objs.Parameter(
        var_name='val_1',
        label='Integer 1',
        ptype=objs.ParameterType.INTEGER)
        val_2 = objs.Parameter(
        var_name='val_2',
        label='Integer 2',
        ptype=objs.ParameterType.INTEGER)
        params = [val_1, val_2]
        outputs = [i_param]
        super().__init__(params, outputs)


    def execute_action(self, val_1, val_2) -> objs.Receipt:
        random_int = random.randint(val_1, val_2)
        output = objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=random_int)
        return objs.Receipt(success=True, primary_output='random_int', outputs={ 'random_int': output })


class RandomFloat(AbstractAction):

    def __init__(self, dao):
        min_val = objs.Parameter(
        var_name='min_val',
        label='Minimum value',
        ptype=objs.ParameterType.FLOAT)
        max_val = objs.Parameter(
        var_name='max_val',
        label='Maximum value',
        ptype=objs.ParameterType.FLOAT)
        params = [min_val, max_val]
        outputs = [f_param]
        super().__init__(params, outputs)


    def execute_action(self, min_val, max_val) -> objs.Receipt:
        random_float = random.uniform(min_val, max_val)
        output = objs.AnyType(ptype=objs.ParameterType.FLOAT, dval=random_float)
        return objs.Receipt(success=True, primary_output='random_float', outputs={ 'random_int': output })

class GaussianRandom(AbstractAction):
    def __init__(self, dao):
        mu = objs.Parameter(
            var_name='mu',
            label='Mean',
            ptype=objs.ParameterType.FLOAT)
        sigma = objs.Parameter(
            var_name='sigma',
            label='Standard Deviation',
            ptype=objs.ParameterType.FLOAT)
        params = [mu, sigma]
        outputs = [f_param]
        super().__init__(params, outputs)

    def execute_action(self, mu, sigma) -> objs.Receipt:
        gaussian_val = random.gauss(mu, sigma)
        output = objs.AnyType(ptype=objs.ParameterType.FLOAT, dval=gaussian_val)
        return objs.Receipt(success=True, primary_output='gaussian_val', outputs={'gaussian_val': output})


class PoissonRandom(AbstractAction):
    def __init__(self, dao):
        lam = objs.Parameter(
            var_name='lambda',
            label='Lambda',
            ptype=objs.ParameterType.FLOAT)
        params = [lam]
        outputs = [i_param]
        super().__init__(params, outputs)

    def execute_action(self, lam) -> objs.Receipt:
        poisson_val = poisson_sample(lam)
        output = objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=poisson_val)
        return objs.Receipt(success=True, primary_output='poisson_val', outputs={'poisson_val': output})



class StringRandom(AbstractAction):
    def __init__(self, dao):
        length = objs.Parameter(
            var_name='length',
            label='Length',
            ptype=objs.ParameterType.INTEGER)
        params = [length]
        outputs = [s_param]
        super().__init__(params, outputs)

    def execute_action(self, length) -> objs.Receipt:
        characters = string.ascii_letters + string.digits + "@#$%^&"
        random_string = ''.join(random.choice(characters) for _ in range(length))
        output = objs.AnyType(ptype=objs.ParameterType.STRING, sval=random_string)
        return objs.Receipt(success=True, primary_output='random_string', outputs={'random_string': output})

