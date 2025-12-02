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

d_param = objs.Parameter(
    var_name='val',
    label='A Float',
    ptype=objs.ParameterType.FLOAT)

b_param = objs.Parameter(
    var_name='b',
    label='A Boolean',
    ptype=objs.ParameterType.BOOLEAN)

# TODO: In all conversions to numeric, first trim whitespace, commas, and dollar sign before converting        
# TODO: StringToInteger - takes STRING, returns INTEGER, fails if can't convert

class StringToInteger(AbstractAction):
    def __init__(self, dao):
        str_input = objs.Parameter(
            var_name='str_input',
            label='Input String',
            ptype=objs.ParameterType.STRING)
        params = [str_input]
        outputs = [i_param]
        super().__init__(params, outputs)

    def execute_action(self, str_input) -> objs.Receipt:
        cleaned_input = str_input.strip().replace(',', '').replace('$', '')
        try:
            integer_value = int(cleaned_input)
            output = objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=integer_value)
            return objs.Receipt(success=True, primary_output='integer_value', outputs={'integer_value': output})
        except ValueError:
            return objs.Receipt(success=False, error_message='Conversion failed')
        

# TODO: StringToFloat - takes STRING, returns INTEGER, fails if can't convert. If % is present, handle appropriately by removing and dividing by 100
class StringToFloat(AbstractAction):
    def __init__(self, dao):
        str_input = objs.Parameter(
            var_name='str_input',
            label='Input String',
            ptype=objs.ParameterType.STRING)
        params = [str_input]
        outputs = [d_param]
        super().__init__(params, outputs)

    def execute_action(self, str_input) -> objs.Receipt:
        cleaned_input = str_input.strip().replace(',', '').replace('$', '').replace('%', '')
        try:
            float_value = float(cleaned_input)
            if '%' in str_input:
                float_value /= 100
            output = objs.AnyType(ptype=objs.ParameterType.FLOAT, dval=float_value)
            return objs.Receipt(success=True, primary_output='float_value', outputs={'float_value': output})
        except ValueError:
            return objs.Receipt(success=False, error_message='Conversion failed')

    
# TODO: IntegerToFloat
class IntegerToFloat(AbstractAction):
    def __init__(self, dao):
        int_input = objs.Parameter(
            var_name='int_input',
            label='Integer Input',
            ptype=objs.ParameterType.INTEGER)
        params = [int_input]
        outputs = [d_param]
        super().__init__(params, outputs)

    def execute_action(self, int_input) -> objs.Receipt:
        float_value = float(int_input)
        output = objs.AnyType(ptype=objs.ParameterType.FLOAT, dval=float_value)
        return objs.Receipt(success=True, primary_output='float_value', outputs={'float_value': output})

# TODO: IntegerToString
class IntegerToString(AbstractAction):
    def __init__(self, dao):
        int_input = objs.Parameter(
            var_name='int_input',
            label='Integer Input',
            ptype=objs.ParameterType.INTEGER)
        params = [int_input]
        outputs = [s_param]
        super().__init__(params, outputs)

    def execute_action(self, int_input) -> objs.Receipt:
        string_value = str(int_input)
        output = objs.AnyType(ptype=objs.ParameterType.STRING, sval=string_value)
        return objs.Receipt(success=True, primary_output='string_value', outputs={'string_value': output})
    

# TODO: FloatToString
class FloatToString(AbstractAction):
    def __init__(self, dao):
        float_input = objs.Parameter(
            var_name='float_input',
            label='Float Input',
            ptype=objs.ParameterType.FLOAT)
        params = [float_input]
        outputs = [s_param]
        super().__init__(params, outputs)

    def execute_action(self, float_input) -> objs.Receipt:
        string_value = str(float_input)
        output = objs.AnyType(ptype=objs.ParameterType.STRING, sval=string_value)
        return objs.Receipt(success=True, primary_output='string_value', outputs={'string_value': output})
    

class StringToInt(AbstractAction):
    def __init__(self, dao):
        input_val = objs.Parameter(
            var_name='input_val',
            label='Input Value',
            ptype=objs.ParameterType.STRING)
        default_value = objs.Parameter(
            var_name='default_value',
            label='Default Value',
            ptype=objs.ParameterType.INTEGER)
        params = [input_val, default_value]
        outputs = [i_param]
        super().__init__(params, outputs)

    def execute_action(self, input_val, default_value) -> objs.Receipt:
        try:
            int_value = int(input_val.strip())
        except (ValueError, TypeError):
            int_value = default_value
            
        output = objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=int_value)
        return objs.Receipt(success=True, primary_output='integer_value', outputs={'integer_value': output})
