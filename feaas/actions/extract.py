import feaas.objects as objs
from feaas.abstract import AbstractAction

class ExtractQuerystringValue(AbstractAction):

    def __init__(self, dao):
        querystring = objs.Parameter(
            var_name='querystring',
            label='querystring',
            ptype=objs.ParameterType.STRING)        
        name = objs.Parameter(
            var_name='name',
            label='name',
            ptype=objs.ParameterType.STRING)        
        default_value = objs.Parameter(
            var_name='default_value',
            label='default_value',
            ptype=objs.ParameterType.STRING)        
        params = [querystring, name, default_value]
        value = objs.Parameter(
            var_name='value',
            label='value',
            ptype=objs.ParameterType.STRING)
        outputs = [value]
        super().__init__(params, outputs)

    def execute_action(self, querystring, name, default_value) -> objs.Receipt:
        # Remove leading '?' if present
        if querystring.startswith('?'):
            querystring = querystring[1:]
            
        # Split into key-value pairs
        pairs = querystring.split('&')
        for pair in pairs:
            if '=' in pair:
                key, value = pair.split('=', 1)
                if key == name:
                    output_value = value
                    break
        else:
            output_value = default_value

        outputs = {
            "value": objs.AnyType(ptype=objs.ParameterType.STRING, sval=output_value)
        }
        
        return objs.Receipt(success=True, primary_output='value', outputs=outputs)