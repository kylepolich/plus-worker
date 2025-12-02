from feaas.stream import util
import feaas.objects as objs


def load_registers(username, script, data):
    err_message = None
    status = objs.PlusScriptStatus.INITIALIZING
    registers = {}
    for k in data.keys():
        v = data[k]
        registers[f'mainInput:{k}'] = util.any_type_ifier(v)
        registers[f'{k}'] = util.any_type_ifier(v)

    for node in script.nodes:
        if node.ntype == objs.PlusScriptNodeType.STATIC:
            s = node.unique_id
            if len(node.outputs) == 0:
                # Used in mock.py
                var_name = "value"
            else:
                # Used in Plus
                var_name = node.outputs[0].var_name
            k = f'{s}:{var_name}'
            registers[k] = node.value

    for param in script.inputs:
        var_name = param.var_name
        reg_name = f'mainInput:{var_name}'
        if not(reg_name in registers.keys()):
            ptype = param.ptype
            any_type = util.get_param_default_as_any_type(param)
            if any_type is None:
                status = objs.PlusScriptStatus.FAILED
                err_message = f'Required parameter {var_name} not provided.'
            else:
                registers[reg_name] = any_type


    # TODO: make sure outputs are fulfilled too
    if len(script.nodes) == 0 and status == objs.PlusScriptStatus.INITIALIZING:
        status = objs.PlusScriptStatus.SUCCEEDED
    registers['username'] = objs.AnyType(ptype=objs.ParameterType.USERNAME, sval=username)
    # TODO: add hostname?
    return registers, status, err_message
