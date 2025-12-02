import feaas.objects as objs
import time
import os


def resolve_default_variables(v: str, username: str):
    if type(v) == str:
        return v.replace('{username}', f'{username}')
    else:
        return v


def get_feed_id(stream_name: str) -> str:
    return f'stream|{stream_name}'


def combine_receipts(timestamp, receipts):
    if len(receipts) == 0:
        return objs.Receipt(
            success=False,
            error_message="Nothing to combine",
            outputs={},
            primary_output=None)
    elif len(receipts) == 1:
        return receipts[0]
    else:
        outputs = {}
        primary_output = None
        total_cost = 0
        success = True
        error_messages = []
        primary_output = ''
        for receipt in receipts:
            if receipt is not None:
                success = success and receipt.success
                if receipt.error_message != "":
                    error_messages.append(receipt.error_message)
                po = receipt.primary_output
                if primary_output == '' and not(po is None or po.strip() == ''):
                    primary_output = po
                total_cost += receipt.cost
                for key in receipt.outputs.keys():
                    outputs[key] = receipt.outputs[key]
        return objs.Receipt(
            success=success,
            error_message="; ".join(error_messages),
            outputs=outputs,
            primary_output=primary_output,
            cost=total_cost,
            timestamp=int(time.time() * 1000))


# def rollbar_exception(tb):
#     pass

# def rollbar_handled_error_record_message(msg: str):
#     rollbar_token = os.getenv('ROLLBAR_TOKEN', "").strip()
#     if rollbar_token:
#         import rollbar
#         rollbar.init(rollbar_token)
#         rollbar.report_message(msg)

def any_type_resolved(any_type):
    ptype = any_type.ptype
    if ptype is None:
        ptype = objs.ParameterType.STRING
    #
    if ptype == objs.ParameterType.RECEIPT:
        receipt = objs.Receipt()
        receipt.ParseFromString(any_type.byval)
        return receipt
    if ptype == objs.ParameterType.STRING:
        return any_type.sval
    elif ptype == objs.ParameterType.ACE:
        return any_type.sval
    elif ptype == objs.ParameterType.BYTES:
        return any_type.byval
    elif ptype == objs.ParameterType.HIDDEN:
        return any_type.sval
    elif ptype == objs.ParameterType.BOOLEAN:
        return any_type.bval
    elif ptype == objs.ParameterType.INTEGER:
        return any_type.ival
    elif ptype == objs.ParameterType.FLOAT:
        return any_type.dval
    elif ptype == objs.ParameterType.KEY:
        return any_type.sval
    elif ptype == objs.ParameterType.PREFIX:
        return any_type.sval
    elif ptype == objs.ParameterType.FIXED_LIST_SINGLE_SELECT:
        return any_type.sval
    elif ptype == objs.ParameterType.FIXED_LIST_MULTI_SELECT:
        return any_type.svals
    elif ptype == objs.ParameterType.URL:
        return any_type.sval
    elif ptype == objs.ParameterType.US_CURRENCY_AMT:
        return any_type.dval
    elif ptype == objs.ParameterType.IP_ADDRESS:
        return any_type.sval
    elif ptype == objs.ParameterType.LIST:
        lst = []
        for item in any_type.svals:
            lst.append(str(item))
        return lst
    elif ptype == objs.ParameterType.CRON:
        return any_type.sval
    elif ptype == objs.ParameterType.OBJECT_ID:
        return any_type.sval
    elif ptype == objs.ParameterType.OWNER:
        return any_type.sval
    elif ptype == objs.ParameterType.UNIQUE_ID:
        return any_type.sval
    elif ptype == objs.ParameterType.JOB_ID:
        return any_type.sval
    elif ptype == objs.ParameterType.VECTOR:
        arr = []
        for item in any_type.dvals:
            arr.append(item)
        return arr
    elif ptype == objs.ParameterType.FEED_ID:
        return any_type.sval
    elif ptype == objs.ParameterType.ACTION_ID:
        return any_type.sval
    elif ptype == objs.ParameterType.DATETIME:
        return any_type.ival      # YUP!  That's right.  EPOCH under the hood!
    elif ptype == objs.ParameterType.COPY:
        return any_type.sval
    elif ptype == objs.ParameterType.STRING_MAP:
        return any_type.smap
    elif ptype == objs.ParameterType.USERNAME:
        return any_type.sval
    elif ptype == objs.ParameterType.HOSTNAME:
        return any_type.sval
    elif ptype == objs.ParameterType.ANY_TYPE_MAP:
        return any_type.mapping
    elif ptype == objs.ParameterType.CSV_COLUMN:
        return any_type.sval
    elif ptype == objs.ParameterType.STREAM_PARAM:
        return any_type.sval
    elif ptype == objs.ParameterType.DISPLAY_ONLY:
        return None
    else:
        raise Exception(f'No defined process to resolve Parameter Type {objs.ParameterType.Name(ptype)}')


def any_type_ifier(v) -> objs.AnyType:
    if isinstance(v, objs.Receipt):
        reg = objs.AnyType()
        reg.ptype = objs.ParameterType.RECEIPT
        reg.byval = v.SerializeToString()
        return reg
        
    try:
        i = int(v)
        if str(i) == str(v):
            return objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=i)
    except:
        pass
    try:
        i = float(v)
        return objs.AnyType(ptype=objs.ParameterType.FLOAT, dval=i)
    except:
        pass
    if type(v) == list:
        arr = []
        for item in v:
            arr.append(str(item))
        return objs.AnyType(ptype=objs.ParameterType.LIST, svals=arr)
    return objs.AnyType(ptype=objs.ParameterType.STRING, sval=str(v))


def get_default_value_from_param(param):
    if param.var_name == 'timestamp':
        return int(time.time() * 1000)
    at = get_param_default_as_any_type(param)
    if at is None:
        return None
    return any_type_resolved(at)


def handle_integer_value(param) -> objs.AnyType:
        if not(param.optional) or param.idefault != 0:
            return objs.AnyType(ptype=param.ptype, ival=param.idefault)
        else:
            return None


def handle_float_value(param) -> objs.AnyType:
        if not(param.optional) or param.ddefault != 0.0:
            return objs.AnyType(ptype=param.ptype, dval=param.ddefault)
        else:
            return None


def handle_string_value(param) -> objs.AnyType:
        if not(param.optional) or param.sdefault != "":
            return objs.AnyType(ptype=param.ptype, sval=param.sdefault.strip())
        else:
            return None


def get_param_default_as_any_type(param) -> objs.AnyType:
    ptype = param.ptype
    sdefault = param.sdefault
    if sdefault.strip() == '':
        sdefault = None
    if ptype == objs.STRING:
        return handle_string_value(param)
    elif ptype == objs.BOOLEAN:
        return objs.AnyType(ptype=ptype, bval=param.bdefault)
    elif ptype == objs.INTEGER:
        return handle_integer_value(param)
    elif ptype == objs.FLOAT:
        return handle_float_value(param)
    elif ptype == objs.KEY:
        handle_string_value(param)
    elif ptype == objs.PREFIX:
        handle_string_value(param)
    elif ptype == objs.FIXED_LIST_SINGLE_SELECT:
        handle_string_value(param)
    elif ptype == objs.FIXED_LIST_MULTI_SELECT:
        return objs.AnyType(ptype=ptype, svals=param.svals)
    elif ptype == objs.URL:
        handle_string_value(param)
    elif ptype == objs.US_CURRENCY_AMT:
        return handle_float_value(param)
    elif ptype == objs.IP_ADDRESS:
        handle_string_value(param)
    elif ptype == objs.LIST:
        return objs.AnyType(ptype=ptype, svals=param.svals)
    elif ptype == objs.CRON:
        handle_string_value(param)
    elif ptype == objs.OBJECT_ID:
        handle_string_value(param)
    elif ptype == objs.UNIQUE_ID:
        handle_string_value(param)
    elif ptype == objs.JOB_ID:
        handle_string_value(param)
    # elif ptype == objs.VECTOR:
    #     return objs.AnyType(ptype=ptype, dvals=param.dvals)
    elif ptype == objs.FEED_ID:
        return handle_string_value(param)
    elif ptype == objs.DATETIME:
        return handle_integer_value(param) # YUP!  That's right.  EPOCH under the hood!
    else:
        return handle_string_value(param)


def make_parameter_given_any_type(var_name, ptype, val):
    if ptype == objs.ParameterType.STRING:
        return objs.Parameter(var_name=var_name, ptype=ptype, sdefault=val.sval)
    elif ptype == objs.ParameterType.BOOLEAN:
        return objs.Parameter(var_name=var_name, ptype=ptype, bdefault=val.bval)
    elif ptype == objs.ParameterType.INTEGER:
        return objs.Parameter(var_name=var_name, ptype=ptype, idefault=val.ival)
    elif ptype == objs.ParameterType.FLOAT:
        return objs.Parameter(var_name=var_name, ptype=ptype, ddefault=val.dval)
    elif ptype == objs.ParameterType.KEY:
        return objs.Parameter(var_name=var_name, ptype=ptype, sdefault=val.sval)
    elif ptype == objs.ParameterType.PREFIX:
        return objs.Parameter(var_name=var_name, ptype=ptype, sdefault=val.sval)
    elif ptype == objs.ParameterType.FIXED_LIST_SINGLE_SELECT:
        return objs.Parameter(var_name=var_name, ptype=ptype, svals=val.svals)
    elif ptype == objs.ParameterType.FIXED_LIST_MULTI_SELECT:
        return objs.Parameter(var_name=var_name, ptype=ptype, svals=val.svals)
    elif ptype == objs.ParameterType.URL:
        return objs.Parameter(var_name=var_name, ptype=ptype, sdefault=val.sval)
    elif ptype == objs.ParameterType.US_CURRENCY_AMT:
        return objs.Parameter(var_name=var_name, ptype=ptype, ddefault=val.dval)
    elif ptype == objs.ParameterType.IP_ADDRESS:
        return objs.Parameter(var_name=var_name, ptype=ptype, sdefault=val.sval)
    elif ptype == objs.ParameterType.LIST:
        return objs.Parameter(var_name=var_name, ptype=ptype, svals=val.svals)
    elif ptype == objs.ParameterType.CRON:
        return objs.Parameter(var_name=var_name, ptype=ptype, sdefault=val.sval)
    elif ptype == objs.ParameterType.OBJECT_ID:
        return objs.Parameter(var_name=var_name, ptype=ptype, sdefault=val.sval)
    elif ptype == objs.ParameterType.UNIQUE_ID:
        return objs.Parameter(var_name=var_name, ptype=ptype, sdefault=val.sval)
    elif ptype == objs.ParameterType.JOB_ID:
        return objs.Parameter(var_name=var_name, ptype=ptype, sdefault=val.sval)
    # elif ptype == objs.ParameterType.VECTOR:
    elif ptype == objs.ParameterType.FEED_ID:
        return objs.Parameter(var_name=var_name, ptype=ptype, sdefault=val.sval)
    elif ptype == objs.ParameterType.DATETIME:
        return objs.Parameter(var_name=var_name, ptype=ptype, idefault=val.ival) # YUP!  That's right.  EPOCH under the hood!
    else:
        return objs.Parameter(var_name=var_name, ptype=ptype, sdefault=val.sval)


def make_any_type(ptype, val):
    if ptype == objs.ParameterType.STRING:
        return objs.AnyType(ptype=ptype, sval=str(val))
    elif ptype == objs.ParameterType.BOOLEAN:
        return objs.AnyType(ptype=ptype, bval=bool(val))
    elif ptype == objs.ParameterType.INTEGER:
        return objs.AnyType(ptype=ptype, ival=int(val))
    elif ptype == objs.ParameterType.FLOAT:
        return objs.AnyType(ptype=ptype, dval=float(val))
    elif ptype == objs.ParameterType.KEY:
        return objs.AnyType(ptype=ptype, sval=str(val))
    elif ptype == objs.ParameterType.PREFIX:
        return objs.AnyType(ptype=ptype, sval=str(val))
    elif ptype == objs.ParameterType.FIXED_LIST_SINGLE_SELECT:
        return objs.AnyType(ptype=ptype, svals=val)
    elif ptype == objs.ParameterType.FIXED_LIST_MULTI_SELECT:
        return objs.AnyType(ptype=ptype, svals=val)
    elif ptype == objs.ParameterType.URL:
        return objs.AnyType(ptype=ptype, sval=str(val))
    elif ptype == objs.ParameterType.US_CURRENCY_AMT:
        return objs.AnyType(ptype=ptype, dval=val.dval)
    elif ptype == objs.ParameterType.IP_ADDRESS:
        return objs.AnyType(ptype=ptype, sval=str(val))
    elif ptype == objs.ParameterType.LIST:
        return objs.AnyType(ptype=ptype, svals=val)
    elif ptype == objs.ParameterType.CRON:
        return objs.AnyType(ptype=ptype, sval=str(val))
    elif ptype == objs.ParameterType.OBJECT_ID:
        return objs.AnyType(ptype=ptype, sval=str(val))
    elif ptype == objs.ParameterType.UNIQUE_ID:
        return objs.AnyType(ptype=ptype, sval=str(val))
    elif ptype == objs.ParameterType.JOB_ID:
        return objs.AnyType(ptype=ptype, sval=str(val))
    # elif ptype == objs.ParameterType.VECTOR:
    elif ptype == objs.ParameterType.FEED_ID:
        return objs.AnyType(ptype=ptype, sval=str(val))
    elif ptype == objs.ParameterType.DATETIME:
        return objs.AnyType(ptype=ptype, ival=int(val)) # YUP!  That's right.  EPOCH under the hood!
    elif ptype == objs.ParameterType.VECTOR:
        return objs.AnyType(ptype=ptype, dvals=val)
    else:
        return objs.AnyType(ptype=ptype, sval=str(val))


def get_params_from_all_pages(oc):
    all_params = []
    has_stream_id = False
    has_timestamp = False
    for page in oc.pages:
        for param in page.params:
            all_params.append(param)
    return all_params


def sns_to_stream_event(stream_id, ts, event_type, message_json):
    data = {}
    for k in message_json.keys():
        v = message_json[k]
        data[k] = any_type_ifier(v)
    return objs.StreamEvent(
        stream_id=stream_id,
        timestamp=ts,
        etype=event_type,
        data=data)