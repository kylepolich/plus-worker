import boto3
from collections.abc import Mapping, Iterable
from collections import defaultdict
import feaas.objects as objs
from feaas.stream import util
import copy
from datetime import datetime
from decimal import Decimal
import furl
from google.protobuf.json_format import MessageToDict
import hashlib
from importlib import import_module
from io import BytesIO, StringIO
import json
import os
from pathlib import Path
import time
import traceback
from urllib.parse import urlparse


class DecimalEncoder(json.JSONEncoder):
    def __init__(self, *args, **kwargs):
        self._sort_keys = kwargs.get("sort_keys", False)
        super().__init__(*args, **kwargs)

    def encode(self, obj):
        if isinstance(obj, boto3.dynamodb.types.Binary):
            return None

        if isinstance(obj, Mapping):
            items = obj.items()
            if self._sort_keys:
                items = sorted(items, key=lambda kv: kv[0])
            return '{' + ', '.join(f'{self.encode(k)}: {self.encode(v)}' for (k, v) in items) + '}'
        
        if isinstance(obj, Iterable) and (not isinstance(obj, str)):
            return '[' + ', '.join(map(self.encode, obj)) + ']'

        if isinstance(obj, Decimal):
            return f'{obj.normalize():f}'  # using normalize() gets rid of trailing 0s, using ':f' prevents scientific notation
        
        if isinstance(obj, bytes):
            return obj.decode('utf-8')  # or however you want to serialize bytes
        
        return super().encode(obj)


def decimal_default(o):
    if isinstance(o, Decimal):
        return float(o)
    raise TypeError


def strip_bytes_fields(obj):
    if isinstance(obj, dict):
        obj.pop('byval', None)
        for v in obj.values():
            strip_bytes_fields(v)
    elif isinstance(obj, list):
        for i in obj:
            strip_bytes_fields(i)


def get_request_from_string(class_full_name: str):
    module_path, class_name = (class_full_name + '.Request').rsplit('.', 1)
    module = import_module(module_path)
    RequestClass = getattr(module, class_name)
    return RequestClass


def clean_json_arr(jarr):
    result = []
    for item in jarr:
        clean = clean_json_dict(item)
        result.append(clean)
    return result


def handle_json(message_json):
    event_type = objs.StreamEvent.EventType.GENERIC
    if 'timestamp' in message_json:
        ts = message_json['timestamp']
        del message_json['timestamp']
    else:
        ts = int(time.time() * 1000)
    if 'stream_id' in message_json:
        stream_id = message_json['stream_id']
        del message_json['stream_id']
    else:
        stream_id = 'sys.error.missing_stream_id'
    data = {}
    for k in message_json.keys():
        v = message_json[k]
        data[k] = util.any_type_ifier(v)
    return objs.StreamEvent(
        stream_id=stream_id,
        timestamp=ts,
        etype=event_type,
        data=data)


def recursive_delete(directory):
    if os.path.isdir(directory):
        directory = Path(directory)
        for item in directory.iterdir():
            if item.is_dir():
                recursive_delete(item)
            else:
                item.unlink()


def clean_json_dict(jobj):
    if type(jobj) == int:
        return jobj
    if type(jobj).__name__ == 'int64':
        return int(jobj)
    if type(jobj).__name__ == 'float64':
        jobj = float(jobj)
        return Decimal(str(jobj))
    if type(jobj).__name__ == 'bool_':
        return bool(jobj)
    if type(jobj) == str:
        return jobj
    if type(jobj) == float:
        return Decimal(str(jobj))
    if type(jobj) == Decimal:
        return jobj
    if type(jobj) == bool:
        return jobj
    if type(jobj) == datetime:
        return str(jobj)
    if type(jobj) == bytes:
        return jobj
    if jobj is None:
        return None
    #
    result = {}
    if type(jobj) == list:
        return clean_json_arr(jobj)
    if type(jobj) == bool:
        jobj = bool(jobj)
    elif type(jobj) != dict:
        jobj = jobj.__dict__
    for col in jobj.keys():
        v = jobj[col]
        result[col] = clean_json_dict(v)
    return result


def match(first, second):
    # If we reach at the end of both strings, we are done
    if len(first) == 0 and len(second) == 0:
        return True
    # Make sure that the characters after '*' are present
    # in second string. This function assumes that the first
    # string will not contain two consecutive '*'
    if len(first) > 1 and first[0] == '*' and  len(second) == 0:
        return False
    # If the first string contains '?', or current characters
    # of both strings match
    if (len(first) > 1 and first[0] == '?') or (len(first) != 0
        and len(second) != 0 and first[0] == second[0]):
        return match(first[1:],second[1:]);
    # If there is *, then there are two possibilities
    # a) We consider current character of second string
    # b) We ignore current character of second string.
    if len(first) !=0 and first[0] == '*':
        return match(first[1:],second) or match(first,second[1:])
    return False


def dataframe2parquet_bytes(df):
    raise Exception("Use local branch instead")
    if df is None or df.shape[0] == 0:
        return None
    d = dict(zip(df.columns, df.dtypes))
    for k in d.keys():
        v = d[k].__class__.__name__
        if v == 'float64':
            df[k].fillna(0, inplace=True)
        elif v == 'bool':
            df[k] = df[k].astype(int)
        else:
            df[k] = df[k].fillna('')
    try:
        out_buffer = BytesIO()
        df.to_parquet(out_buffer, engine='pyarrow')
    except:
        # TODO: fix this awful technical debt
        print('try harder')
        out_buffer = _try_harder(df)
    out_buffer.seek(0)
    bytez = out_buffer.read()
    return bytez


def dataframe2csv(df):
    out_buffer = StringIO()
    df.to_csv(out_buffer, index=False)
    out_buffer.seek(0)
    csv = out_buffer.read()
    return csv


def _try_harder(df):
    out_buffer = BytesIO()
    for k in df.keys():
        df[k] = df[k].astype(str).fillna('')
    df.to_parquet(out_buffer, engine='pyarrow')
    return out_buffer


def get_domain(uri, furl_dict=None):
    if uri.find('://') == -1:
        uri = f'http://{uri}'
    if furl_dict is None:
        try:
            furl_dict = furl.furl(uri).asdict()
        except:
            raise Exception(f'Invalid URL: {uri}')
    domain = furl_dict['host']
    if domain is None:
        raise Exception(f'Invalid URL: {uri}')
    if domain.startswith('www.'):
        domain = domain[4:]
    arr = domain.split('.')
    arr.reverse()
    return '.'.join(arr)


def get_extension(key, ext='.unknown'):
    if key is None:
        return None
    # h = key.find('://')
    # if h != -1:
    #     return ext
    i = key.rfind('/') + 1
    j = key.find('.', i)
    k = key.find('@', i)
    if k != -1:
        k = key.find('.com', k)
        if k != -1:
            j = key.find('.', k+1)
    if j == -1:
        return ext
    ext = key[j:]
    return ext.lower()


def get_prefix(key):
    if key is None:
        return None
    i = key.rfind('/') + 1
    return key[0:i]


def get_stem(key):
    return Path(key).stem


def get_username(key):
    return key.split('/')[1]


def get_name(key):
    return Path(key).name


def get_family_prefix(key):
    i = key.rfind('/') + 1
    j = key.find('.', i)
    if j == -1:
        return None
    return key[0:j]


def change_extension(key, ext):
    ext2 = get_extension(key)
    i = key.rfind(ext2)
    return key[0:i] + ext


def get_filename_from_url(url):
    a = urlparse(url)
    p = a.path  # '/some/long/path/a_filename.jpg'
    name = Path(p).name
    ext = get_extension(p)
    if name == '':
        name = 'index'
    if name.endswith(ext):
        return name
    else:
        return name + ext


def get_filename_from_key(key):
    i = key.rfind('/') + 1
    ext = get_extension(key)
    return key[i:]


def extension_match(ext, ext_pattern):
    if ext_pattern == '.*':
        return True
    if ext == ext_pattern:
        return True
    if ext.endswith(ext_pattern):
        return True
    return False


def filter_dict(old_dict, your_keys, default_value=None, exclude=True):
    return { 
        your_key: old_dict.get(your_key, default_value) 
        for your_key in your_keys 
        if (your_key in old_dict) and exclude
    }


def defaultdict_to_regulardict(d):
    if isinstance(d, defaultdict):
        d = {k: defaultdict_to_regulardict(v) for k, v in d.items()}
    return d


def get_dicts_from_receipts(receipts) -> list:
    out = []
    for receipt in receipts:
        out.append(receipt.__dict__)
    return out


def build_request(class_full_name: str, kwargs: dict):
    try:
        module_path, class_name = class_full_name.rsplit('.', 1)
        module = import_module(module_path)
        Request = getattr(module, class_name)
    except (ImportError, AttributeError) as e:
        print('Import error:', class_full_name)
        raise ImportError(class_full_name)
    r = Request(**kwargs)
    return r


def work_from_dict(work_dict):
    class_full_name = work_dict['type']
    # TODO: need a better system.  This is "hidden" to a new dev
    work = build_request(class_full_name, work_dict)
    return work


def handler_from_dict(work_dict: dict, app_manager):
    if 'parser_name' in work_dict:
        class_full_name = work_dict['parser_name']
    else:
        class_full_name = work_dict['type']
        kwargs = work_dict
    if class_full_name.endswith('.Request'):
        class_full_name = class_full_name[0:-8] + '.Handler'
        kwargs = copy.deepcopy(work_dict)
        kwargs['app_manager'] = app_manager
    handler = build_request(class_full_name, kwargs)
    return handler


def get_es_doc_address(application_name, extension, dest_key):
    return {
        "index": f"feaas",
        "doc_type": extension,
        "id": dest_key
    }


def get_common_image_extensions() -> list:
    return [".jpeg", ".jpg", ".png"]


def get_common_text_extensions() -> list:
    return [".text", ".txt", ".csv"]


def build_action_class(action_id):
    module_path, class_name = action_id.rsplit('.', 1)
    module = import_module(module_path)
    Action = getattr(module, class_name)
    return Action



def round_time(timestamp, agg_by):
    dt = datetime.fromtimestamp(timestamp/1000)
    if agg_by == 'second':
        dt = datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)
        return int(dt.timestamp() * 1000)
    elif agg_by == 'minute':
        dt = datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, 0)
        return int(dt.timestamp() * 1000)
    elif agg_by == 'hour':
        dt = datetime(dt.year, dt.month, dt.day, dt.hour, 0, 0)
        return int(dt.timestamp() * 1000)
    elif agg_by == 'day':
        dt = datetime(dt.year, dt.month, dt.day, 0, 0, 0)
        return int(dt.timestamp() * 1000)
    elif agg_by == 'month':
        dt = datetime(dt.year, dt.month, 1, 0, 0, 0)
        return int(dt.timestamp() * 1000)
    elif agg_by == 'year':
        dt = datetime(dt.year, 1, 1, 0, 0, 0)
        return int(dt.timestamp() * 1000)
    else:
        raise Exception(f'Unknown time aggregation: {agg_by}')


def md5(s: str) -> str:
    b = s.encode('utf-8')
    return hashlib.md5(b).hexdigest()


def protoBufIntFix(o):
    if type(o) == dict:
        for k in o.keys():
            if k == 'ival':
                o[k] = int(o[k])
            else:
                o[k] = protoBufIntFix(o[k])
    elif type(o) == list:
        for i in range(len(o)):
            o[i] = protoBufIntFix(o[i])
    return o


def clean_dashboard_schema(dashboard):
    object_id = dashboard.object_id
    o = MessageToDict(dashboard, preserving_proto_field_name=True)
    for i, item in enumerate(o['elements']):
        for var in ['x', 'y', 'w', 'h']:
            if var in o['elements'][i]:
                o['elements'][i][var] = int(o['elements'][i][var])
    return o


def extend_receipt(receipt, outputs2):
    for k in receipt.outputs.keys():
        val = receipt.outputs[k]
        if k not in outputs2:
            outputs2[k] = val
    r2 = objs.Receipt(
        success=receipt.success,
        error_message=receipt.error_message,
        outputs=outputs2,
        primary_output=receipt.primary_output,
        timestamp=int(time.time() * 1000),
        cost = receipt.cost)
    return r2


def format_error_message(e: Exception):
    tb_list = traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__)
    tb_str = "".join(tb_list)
    return f"{type(e)}: {str(e)}; {tb_str}"


def get_Action_from_action_id(action_id, dao):
    if action_id.startswith('feaas-py.'):
        i = len('feaas-py.')
        action_id = action_id[i:]
    elif action_id.startswith('sys.'):
        doc = dao.get_docstore().get_document(action_id)
        action_id = doc['sys_action_id']
    try:
        module_path, class_name = action_id.rsplit('.', 1)
        module = import_module(module_path)
        Action = getattr(module, class_name)
    except (ImportError, AttributeError) as e:
        if action_id.startswith("src.feaas"):
            aid2 = action_id[4:]
            module_path, class_name = aid2.rsplit('.', 1)
            module = import_module(module_path)
            Action = getattr(module, class_name)
            return Action(dao)
        msg = f'Import error in file_processor servicing {action_id}'
        err = traceback.format_exc()
        print(err)
        raise ImportError(action_id)
    action = Action(dao)
    return action


def populate_kwargs(kwargs, trigger, action, populated_key, key=None):
    for k in trigger.param_values.keys():
        if k in trigger.param_values:
            v = trigger.param_values[k]
        else:
            v = None
        if (v is None or str(v).strip() == '') and not(populated_key) and key is not None:
            v = objs.AnyType(ptype=objs.ParameterType.KEY, sval=key)
            kwargs[k] = v
            populated_key = True
        # begin workaround for https://github.com/kylepolich/portal/issues/2350
        if k == 'src_key' and v.sval[-1] == '/':
            pass
        else:
            value = util.any_type_resolved(v)
            if value is not None:
                kwargs[k] = value
    for param in action.action.params:
        var_name = param.var_name
        if param.ptype == objs.ParameterType.KEY and key is not None:
            if var_name not in kwargs or kwargs[var_name].strip() == '':
                # TODO: apply validations such as ENDS_WITH .json
                kwargs[var_name] = key
        elif var_name == 'src_prefix':
            kwargs[var_name] = trigger.src_prefix
    return kwargs, populated_key
