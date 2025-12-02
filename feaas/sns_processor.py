import feaas.objects as objs
from feaas.stream import util
from google.protobuf.json_format import Parse
import json
import time


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


def convert_sns_msg_to_sqs_msg(event, docstore):
    s3_event_dict = json.loads(event.message)
    bucket_name = s3_event_dict['Records'][0]['s3']['bucket']['name']
    key = s3_event_dict['Records'][0]['s3']['object']['key']
    s3_event_type = s3_event_dict['Records'][0]['eventName']
    sys_s3_bucket_obj_id = f"sys.aws.s3/{bucket_name}" # see chalicelib/actions/vendor/aws/s3/ls.py or s3listener.py
    system_s3_bucket_record = docstore.get_document(sys_s3_bucket_obj_id)
    username = system_s3_bucket_record.get('username', 'missing')
    credentials_object_id = system_s3_bucket_record.get('credentials_object_id', 'missing')
    sqs_msg_content = {
      "username": username,
      "_type": "aws-s3-notification",
      "bucket_name": bucket_name,
      "key": key,
      "credentials_object_id": credentials_object_id,
      "s3_event_type": s3_event_type
    }
    return sqs_msg_content


def convert_sns_msg_to_stream_event(event) -> objs.StreamEvent:
    data = {}
    if event.message == 'ping':
        data['ping'] = util.any_type_ifier('pong')
        return objs.StreamEvent(
            stream_id='test@dataskeptic.com/test.convert_sns_msg_to_stream_event',
            timestamp=int(time.time() * 1000),
            etype=objs.StreamEvent.EventType.PING,
            data=data)
    s = event.subject
    try:
        event_type = int(s)
        if event_type == objs.StreamEvent.EventType.PING:
            data['ping'] = util.any_type_ifier('pong')
            return objs.StreamEvent(
                stream_id='test@dataskeptic.com/test.convert_sns_msg_to_stream_event',
                timestamp=int(time.time() * 1000),
                etype=objs.StreamEvent.EventType.PING,
                data=data)
        return Parse(event.message, objs.StreamEvent())
    except:
        print("event.message = ", event.message)
        message_json = json.loads(event.message)
        return handle_json(message_json)

