import boto3
from feaas.dao.queue.abstract_queue import AbstractQueue as Queue
import json
import logging
import time
import uuid


log = logging.getLogger(__name__)


class SqsQueue(Queue):


    def __init__(self, queue_url, access_key=None, secret_key=None):
        super().__init__()
        self.queue_url = queue_url
        if self.queue_url is None:
            msg = 'Cannot create SQS queue without url'
            log.error(msg)
            # raise Exception(msg)
        else:
            region = queue_url.split('.')[1]
            if region == 'queue':
                i = len('https://')
                j = queue_url.find('.', i)
                region = queue_url[i:j]
            if access_key is not None:
                self.client = boto3.client('sqs', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region)
                self.resource = boto3.resource('sqs', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region)
            else:
                self.client = boto3.client('sqs', region_name=region)
                self.resource = boto3.resource('sqs', region_name=region)


    def _add_message(self, mb, delay_seconds=0) -> str:
        response = self.client.send_message(
            QueueUrl=self.queue_url,
            DelaySeconds=delay_seconds,
            MessageAttributes={},
            MessageBody=mb
        )
        msgId = response['MessageId']
        if 'dead-letter-queue' in self.queue_url:
            # exit early, already logged
            return msgId
        return msgId


    def _get_message(self, num_msgs=1):
        messages = self.client.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=num_msgs,
            AttributeNames=['All'])
        if 'Messages' in messages:
            message = messages['Messages'][0]
            return message
        else:
            return None


    def _delete_message(self, receipt_handle):
        self.client.delete_message(QueueUrl=self.queue_url, ReceiptHandle=receipt_handle)


    def get_size(self):
        i = self.queue_url.rfind('/')
        qn = self.queue_url[i+1:]
        q = self.resource.get_queue_by_name(QueueName=qn)
        return int(q.attributes.get('ApproximateNumberOfMessages'))


    def add_messages(self, msgs, delay_seconds=0) -> str:
        entries = []
        for msg in msgs:
            o = {
                'Id': str(uuid.uuid4()),
                'MessageBody': json.dumps(msg),
                'DelaySeconds': delay_seconds,
            }
            entries.append(o)
        response = self.client.send_messages(Entries=entries)


# Batch
"""
response = queue.send_messages(
    Entries=[
        {
            'Id': 'string',
            'MessageBody': 'string',
            'DelaySeconds': 123,
            'MessageDeduplicationId': 'string',
            'MessageGroupId': 'string'
        },
    ]
)
"""