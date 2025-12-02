from feaas.dao.blobstore.s3.s3blobstore import S3Blobstore as Blobstore
from feaas.dao.docstore.dynamo import DynamoDocstore, DynamoStream
from feaas.dao.queue.sqs import SqsQueue as Queue
# from feaas.dao.search.elasticsearch import ElasticSearchDao
import logging
import os


log = logging.getLogger(__name__)
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))


def get_value_or_none(d, value, default=None):
    # We want to minimize the amount of sensitive data in .github/workflows/pythonapp.yml
    if value in d:
        return d[value]
    else:
        log.warning(f"Missing environment variable: {value}")
        return default


class DataAccessObject(object):


    def __init__(self, props={}, sns=None, running_as_worker=False):

        if 'ACCESS_KEY' in props and 'SECRET_KEY' in props:
            self.access_key = props['ACCESS_KEY']
            self.secret_key = props['SECRET_KEY']
        else:
            print("Getting alt configs; must not be on lambda.")
            self.access_key = props['AWS_ACCESS_KEY_ID']
            self.secret_key = props['AWS_SECRET_ACCESS_KEY']
        self.region_name = props['REGION']
        table_name      = get_value_or_none(props, 'DYNAMO_TABLE')
        streams_table   = get_value_or_none(props, 'DYNAMO_STREAMS_TABLE')
        if table_name is None:
            table_name = get_value_or_none(props, 'OBJECT_TABLE_NAME')
        if streams_table is None:
            streams_table = get_value_or_none(props, 'METRIC_TABLE_NAME')
        bucket_name     = get_value_or_none(props, 'PRIMARY_BUCKET')
        queue_url       = None #get_value_or_none(props, 'QUEUE_URL')
        async_queue_url = None #get_value_or_none(props, 'ASYNC_QUEUE_URL')
        self.blobstore = Blobstore(bucket_name, self.access_key, self.secret_key)
        self.docstore = DynamoDocstore(table_name, self.access_key, self.secret_key)
        self.streams = DynamoStream(streams_table, self.access_key, self.secret_key)
        self.sys_name = get_value_or_none(props, 'SYS_NAME')
        if queue_url is not None:
            self.work_queue = Queue(queue_url, self.access_key, self.secret_key)
        else:
            self.work_queue = None
        if async_queue_url is not None:
            self.async_queue = Queue(async_queue_url, self.access_key, self.secret_key)
        else:
            log.debug(f"Setting self.async_queue = None")
            self.async_queue = None
        self.local_cache = {}
        self.sns = sns
        self.running_as_worker = running_as_worker


    def get_blobstore(self):
        return self.blobstore


    def get_docstore(self):
        return self.docstore


    def get_streams(self):
        return self.streams

