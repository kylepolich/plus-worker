# This file exists to create a single point of control
# It is used in feaas.py and in test.py

import logging

logging.getLogger('requests').setLevel(logging.CRITICAL)
logging.getLogger('s3transfer').setLevel(logging.CRITICAL)
logging.getLogger('boto3').setLevel(logging.CRITICAL)
logging.getLogger('botocore').setLevel(logging.CRITICAL)
logging.getLogger('botocore.credentials').setLevel(logging.ERROR)
logging.getLogger('chardet').setLevel(logging.CRITICAL)
logging.getLogger('urllib3').setLevel(logging.WARN)
logging.getLogger('matplotlib').setLevel(logging.CRITICAL)
logging.getLogger('paramiko.transport').setLevel(logging.ERROR)
logging.getLogger('feaas.dao').setLevel(logging.WARN)
logging.getLogger('elasticsearch').setLevel(logging.ERROR)
logging.getLogger('numba').setLevel(logging.ERROR)
logging.getLogger('chalicelib.api.work').setLevel(logging.WARN)
logging.getLogger('pdfminer').setLevel(logging.ERROR)
logging.getLogger('stripe').setLevel(logging.ERROR)
logging.getLogger('PIL').setLevel(logging.ERROR)
logging.getLogger('python_http_client.client').setLevel(logging.ERROR)
logging.getLogger('googleapiclient').setLevel(logging.WARN)
logging.getLogger('fuzzywuzzy').setLevel(logging.ERROR)
