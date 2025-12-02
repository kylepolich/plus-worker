import feaas.objects as objs
from feaas.stream import util
from feaas.util import common
from feaas.sys.executor import ActionExecutor
from feaas.psee.psee import PlusScriptExecutionEngine
from datetime import datetime
from decimal import Decimal
from google.protobuf.json_format import MessageToDict, Parse
from importlib import import_module
import json
import logging
import os
import requests
import traceback
import time
import uuid


log = logging.getLogger(__name__)
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))


class CollectionProcessor(object):


    def __init__(self, dao, secret, sys_name):
        self.dao = dao
        self.action_executor = ActionExecutor(dao, sys_name)
        self.docstore = dao.get_docstore()
        self.psee = PlusScriptExecutionEngine(dao, self.action_executor)


    def handle_record_update(self, object_id):
        # TODO: get object
        # TODO: get triggers
        # TODO: run triggers
        # TODO: update record
        pass

