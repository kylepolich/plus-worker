# from boto3.dynamodb.conditions import Key
# from google.protobuf.json_format import Parse, MessageToDict
# from importlib import import_module
# import logging
# import os
# import pkgutil
# import pyclbr

# from feaas.dao.dao import DataAccessObject
# from feaas.util import common
# from chalicelib.load_props import load_props
# from seed.actions.updater import update_actions

# log = logging.getLogger(__name__)
# logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
# log.info("Start")

# props = load_props(True)
# dao = DataAccessObject(props)

# action_path = 'chalicelib/actions'
# feaas_namespace_prefix = 'feaas-py'
# min_action_count = 20
# update_actions(dao, action_path, feaas_namespace_prefix, min_action_count)

# log.info("Complete")
