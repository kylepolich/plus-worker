import sys; sys.path.insert(0, 'src')
from feaas.dao.dao import DataAccessObject
from feaas.util import common
import hashlib
import json
import time
import os
from feaas.sys.actions import AvailableActionCrawler

if 'AWS_REGION' in os.environ:
    print("Running on Lambda")
else:
    print("Running locally")
    from dotenv import load_dotenv
    load_dotenv()

props = dict(os.environ)

prod_url = 'https://9b8tp8giu6.execute-api.us-east-1.amazonaws.com/api/'
# prod_url = 'https://6infuttxy5.execute-api.us-east-1.amazonaws.com/dev/'
ecs_url = 'http://feaas-py-worker.dataskeptic.com/'

dao = DataAccessObject(props, None)
docstore = dao.get_docstore()
streams = dao.get_streams()


def do_testing():
    testfiles = glob.glob('../../itest/**/*.json', recursive=True)
    # testfiles = glob.glob('../../../feaas-py-worker/itest/**/*.json', recursive=True)
    tests = []
    for test in testfiles:
        o = json.load(open(test, 'r'))
        o['base'] = url
        # o['base'] = ecs_url
        tests.append(o)

    print(len(tests))


    identify_untested_actions(tests, url)

    pass_count = 0

    for test in tests:
        action_id = test['action_id']
        try:
            s, o = run_test(test)
        except:
            print(action_id, traceback.format_exc())
            s = False
            o = { 'error_message': '500' }
        if not(s):
            if 'error_message' in o:
                em = o['error_message']
            else:
                em = 'fail'
            print('FAIL!', action_id, em)
            print('-------------------------------------')
        else:
            pass_count += 1

    print("Passes:", pass_count, '/', len(tests))


def format_action_objects(sys_name, actions):
    action_docs = []
    for action in actions:
        #slug = make_slug(sys_name, action)
        sys_action_id = action['sys_action_id']
        unique_id = assign_unique_id(sys_name, sys_action_id)
        owner = 'sys.actions'
        doc = {
            'object_id': f'{owner}.{unique_id}',
            'owner': owner,
            'sys_name': sys_name,
            'sys_action_id': sys_action_id,
            'params': action['params'],
            'outputs': action['outputs'],
            'unique_id': unique_id,
        }
        action_docs.append(doc)
    return action_docs


def assign_unique_id(sys_name, sys_action_id):
    i = f'{sys_name}:{sys_action_id}'.lower().strip()
    return hashlib.md5(i.encode('utf-8')).hexdigest()


def get_new_data(sys_name, module_prefix):
    new_items = []
    exist_items = []
    updated_items = []

    owner = 'sys.actions'
    existing_actions = docstore.get_list(owner)
    object_ids = []
    for ea in existing_actions:
        object_ids.append(ea['object_id'])
    existing_actions = docstore.get_batch_documents(object_ids)
    db = {}
    for ea in existing_actions:
        object_id = ea['object_id']
        db[object_id] = ea

    data = {}
    #get actions from crawler not APi
    action_crawler = AvailableActionCrawler(sys_name=sys_name, dao=dao, module_prefix=module_prefix, owner = 'sys.action')
    actions = action_crawler.get_actions()
    data[sys_name] = actions

    for sys_name in data.keys():
        actions = data[sys_name]
        arr = format_action_objects(sys_name, actions)
        for item in arr:
            object_id = item['object_id']
            if object_id in db:
                existing = db[object_id]
                for rmc in ['icon', 'label', 'short_desc', 'public_action_id']:
                    if rmc in existing:
                        del existing[rmc]
                existing = common.clean_json_dict(existing)
                item = common.clean_json_dict(item)
                if 'run_mode' in existing:
                    del existing['run_mode']
                s1 = json.dumps(existing, cls=common.DecimalEncoder, sort_keys=True)
                s2 = json.dumps(item, cls=common.DecimalEncoder, sort_keys=True)
                checksum1 = hashlib.md5(s1.encode('utf-8')).hexdigest()
                checksum2 = hashlib.md5(s2.encode('utf-8')).hexdigest()
                if checksum1 == checksum2:
                    exist_items.append(item)
                else:
                    sys_action_id = item['sys_action_id']
                    differences = []
                    all_keys = set(existing.keys()).union(set(item.keys()))
                    for key in all_keys:
                        if key not in ['image']:
                            existing_value = existing.get(key, 'Not present')
                            item_value = item.get(key, 'Not present')
                            if existing_value != item_value:
                                differences.append(f"{key}: {existing_value} -> {item_value}")
                    if len(differences) > 0:
                        print("Found delta for", sys_action_id)
                        diffs = '\n'.join(differences) if differences else "No differences found."
                        print(diffs)
                        updated_items.append(item)
            else:
                new_items.append(item)

    return new_items, exist_items, updated_items


def do_work(sys_name, module_prefix):

	new_items, exist_items, updated_items = get_new_data(sys_name, module_prefix)

	u = {
		'num_new_items': len(new_items),
		'num_existing_items': len(exist_items),
		'num_updates': len(updated_items),
		'new_items': [],
		'exist_items': [],
		'updated_items': [],
	}

	if len(new_items) + len(updated_items) == 0:
		print("Nothing to do")
		return u

	stream_id = 'sys.releases'

	for item in new_items:
		u['new_items'].append({ "object_id": item['object_id'], "sys_name": item['sys_name'], "sys_action_id": item['sys_action_id'] })

	if len(new_items) > 0:
	    docstore.batch_write_objects(new_items)

	for item in updated_items:
		u['updated_items'].append({
            "object_id": item['object_id'],
            "sys_name": item['sys_name'],
            "sys_action_id": item['sys_action_id'],
            "unique_id": item['unique_id']
        })
		docstore.update_document(item['object_id'], item)

	streams.update_feed(stream_id, int(time.time() * 1000), u)

	return u



# resp = do_work()
# print(resp)