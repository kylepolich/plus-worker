import feaas.objects as objs
from feaas.sys.executor import ActionExecutor


class PlusHelper(object):


    def __init__(self, username, dao):
        self.username = username
        self.dao = dao
        sys_name = 'local'
        self.action_executor = ActionExecutor(dao, sys_name)


    def find_actions(self, query: str):
        if query.lower() == 'pdf2text':
            return ['sys.actions.d47ca422df4ac465fd74ef60cdec281b']
        if query.lower() == 'openai':
            return ['sys.actions.b80908f29125ada9f8e2f8df0aa5afae']
        if query.lower() == 'sendgrid':
            return ['sys.actions.1ebcda211bacbf114694d4bcb3d5ed65']
        # TODO: import what cli has to here.  merge to one code path
        # TODO: extend as an intelligent, intuitive, google-like way of querying actions
        return []


    def execute_action(self, action_id: str, data):
        if action_id is None:
            return objs.Receipt(success=False, error_message=f"Requires {action_id}")
        r = self.action_executor.begin_action_execution(action_id, self.username, data)
        return r