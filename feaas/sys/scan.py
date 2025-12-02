import time
from feaas.abstract import AbstractAction
import feaas.objects as objs


class ScanAndSummarize(AbstractAction):


    def __init__(self, dao):
        username = objs.Parameter(
            var_name='username',
            label='Username',
            ptype=objs.ParameterType.USERNAME)
        params = [username]

        total_records = objs.Parameter(
            optional=True,
            var_name='total_records',
            label='Total Records',
            ptype=objs.ParameterType.INTEGER)
        outputs = [total_records]
        super().__init__(params, outputs)
        self.primary_output = 'total_records'
        self.docstore = dao.get_docstore()
        self.streams = dao.get_streams()


    def execute_action(self, username) -> objs.Receipt:
        items = self.docstore.get("")

        start_time = time.time()

        result = dict()
        result['total_records'] = len(items)
        result['missing_owner_count'] = 0
        result['ttl_delete_count'] = 0
        result['owners'] = dict()

        for item in items:
            if "ttl" in item and time.time() > float(item["ttl"]):
                self.docstore.delete_document(item["object_id"])
                result['ttl_delete_count'] += 1

            if "owner" not in item:
                result['missing_owner_count'] += 1
                continue
            elif item["owner"] in result['owners']:
                result['owners'][item["owner"]] += 1
            else:
                result['owners'][item["owner"]] = 1

        result['time_of_execution'] = time.time() - start_time
        self.streams.update_feed('sys.scan_results', int(time.time()), result)

        outputs = {
            self.primary_output: objs.AnyType(ival=result['total_records'])
        }
        return objs.Receipt(success=True, error_message=None, outputs=outputs, primary_output=self.primary_output)
