import feaas.objects as objs
import feaas.objects as objs
from feaas.abstract import AbstractAction


class PseeScriptValidator(AbstractAction):
    def __init__(self, dao):
        psee_object_id = objs.Parameter(
            var_name='psee_object_id',
            label='PlusScript',
            ptype=objs.ParameterType.OBJECT_ID,
            src_owners=['{hostname}/{username}/ps-file-trigger', '{hostname}/{username}/ps-stream-trigger'])
        data_object_id = objs.Parameter(
            var_name='data_object_id',
            label='Input',
            ptype=objs.ParameterType.OBJECT_ID)
        params = [psee_object_id, data_object_id]
        missing_inputs = objs.Parameter(
            var_name='missing_inputs',
            label='Missing Inputs',
            ptype=objs.ParameterType.INTEGER)
        missing_links = objs.Parameter(
            var_name='missing_links',
            label='Missing Links',
            ptype=objs.ParameterType.INTEGER)
        outputs = [missing_inputs, missing_links]
        super().__init__(params, outputs)
        self.docstore = dao.get_docstore()


    def execute_action(self, psee_object_id, data_object_id) -> objs.Receipt:
        script = self.docstore.get_document(psee_object_id)
        if script is None:
            return objs.Receipt(success=False, error_message=f'Cannot find PlusScript in {psee_object_id}')
        data = self.docstore.get_document(data_object_id)
        if data is None:
            data = {}

        missing_inputs = self._get_missing_inputs(script, data)
        missing_links = self._get_missing_links(script)
        
        n1 = len(missing_inputs)
        n2 = len(missing_links)

        outputs = {
            'missing_inputs': objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=n1),
            'missing_links': objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=n2)
        }
        if n1 + n2 > 0:
            errors = []
            errors.extend(missing_inputs)
            errors.extend(missing_links)
            msg = "; ".join(errors)
            return objs.Receipt(success=False, error_message=msg, primary_output='missing_inputs', outputs=outputs)
        else:
            return objs.Receipt(success=True, primary_output='missing_inputs', outputs=outputs)



    def _get_missing_inputs(self, script: objs.PlusScript, data):
        missing_inputs = []

        # check that all inputs are present
        for input_item in script.get('inputs', []):
            if not input_item['optional']:
                label = input_item['label']
                if label not in data:
                    missing_inputs.append(f"Missing required input variable: {label}")


       
        return missing_inputs
    
    def _get_missing_links(self, script):
        links = script.get('links', [])
        linked_var_names = set()

        # get all sourceHandle and targetHandle var_names to the set
        for link in links:
            if 'sourceHandle' in link:
                linked_var_names.add(link['sourceHandle'])
            if 'targetHandle' in link:
                linked_var_names.add(link['targetHandle'])

        missing_links = []

        # collect missing var_names from a list of items
        def collect_var_names(items, key):
            for item in items:
                for io in item.get(key, []):
                    var_name = io.get('var_name')
                    if var_name and not io.get('optional', False) and var_name not in linked_var_names:
                        missing_links.append(var_name)

        # Collect missing var_names from inputs, outputs, and nodes
        collect_var_names(script.get('inputs', []), 'inputs')
        collect_var_names(script.get('outputs', []), 'outputs')
        collect_var_names(script.get('nodes', []), 'outputs')
        collect_var_names(script.get('nodes', []), 'inputs')

        # Remove duplicates
        missing_links = list(set(missing_links))

        return missing_links