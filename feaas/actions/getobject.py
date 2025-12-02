import feaas.objects as objs
from feaas.abstract import AbstractAction


class GetObjectValue(AbstractAction):
    def __init__(self, dao):
        object_id = objs.Parameter(
            var_name='object_id',
            label='Object ID',
            ptype=objs.ParameterType.OBJECT_ID)
        name = objs.Parameter(
            var_name='name',
            label='Name',
            ptype=objs.ParameterType.STRING)
        output_name = objs.Parameter(
            var_name='output_name',
            label='output name',
            ptype=objs.ParameterType.STRING)
        params = [object_id, name]
        outputs = [output_name]
        super().__init__(params, outputs)
        self.docstore = dao.get_docstore()


    def execute_action(self, object_id, name) -> objs.Receipt:
        doc = self.docstore.get_document(object_id)
        if doc is None:
            return objs.Receipt(success=False, error_message='Object does not exist') 
        output_name = doc.get(name)
        if output_name is None:
            return objs.Receipt(success=False, error_message=f'No name key found in object {doc}')
        output_name = str(output_name)
        output = objs.AnyType(ptype=objs.ParameterType.STRING, sval=output_name)
        return objs.Receipt(success=True, primary_output='output_name', outputs={'output_name': output})
