import feaas.objects as objs
from feaas.router.dialogs.abstract import AbstractDialog
from feaas.util import common
from google.protobuf.json_format import Parse, MessageToDict
import json


class ObjectConstructorDialog(AbstractDialog):


    def __init__(self, dao):
        super()
        self.docstore = dao.get_docstore()
        self.streams = dao.get_streams()


    def handle_event(self, session, dialog, primary_input, input_dict, now) -> bool:
        doc = self.docstore.get_document(dialog.oc_object_id)
        oc = Parse(json.dumps(doc, cls=common.DecimalEncoder), objs.ObjectConstructor(), ignore_unknown_fields=True)
        object_id = oc.object_id
        greeting = oc.name
        i = object_id.rfind('.')
        uid = object_id[i+1:]
        completed_key = f'completed_oc_{uid}'
        if completed_key in session:
            return None
        started_key = f'stared_oc_{uid}'
        if started_key not in session:
            session[started_key] = now
            actions = [self._make_utterance(greeting)]
        for page in oc.pages:
            for param in page.params:
                var_name = param.var_name
                # ptype = param.ptype
                if var_name not in session:
                    label = param.label
                    actions.append(self._make_utterance(label))
                    return actions
        session[completed_key] = now
        # TODO: how to validate and store reply
        actions = None
        return actions


    def _make_utterance(self, text):
        return objs.Action(
            action_id='feaas-py.chalicelib.actions.router.chatbot.utterance.text.TextUtterance',
            runtime_id=0
            # params=
        )
    # repeated Parameter params = 7;
    # repeated Parameter outputs = 10;


