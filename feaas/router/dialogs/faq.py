from feaas.router.dialogs.abstract import AbstractDialog


class FaqDialog(AbstractDialog):


    def __init__(self, dao):
        super()
        self.docstore = dao.get_docstore()
        self.streams = dao.get_streams()


    def _match(self, question, text):
        if question.lower().strip() == text.lower().strip():
            return True
        print("TODO: fuzzy match levels", question, text)
        return False


    def handle_event(self, session, dialog, primary_input, input_dict, now) -> bool:
        # TODO: recent_stream_id
        text = input_dict[primary_input]
        for option in dialog.faq_options:
            question = option.question
            if self._match(question, text):
                actions = option.actions
                for field_name in ['stream_id', 'timestamp']:
                    if field_name in input_dict:
                        del input_dict[field_name]
                self.streams.update_feed(dialog.recent_stream_id, now, input_dict)
                return actions
        return None


