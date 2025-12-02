from feaas.router.dialogs.abstract import AbstractDialog


class HitlDialog(AbstractDialog):


    def __init__(self):
        super()


    def _match(self, question, text):
        return False


    def handle_event(self, session, dialog, primary_input, input_dict, now) -> bool:
        text = input_dict[primary_input]
        for option in dialog.faq_options:
            question = option.question
            if self._match(question, text):
                actions = dialog.actions
                self.streams.update_feed(dialog.recent_stream_id, now, input_dict)
                # int64 first_used = 4;
                # int64 last_used = 5;
                # int64 use_count = 6;
                return True
        return False


