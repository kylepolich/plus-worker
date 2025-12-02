from feaas.router.dialogs.abstract import AbstractDialog
from feaas.router.matchers import exact_match
from feaas.router.matchers import fuzzy_match
from feaas.router.matchers import phrase_match
from feaas.router.matchers import match_all
from feaas.stream import util


class DecisionTreeDialog(AbstractDialog):


    def __init__(self):
        super()


    def handle_event(self, session, dialog, primary_input, input_dict, now) -> list: # of actions or None if not a match
        text = input_dict[primary_input]
        if len(dialog.matchers) == 0:
            return None
        for matcher in dialog.matchers:
            params = matcher.params
            for param in params:
                if primary_input not in input_dict:
                    return None
                val = input_dict[primary_input]
                if param.var_name not in dialog.matcher_param_value:
                    return None
                goal = dialog.matcher_param_value[param.var_name]
                if val != goal.sval:
                    return None
        actions = dialog.actions
        return actions


