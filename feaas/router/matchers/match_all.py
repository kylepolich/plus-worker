from feaas.router.matchers.abstract import AbstractMatcher


class MatchAll(AbstractMatcher):


    def __init__(self, match_all):
        self.match_all = match_all


    def matches(self, primary_input: str, input_dict: dict) -> bool:
        return self.match_all
