from feaas.router.matchers.abstract import AbstractMatcher


class ExactMatch(AbstractMatcher):


    def __init__(self, password):
        self.password = password


    def matches(self, primary_input: str, input_dict: dict) -> bool:
        if primary_input not in input_dict:
            return self.password is None
        return self.password == input_dict[primary_input]
