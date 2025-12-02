from feaas.router.matchers.abstract import AbstractMatcher


class PhraseMatch(AbstractMatcher):


    def __init__(self, phrase):
        self.phrase = phrase


    def matches(self, primary_input: str, input_dict: dict) -> bool:
        if primary_input not in input_dict:
            return self.phrase is None
        i = input_dict[primary_input].find(self.phrase)
        return i >= 0
