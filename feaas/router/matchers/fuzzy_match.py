# from feaas.router.matchers.abstract import AbstractMatcher
# from rapidfuzz import fuzz, process


# class FuzzyMatch(AbstractMatcher):


#     def __init__(self, phrase, threshold):
#         self.phrase = phrase
#         self.threshold = threshold


#     def matches(self, primary_input: str, input_dict: dict) -> bool:
#         if primary_input not in input_dict:
#             return self.phrase is None
#         # Use token_sort_ratio from rapidfuzz
#         match = fuzz.token_sort_ratio(self.phrase, input_dict[primary_input]) / 100.0
#         return match > self.threshold
