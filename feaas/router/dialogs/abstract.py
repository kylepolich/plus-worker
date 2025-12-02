from abc import abstractmethod, ABC


class AbstractDialog(ABC):


	def __init__(self):
		pass


	@abstractmethod
	def handle_event(self, session, dialog, primary_input, input_dict, now) -> list: # of actions or None if not a match
		pass


