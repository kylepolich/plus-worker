import feaas.objects as objs

def validate_input(value, param: objs.Parameter):
	errors = []
	for validation in param.validations:
		vtype = validation.vtype
		msg = None
		if vtype == objs.ValidationType.ENDS_WITH:
			msg = _check_ends_with(value, validation.sval)
		elif vtype == objs.ValidationType.VALID_URL:
			msg = _check_valid_url(value, validation.sval)
		elif vtype == objs.ValidationType.MIN_LENGTH:
			msg = _check_min_length(value, validation.sval)
		elif vtype == objs.ValidationType.MAX_LENGTH:
			msg = _check_max_length(value, validation.sval)
		elif vtype == objs.ValidationType.GREATER_THAN:
			msg = _check_greater_than(value, validation.ival)
		elif vtype == objs.ValidationType.LESS_THAN:
			msg = _check_less_than(value, validation.ival)
		elif vtype == objs.ValidationType.EMAIL:
			msg = _check_is_email(value, validation.sval)
		elif vtype == objs.ValidationType.STARTS_WITH:
			msg = _check_starts_with(value, validation.sval)
		else:
			msg = f"Unsupported ValidationType {vtype}"
		if msg is None:
			if validation.error_message is not None and validation.error_message.strip() != "":
				errors.append(validation.error_message) # Go with the hard coded (more specific) value if present
			else:
				errors.append(msg)

	return errors

	def _check_starts_with(value, validation_param):
		if str(value).startswith(validation_param):
			return None
		else:
			return f'Value {value} should start with {validation_param}'

	def _check_ends_with(value, validation_param):
		if str(value).endswith(validation_param):
			return None
		else:
			return f'Value {value} should end with {validation_param}'

	def _check_min_length(value, validation_param):
		if len(str(value)) > validation_param:
			return None
		else:
			return f'Value {value} needs a minimum length of {validation_param}'

	def _check_max_length(value, validation_param):
		if len(str(value)) <= validation_param:
			return None
		else:
			return f'Value {value} needs a maximum length of {validation_param}'

	def _check_greater_than(value, validation_param):
		if float(value) > validation_param:
			return None
		else:
			return f'Value {value} should be greather than {validation_param}'

	def _check_less_than(value, validation_param):
		if float(value) < validation_param:
			return None
		else:
			return f'Value {value} should be less than {validation_param}'

	def _check_valid_url(value, validation_param):
		# TODO: implement
		return 'No implemented'

	def _check_is_email(value, validation_param):
		# TODO: implement
		return 'No implemented'

