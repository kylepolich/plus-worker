import feaas.objects as objs
from feaas.abstract import AbstractAction
from datetime import datetime

class DateExtractor(AbstractAction):
    def __init__(self, dao):
        date_str = objs.Parameter(
            var_name='date_str',
            label='Date String',
            ptype=objs.ParameterType.STRING
        )
        dt_value = objs.Parameter(
            var_name='dt_value',
            label='Date',
            ptype=objs.ParameterType.DATETIME
        )
        params = [date_str]
        outputs = [dt_value]
        super().__init__(params, outputs)

    def execute_action(self, date_str) -> objs.Receipt:
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            epoch_time = int(dt.timestamp())
            output = objs.AnyType(ptype=objs.ParameterType.DATETIME, ival=epoch_time)
            return objs.Receipt(success=True, primary_output='dt_value', outputs={'dt_value': output})
        except ValueError:
            return objs.Receipt(success=False, error_message="Invalid date format. Expected 'yyyy-mm-dd'")
