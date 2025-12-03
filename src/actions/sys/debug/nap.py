"""
Debug action that sleeps for a specified number of seconds.
Useful for testing long-running Fargate jobs.
"""
import time
from feaas.action import AbstractAction
from feaas import objects as objs


class Nap(AbstractAction):
    """Sleep for a specified number of seconds."""

    @staticmethod
    def get_name():
        return "sys.debug.Nap"

    @staticmethod
    def get_label():
        return "Nap (Sleep)"

    @staticmethod
    def get_short_desc():
        return "Sleep for a specified number of seconds"

    @staticmethod
    def get_inputs():
        return [
            objs.Parameter(
                var_name="seconds",
                ptype=objs.ParameterType.INTEGER,
                label="Seconds",
                short_desc="Number of seconds to sleep",
                required=True
            )
        ]

    @staticmethod
    def get_outputs():
        return [
            objs.Parameter(
                var_name="slept_seconds",
                ptype=objs.ParameterType.INTEGER,
                label="Slept Seconds",
                short_desc="Number of seconds actually slept"
            ),
            objs.Parameter(
                var_name="message",
                ptype=objs.ParameterType.STRING,
                label="Message",
                short_desc="Confirmation message"
            )
        ]

    def execute(self):
        seconds = self.params.get("seconds", 0)

        # Clamp to reasonable range
        if seconds < 0:
            seconds = 0
        if seconds > 3600:  # Max 1 hour
            seconds = 3600

        print(f"Nap: Sleeping for {seconds} seconds...")
        time.sleep(seconds)
        print(f"Nap: Woke up after {seconds} seconds")

        return objs.Receipt(
            success=True,
            outputs={
                "slept_seconds": objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=seconds),
                "message": objs.AnyType(ptype=objs.ParameterType.STRING, sval=f"Slept for {seconds} seconds")
            }
        )
