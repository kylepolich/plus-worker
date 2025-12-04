"""
Debug action that sleeps for a specified number of seconds.
Useful for testing long-running Fargate jobs.
"""
import time

import feaas.objects as objs
from feaas.abstract import AbstractAction


class Nap(AbstractAction):
    """Sleep for a specified number of seconds."""

    def __init__(self, dao):
        params = [
            objs.Parameter(
                var_name="seconds",
                ptype=objs.ParameterType.INTEGER,
                label="Seconds",
                hint="Number of seconds to sleep (max 3600)"
            )
        ]

        outputs = [
            objs.Parameter(
                var_name="slept_seconds",
                ptype=objs.ParameterType.INTEGER,
                label="Slept Seconds"
            ),
            objs.Parameter(
                var_name="message",
                ptype=objs.ParameterType.STRING,
                label="Message"
            )
        ]

        super().__init__(params, outputs)

    def execute_action(self, seconds=0) -> objs.Receipt:
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
