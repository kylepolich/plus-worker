"""FFMPEG Trim action - cut a segment from media file."""
import os
import tempfile

import feaas.objects as objs
from src.actions.vendor.ffmpeg.base import FFMPEGAction


class Trim(FFMPEGAction):
    """Trim a media file to a specific time range."""

    def __init__(self, dao):
        params = [
            objs.Parameter(var_name='file', label='Media File', ptype=objs.ParameterType.STRING),
            objs.Parameter(var_name='start_time', label='Start Time', ptype=objs.ParameterType.STRING),
            objs.Parameter(var_name='end_time', label='End Time', ptype=objs.ParameterType.STRING),
            objs.Parameter(var_name='duration', label='Duration', ptype=objs.ParameterType.STRING),
        ]
        outputs = [
            objs.Parameter(var_name='file', label='Trimmed File', ptype=objs.ParameterType.STRING),
        ]
        super().__init__(dao, params, outputs)

    def execute_action(self, file, start_time='0', end_time=None, duration=None) -> objs.Receipt:
        local_input = None
        local_output = None

        try:
            local_input = self.download_file(file)

            ext = os.path.splitext(file)[1] or ".mp4"
            fd, local_output = tempfile.mkstemp(suffix=ext)
            os.close(fd)

            args = ["-ss", str(start_time), "-i", local_input]

            if duration:
                args += ["-t", str(duration)]
            elif end_time:
                args += ["-to", str(end_time)]

            args += ["-c", "copy", "-avoid_negative_ts", "make_zero"]
            args.append(local_output)

            self.run_ffmpeg(args)

            output_key = self.get_output_key(file, "trimmed")
            self.upload_file(local_output, output_key)

            return objs.Receipt(
                success=True, primary_output='file',
                outputs={'file': objs.AnyType(ptype=objs.ParameterType.STRING, sval=output_key)}
            )

        except Exception as e:
            return objs.Receipt(success=False, error_message=str(e))
        finally:
            self.cleanup(local_input, local_output)
