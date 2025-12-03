"""FFMPEG Concat action - join multiple media files."""
import os
import tempfile

import feaas.objects as objs
from src.actions.vendor.ffmpeg.base import FFMPEGAction


class Concat(FFMPEGAction):
    """Concatenate multiple media files into one."""

    def __init__(self, dao):
        params = [
            objs.Parameter(var_name='files', label='Media Files', ptype=objs.ParameterType.LIST),
            objs.Parameter(var_name='output_format', label='Output Format', ptype=objs.ParameterType.STRING),
        ]
        outputs = [
            objs.Parameter(var_name='file', label='Concatenated File', ptype=objs.ParameterType.STRING),
        ]
        super().__init__(dao, params, outputs)

    def execute_action(self, files, output_format=None) -> objs.Receipt:
        if not files or len(files) == 0:
            return objs.Receipt(success=False, error_message="No files provided for concatenation")

        local_inputs = []
        local_output = None
        concat_list_path = None

        try:
            for key in files:
                local_inputs.append(self.download_file(key))

            if not output_format:
                output_format = os.path.splitext(files[0])[1].lstrip('.') or "mp4"

            fd, concat_list_path = tempfile.mkstemp(suffix=".txt")
            with os.fdopen(fd, 'w') as f:
                for path in local_inputs:
                    escaped = path.replace("'", "'\\''")
                    f.write(f"file '{escaped}'\n")

            fd, local_output = tempfile.mkstemp(suffix=f".{output_format}")
            os.close(fd)

            args = ["-f", "concat", "-safe", "0", "-i", concat_list_path, "-c", "copy", local_output]
            self.run_ffmpeg(args)

            output_key = self.get_output_key(files[0], "concat", output_format)
            self.upload_file(local_output, output_key)

            return objs.Receipt(
                success=True, primary_output='file',
                outputs={'file': objs.AnyType(ptype=objs.ParameterType.STRING, sval=output_key)}
            )

        except Exception as e:
            return objs.Receipt(success=False, error_message=str(e))
        finally:
            self.cleanup(local_output, concat_list_path, *local_inputs)
