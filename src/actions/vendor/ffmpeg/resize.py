"""FFMPEG Resize action - scale video dimensions."""
import os
import tempfile

import feaas.objects as objs
from src.actions.vendor.ffmpeg.base import FFMPEGAction


class Resize(FFMPEGAction):
    """Resize video to different dimensions."""

    def __init__(self, dao):
        params = [
            objs.Parameter(var_name='file', label='Video File', ptype=objs.ParameterType.STRING),
            objs.Parameter(var_name='preset', label='Resolution Preset', ptype=objs.ParameterType.STRING),
            objs.Parameter(var_name='width', label='Width', ptype=objs.ParameterType.INTEGER),
            objs.Parameter(var_name='height', label='Height', ptype=objs.ParameterType.INTEGER),
        ]
        outputs = [
            objs.Parameter(var_name='file', label='Resized Video', ptype=objs.ParameterType.STRING),
        ]
        super().__init__(dao, params, outputs)

    def execute_action(self, file, preset=None, width=None, height=None) -> objs.Receipt:
        presets = {
            "480p": (None, 480), "720p": (None, 720), "1080p": (None, 1080),
            "1440p": (None, 1440), "2160p": (None, 2160),
        }

        if preset and preset in presets:
            width, height = presets[preset]

        if width is None and height is None:
            return objs.Receipt(success=False, error_message="Must specify preset, width, height, or both")

        local_input = None
        local_output = None

        try:
            local_input = self.download_file(file)

            ext = os.path.splitext(file)[1] or ".mp4"
            fd, local_output = tempfile.mkstemp(suffix=ext)
            os.close(fd)

            if width and height:
                scale = f"scale={width}:{height}"
            elif width:
                scale = f"scale={width}:-2"
            else:
                scale = f"scale=-2:{height}"

            args = ["-i", local_input, "-vf", scale, "-c:v", "libx264", "-crf", "23", "-c:a", "copy", local_output]
            self.run_ffmpeg(args)

            suffix = preset if preset else f"{width or 'auto'}x{height or 'auto'}"
            output_key = self.get_output_key(file, suffix)
            self.upload_file(local_output, output_key)

            return objs.Receipt(
                success=True, primary_output='file',
                outputs={'file': objs.AnyType(ptype=objs.ParameterType.STRING, sval=output_key)}
            )

        except Exception as e:
            return objs.Receipt(success=False, error_message=str(e))
        finally:
            self.cleanup(local_input, local_output)
