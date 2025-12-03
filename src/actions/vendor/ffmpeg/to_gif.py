"""FFMPEG ToGif action - convert video to animated GIF."""
import os
import tempfile

import feaas.objects as objs
from src.actions.vendor.ffmpeg.base import FFMPEGAction


class ToGif(FFMPEGAction):
    """Convert video segment to animated GIF."""

    def __init__(self, dao):
        params = [
            objs.Parameter(var_name='video_file', label='Video File', ptype=objs.ParameterType.STRING),
            objs.Parameter(var_name='start_time', label='Start Time', ptype=objs.ParameterType.STRING),
            objs.Parameter(var_name='duration', label='Duration', ptype=objs.ParameterType.FLOAT),
            objs.Parameter(var_name='fps', label='Frame Rate', ptype=objs.ParameterType.INTEGER),
            objs.Parameter(var_name='width', label='Width', ptype=objs.ParameterType.INTEGER),
        ]
        outputs = [
            objs.Parameter(var_name='gif_file', label='GIF File', ptype=objs.ParameterType.STRING),
        ]
        super().__init__(dao, params, outputs)

    def execute_action(self, video_file, start_time='0', duration=5.0, fps=10, width=480) -> objs.Receipt:
        local_input = None
        local_output = None
        palette_path = None

        try:
            local_input = self.download_file(video_file)

            fd, local_output = tempfile.mkstemp(suffix=".gif")
            os.close(fd)
            fd, palette_path = tempfile.mkstemp(suffix=".png")
            os.close(fd)

            filters = f"fps={fps},scale={width}:-1:flags=lanczos"

            # Two-pass for better quality GIFs
            self.run_ffmpeg(["-ss", str(start_time), "-t", str(duration), "-i", local_input,
                             "-vf", f"{filters},palettegen=stats_mode=diff", palette_path])

            self.run_ffmpeg(["-ss", str(start_time), "-t", str(duration), "-i", local_input,
                             "-i", palette_path,
                             "-lavfi", f"{filters}[x];[x][1:v]paletteuse=dither=bayer:bayer_scale=5:diff_mode=rectangle",
                             local_output])

            output_key = self.get_output_key(video_file, "gif", ".gif")
            self.upload_file(local_output, output_key)

            return objs.Receipt(
                success=True, primary_output='gif_file',
                outputs={'gif_file': objs.AnyType(ptype=objs.ParameterType.STRING, sval=output_key)}
            )

        except Exception as e:
            return objs.Receipt(success=False, error_message=str(e))
        finally:
            self.cleanup(local_input, local_output, palette_path)
