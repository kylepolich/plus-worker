"""FFMPEG Thumbnail action - extract a single frame from a video at a given timestamp."""
import feaas.objects as objs
from src.actions.vendor.ffmpeg.base import FFMPEGAction


class Thumbnail(FFMPEGAction):
    """Grab one frame from a video at `at_sec` and write it as png/jpg/webp."""

    def __init__(self, dao):
        params = [
            objs.Parameter(var_name='file', label='Video File', ptype=objs.ParameterType.STRING),
            objs.Parameter(var_name='at_sec', label='Timestamp (s)', ptype=objs.ParameterType.FLOAT),
            objs.Parameter(var_name='format', label='Image Format', ptype=objs.ParameterType.STRING),
            objs.Parameter(var_name='max_width_px', label='Max Width (px)', ptype=objs.ParameterType.INTEGER),
        ]
        outputs = [
            objs.Parameter(var_name='file', label='Thumbnail File', ptype=objs.ParameterType.STRING),
        ]
        super().__init__(dao, params, outputs)

    def execute_action(self, file, at_sec=0.0, format='jpg', max_width_px=0) -> objs.Receipt:
        local_input = None
        local_output = None
        try:
            import os
            import tempfile

            fmt = (format or 'jpg').lower().lstrip('.')
            if fmt not in ('jpg', 'jpeg', 'png', 'webp'):
                return objs.Receipt(success=False, error_message=f"Unsupported format: {format}")

            try:
                ts = max(0.0, float(at_sec or 0))
            except (TypeError, ValueError):
                return objs.Receipt(success=False, error_message=f"at_sec must be numeric, got: {at_sec!r}")

            try:
                width = int(max_width_px or 0)
            except (TypeError, ValueError):
                width = 0

            local_input = self.download_file(file)
            fd, local_output = tempfile.mkstemp(suffix=f".{fmt}")
            os.close(fd)

            # -ss before -i is fast seek (keyframe), accurate enough for thumbnails.
            args = ["-ss", f"{ts:.3f}", "-i", local_input, "-frames:v", "1"]
            if width > 0:
                # Scale preserving aspect ratio; -2 = nearest even number for codec compat.
                args += ["-vf", f"scale='min({width},iw)':-2"]
            args += ["-q:v", "2", local_output]
            self.run_ffmpeg(args)

            output_key = self.get_output_key(file, f"thumb_{int(round(ts))}s", new_ext=fmt)
            self.upload_file(local_output, output_key)

            return objs.Receipt(
                success=True, primary_output='file',
                outputs={'file': objs.AnyType(ptype=objs.ParameterType.STRING, sval=output_key)},
            )
        except Exception as e:
            return objs.Receipt(success=False, error_message=str(e))
        finally:
            self.cleanup(local_input, local_output)
