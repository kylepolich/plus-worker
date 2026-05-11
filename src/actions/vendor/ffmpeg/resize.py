"""FFMPEG Resize action - scale image or video dimensions.

Supports two modes:
  - Proportional scale via `scalar` (e.g. 0.5 = half size). Works for images and video.
  - Absolute dimensions via `preset` (video: 480p/720p/1080p/1440p/2160p) or explicit
    `width` / `height`.

The input may be given as `file` or `src_key` (alias). When `out_format` is supplied the
output is re-encoded to that container/extension (png/gif/jpg/jpeg for images, mp4/webm/... for video).
"""
import os
import tempfile

import feaas.objects as objs
from src.actions.vendor.ffmpeg.base import FFMPEGAction

IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".gif", ".bmp", ".webp", ".tif", ".tiff", ".ico"}
PRESETS = {
    "480p": (None, 480), "720p": (None, 720), "1080p": (None, 1080),
    "1440p": (None, 1440), "2160p": (None, 2160),
}


class Resize(FFMPEGAction):
    """Resize an image or video — proportionally (`scalar`) or to fixed dimensions."""

    def __init__(self, dao):
        params = [
            objs.Parameter(var_name='file', label='Image or Video File', ptype=objs.ParameterType.STRING),
            objs.Parameter(var_name='scalar', label='Scale Factor (e.g. 0.5)', ptype=objs.ParameterType.FLOAT),
            objs.Parameter(var_name='preset', label='Resolution Preset', ptype=objs.ParameterType.STRING),
            objs.Parameter(var_name='width', label='Width', ptype=objs.ParameterType.INTEGER),
            objs.Parameter(var_name='height', label='Height', ptype=objs.ParameterType.INTEGER),
            objs.Parameter(var_name='out_format', label='Output Format', ptype=objs.ParameterType.STRING),
        ]
        outputs = [
            objs.Parameter(var_name='file', label='Resized File', ptype=objs.ParameterType.STRING),
        ]
        super().__init__(dao, params, outputs)

    def execute_action(self, file=None, src_key=None, scalar=None, preset=None,
                       width=None, height=None, out_format=None) -> objs.Receipt:
        in_key = file or src_key
        if not in_key:
            return objs.Receipt(success=False, error_message="No input file given (expected 'file' or 'src_key')")

        # Normalize scale factor
        if scalar is not None:
            try:
                scalar = float(scalar)
            except (TypeError, ValueError):
                return objs.Receipt(success=False, error_message=f"scalar must be numeric, got {scalar!r}")
            if scalar <= 0:
                return objs.Receipt(success=False, error_message=f"scalar must be > 0, got {scalar}")

        if preset and preset in PRESETS:
            width, height = PRESETS[preset]

        if scalar is None and width is None and height is None and not preset:
            return objs.Receipt(success=False, error_message="Must specify scalar, preset, width, or height")

        src_ext = os.path.splitext(in_key)[1].lower()
        is_image = src_ext in IMAGE_EXTS

        out_ext = src_ext or (".png" if is_image else ".mp4")
        if out_format:
            of = out_format.lower().lstrip(".")
            out_ext = f".{of}"
            # An explicit image output format means treat the job as an image job.
            if f".{of}" in IMAGE_EXTS:
                is_image = True

        local_input = None
        local_output = None
        try:
            local_input = self.download_file(in_key)
            fd, local_output = tempfile.mkstemp(suffix=out_ext)
            os.close(fd)

            if scalar is not None:
                # trunc to even dims — required by yuv420p / libx264 and harmless for images
                vf = f"scale=trunc(iw*{scalar}/2)*2:trunc(ih*{scalar}/2)*2"
            elif width and height:
                vf = f"scale={width}:{height}"
            elif width:
                vf = f"scale={width}:-2"
            else:
                vf = f"scale=-2:{height}"

            args = ["-i", local_input, "-vf", vf]
            if is_image:
                args += ["-frames:v", "1"]
            else:
                args += ["-c:v", "libx264", "-crf", "23", "-c:a", "copy"]
            args.append(local_output)

            proc = self.run_ffmpeg(args, check=False)
            if proc.returncode != 0:
                return objs.Receipt(success=False, error_message=f"ffmpeg failed: {(proc.stderr or '')[-1000:]}")

            suffix = (f"r{scalar}" if scalar is not None
                      else preset if preset
                      else f"{width or 'auto'}x{height or 'auto'}")
            new_ext = out_ext if out_format else None
            output_key = self.get_output_key(in_key, suffix, new_ext)
            self.upload_file(local_output, output_key)

            return objs.Receipt(
                success=True, primary_output='file',
                outputs={'file': objs.AnyType(ptype=objs.ParameterType.STRING, sval=output_key)},
            )
        except Exception as e:
            return objs.Receipt(success=False, error_message=str(e))
        finally:
            self.cleanup(local_input, local_output)
