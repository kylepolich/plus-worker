"""FFMPEG Overlay action - add watermark or image overlay to video."""
import os
import tempfile

import feaas.objects as objs
from src.actions.vendor.ffmpeg.base import FFMPEGAction


class Overlay(FFMPEGAction):
    """Add image overlay (watermark/logo) to video."""

    def __init__(self, dao):
        params = [
            objs.Parameter(var_name='video_file', label='Video File', ptype=objs.ParameterType.STRING),
            objs.Parameter(var_name='image_file', label='Overlay Image', ptype=objs.ParameterType.STRING),
            objs.Parameter(var_name='position', label='Position', ptype=objs.ParameterType.STRING),
            objs.Parameter(var_name='padding', label='Padding', ptype=objs.ParameterType.INTEGER),
            objs.Parameter(var_name='opacity', label='Opacity', ptype=objs.ParameterType.FLOAT),
            objs.Parameter(var_name='scale', label='Scale', ptype=objs.ParameterType.FLOAT),
        ]
        outputs = [
            objs.Parameter(var_name='file', label='Video with Overlay', ptype=objs.ParameterType.STRING),
        ]
        super().__init__(dao, params, outputs)

    def execute_action(self, video_file, image_file, position='bottomright',
                       padding=10, opacity=1.0, scale=1.0) -> objs.Receipt:
        local_video = None
        local_image = None
        local_output = None

        try:
            local_video = self.download_file(video_file)
            local_image = self.download_file(image_file)

            ext = os.path.splitext(video_file)[1] or ".mp4"
            fd, local_output = tempfile.mkstemp(suffix=ext)
            os.close(fd)

            positions = {
                "topleft": f"{padding}:{padding}",
                "topright": f"W-w-{padding}:{padding}",
                "bottomleft": f"{padding}:H-h-{padding}",
                "bottomright": f"W-w-{padding}:H-h-{padding}",
                "center": "(W-w)/2:(H-h)/2",
            }
            pos = positions.get(position, positions["bottomright"])

            filter_parts = []
            overlay_input = "[1:v]"

            if scale != 1.0:
                filter_parts.append(f"[1:v]scale=iw*{scale}:ih*{scale}[scaled]")
                overlay_input = "[scaled]"

            if opacity < 1.0:
                if scale != 1.0:
                    filter_parts.append(f"{overlay_input}format=rgba,colorchannelmixer=aa={opacity}[img]")
                else:
                    filter_parts.append(f"[1:v]format=rgba,colorchannelmixer=aa={opacity}[img]")
                overlay_input = "[img]"

            filter_parts.append(f"[0:v]{overlay_input}overlay={pos}")
            filter_complex = ";".join(filter_parts) if filter_parts else f"[0:v][1:v]overlay={pos}"

            args = ["-i", local_video, "-i", local_image, "-filter_complex", filter_complex,
                    "-c:a", "copy", local_output]
            self.run_ffmpeg(args)

            output_key = self.get_output_key(video_file, "watermarked")
            self.upload_file(local_output, output_key)

            return objs.Receipt(
                success=True, primary_output='file',
                outputs={'file': objs.AnyType(ptype=objs.ParameterType.STRING, sval=output_key)}
            )

        except Exception as e:
            return objs.Receipt(success=False, error_message=str(e))
        finally:
            self.cleanup(local_video, local_image, local_output)
