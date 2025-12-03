"""FFMPEG Compress action - reduce file size."""
import json
import os
import tempfile

import feaas.objects as objs
from src.actions.vendor.ffmpeg.base import FFMPEGAction


class Compress(FFMPEGAction):
    """Compress video to reduce file size."""

    def __init__(self, dao):
        params = [
            objs.Parameter(var_name='file', label='Video File', ptype=objs.ParameterType.STRING),
            objs.Parameter(var_name='quality', label='Quality Preset', ptype=objs.ParameterType.STRING),
            objs.Parameter(var_name='target_size_mb', label='Target Size (MB)', ptype=objs.ParameterType.FLOAT),
        ]
        outputs = [
            objs.Parameter(var_name='file', label='Compressed Video', ptype=objs.ParameterType.STRING),
            objs.Parameter(var_name='size_reduction', label='Size Reduction %', ptype=objs.ParameterType.FLOAT),
        ]
        super().__init__(dao, params, outputs)

    def execute_action(self, file, quality='medium', target_size_mb=None) -> objs.Receipt:
        crf_values = {"low": 32, "medium": 26, "high": 20}
        local_input = None
        local_output = None

        try:
            local_input = self.download_file(file)
            original_size = os.path.getsize(local_input)

            ext = os.path.splitext(file)[1] or ".mp4"
            fd, local_output = tempfile.mkstemp(suffix=ext)
            os.close(fd)

            if target_size_mb:
                probe_result = self.run_ffprobe(["-v", "quiet", "-print_format", "json", "-show_format", local_input])
                data = json.loads(probe_result.stdout)
                duration = float(data["format"].get("duration", 60))

                target_bits = target_size_mb * 8 * 1024 * 1024
                audio_bits = 128 * 1000 * duration
                video_bitrate = max(int((target_bits - audio_bits) / duration), 100000)

                self.run_ffmpeg(["-i", local_input, "-c:v", "libx264", "-b:v", str(video_bitrate),
                                 "-pass", "1", "-an", "-f", "null", "/dev/null"])
                args = ["-i", local_input, "-c:v", "libx264", "-b:v", str(video_bitrate),
                        "-pass", "2", "-c:a", "aac", "-b:a", "128k", local_output]
            else:
                crf = crf_values.get(quality, 26)
                args = ["-i", local_input, "-c:v", "libx264", "-crf", str(crf),
                        "-preset", "medium", "-c:a", "aac", "-b:a", "128k", local_output]

            self.run_ffmpeg(args)

            compressed_size = os.path.getsize(local_output)
            reduction = ((original_size - compressed_size) / original_size) * 100

            output_key = self.get_output_key(file, "compressed")
            self.upload_file(local_output, output_key)

            return objs.Receipt(
                success=True, primary_output='file',
                outputs={
                    'file': objs.AnyType(ptype=objs.ParameterType.STRING, sval=output_key),
                    'size_reduction': objs.AnyType(ptype=objs.ParameterType.FLOAT, dval=round(reduction, 1)),
                }
            )

        except Exception as e:
            return objs.Receipt(success=False, error_message=str(e))
        finally:
            self.cleanup(local_input, local_output)
            for f in ["ffmpeg2pass-0.log", "ffmpeg2pass-0.log.mbtree"]:
                self.cleanup(f)
