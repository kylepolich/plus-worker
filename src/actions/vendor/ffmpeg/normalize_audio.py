"""FFMPEG Normalize Audio action - normalize audio levels."""
import json
import os
import re
import tempfile

import feaas.objects as objs
from src.actions.vendor.ffmpeg.base import FFMPEGAction


class NormalizeAudio(FFMPEGAction):
    """Normalize audio levels in a media file."""

    def __init__(self, dao):
        params = [
            objs.Parameter(var_name='file', label='Media File', ptype=objs.ParameterType.STRING),
            objs.Parameter(var_name='target_level', label='Target Level (dB)', ptype=objs.ParameterType.FLOAT),
            objs.Parameter(var_name='method', label='Normalization Method', ptype=objs.ParameterType.STRING),
        ]
        outputs = [
            objs.Parameter(var_name='file', label='Normalized File', ptype=objs.ParameterType.STRING),
        ]
        super().__init__(dao, params, outputs)

    def execute_action(self, file, target_level=-16.0, method='loudnorm') -> objs.Receipt:
        local_input = None
        local_output = None

        try:
            local_input = self.download_file(file)

            ext = os.path.splitext(file)[1] or ".mp4"
            fd, local_output = tempfile.mkstemp(suffix=ext)
            os.close(fd)

            if method == "loudnorm":
                analyze_result = self.run_ffmpeg(
                    ["-i", local_input, "-af", f"loudnorm=I={target_level}:TP=-1.5:LRA=11:print_format=json",
                     "-f", "null", "-"], check=False)

                json_match = re.search(r'\{[^{}]+\}', analyze_result.stderr, re.DOTALL)
                if json_match:
                    measured = json.loads(json_match.group())
                    audio_filter = (
                        f"loudnorm=I={target_level}:TP=-1.5:LRA=11:"
                        f"measured_I={measured.get('input_i', -24)}:"
                        f"measured_TP={measured.get('input_tp', -1)}:"
                        f"measured_LRA={measured.get('input_lra', 7)}:"
                        f"measured_thresh={measured.get('input_thresh', -34)}:"
                        f"offset={measured.get('target_offset', 0)}:linear=true"
                    )
                else:
                    audio_filter = f"loudnorm=I={target_level}:TP=-1.5:LRA=11"

            elif method == "peak":
                audio_filter = "volume=replaygain=peak"
            else:  # rms
                analyze_result = self.run_ffmpeg(["-i", local_input, "-af", "volumedetect", "-f", "null", "-"],
                                                  check=False)
                match = re.search(r'mean_volume:\s*([-\d.]+)\s*dB', analyze_result.stderr)
                if match:
                    adjustment = target_level - float(match.group(1))
                    audio_filter = f"volume={adjustment}dB"
                else:
                    audio_filter = "volume=0dB"

            probe_result = self.run_ffprobe(["-v", "quiet", "-print_format", "json", "-show_streams", local_input])
            streams = json.loads(probe_result.stdout).get("streams", [])
            has_video = any(s.get("codec_type") == "video" for s in streams)

            if has_video:
                args = ["-i", local_input, "-af", audio_filter, "-c:v", "copy", "-c:a", "aac", "-b:a", "192k", local_output]
            else:
                codec = "libmp3lame" if ext == ".mp3" else "aac"
                args = ["-i", local_input, "-af", audio_filter, "-c:a", codec, "-b:a", "192k", local_output]

            self.run_ffmpeg(args)

            output_key = self.get_output_key(file, "normalized")
            self.upload_file(local_output, output_key)

            return objs.Receipt(
                success=True, primary_output='file',
                outputs={'file': objs.AnyType(ptype=objs.ParameterType.STRING, sval=output_key)}
            )

        except Exception as e:
            return objs.Receipt(success=False, error_message=str(e))
        finally:
            self.cleanup(local_input, local_output)
