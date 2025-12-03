"""FFMPEG Probe action - get media file metadata."""
import json

import feaas.objects as objs
from src.actions.vendor.ffmpeg.base import FFMPEGAction


class Probe(FFMPEGAction):
    """Get metadata from a media file using ffprobe."""

    def __init__(self, dao):
        file_param = objs.Parameter(
            var_name='file',
            label='Media File',
            ptype=objs.ParameterType.STRING)

        params = [file_param]

        outputs = [
            objs.Parameter(var_name='duration', label='Duration (seconds)', ptype=objs.ParameterType.FLOAT),
            objs.Parameter(var_name='width', label='Width', ptype=objs.ParameterType.INTEGER),
            objs.Parameter(var_name='height', label='Height', ptype=objs.ParameterType.INTEGER),
            objs.Parameter(var_name='codec', label='Codec', ptype=objs.ParameterType.STRING),
            objs.Parameter(var_name='audio_codec', label='Audio Codec', ptype=objs.ParameterType.STRING),
            objs.Parameter(var_name='fps', label='Frame Rate', ptype=objs.ParameterType.FLOAT),
            objs.Parameter(var_name='bitrate', label='Bitrate (kbps)', ptype=objs.ParameterType.INTEGER),
            objs.Parameter(var_name='format', label='Format', ptype=objs.ParameterType.STRING),
        ]

        super().__init__(dao, params, outputs)

    def execute_action(self, file) -> objs.Receipt:
        local_path = None

        try:
            local_path = self.download_file(file)

            result = self.run_ffprobe([
                "-v", "quiet", "-print_format", "json",
                "-show_format", "-show_streams", local_path
            ])
            data = json.loads(result.stdout)

            format_info = data.get("format", {})
            video_stream = None
            audio_stream = None

            for stream in data.get("streams", []):
                if stream.get("codec_type") == "video" and not video_stream:
                    video_stream = stream
                elif stream.get("codec_type") == "audio" and not audio_stream:
                    audio_stream = stream

            duration = float(format_info.get("duration", 0))
            format_name = format_info.get("format_name", "")
            bitrate = int(format_info.get("bit_rate", 0)) // 1000
            width, height, codec, audio_codec, fps = 0, 0, "", "", 0.0

            if video_stream:
                width = video_stream.get("width", 0)
                height = video_stream.get("height", 0)
                codec = video_stream.get("codec_name", "")
                fps_str = video_stream.get("r_frame_rate", "0/1")
                if "/" in fps_str:
                    num, den = fps_str.split("/")
                    fps = float(num) / float(den) if float(den) > 0 else 0

            if audio_stream:
                audio_codec = audio_stream.get("codec_name", "")

            outputs = {
                'duration': objs.AnyType(ptype=objs.ParameterType.FLOAT, dval=duration),
                'width': objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=width),
                'height': objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=height),
                'codec': objs.AnyType(ptype=objs.ParameterType.STRING, sval=codec),
                'audio_codec': objs.AnyType(ptype=objs.ParameterType.STRING, sval=audio_codec),
                'fps': objs.AnyType(ptype=objs.ParameterType.FLOAT, dval=fps),
                'bitrate': objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=bitrate),
                'format': objs.AnyType(ptype=objs.ParameterType.STRING, sval=format_name),
            }

            return objs.Receipt(success=True, primary_output='duration', outputs=outputs)

        except Exception as e:
            return objs.Receipt(success=False, error_message=str(e))
        finally:
            self.cleanup(local_path)
