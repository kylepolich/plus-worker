"""FFMPEG Probe action - get media file metadata."""
import json
from typing import Any, Dict, List

from src.actions.vendor.ffmpeg.base import FFMPEGAction


class Probe(FFMPEGAction):
    """Get metadata from a media file using ffprobe."""

    action_id = "ffmpeg.probe"
    label = "Probe Media"
    short_desc = "Get duration, dimensions, codec, and other metadata from a media file"
    icon = "info"

    @property
    def params(self) -> List[Dict[str, Any]]:
        return [
            {
                "var_name": "file",
                "label": "Media File",
                "ptype": "STRING",
                "hint": "S3 key of the media file to probe",
            }
        ]

    @property
    def outputs(self) -> List[Dict[str, Any]]:
        return [
            {"var_name": "duration", "label": "Duration (seconds)", "ptype": "DOUBLE"},
            {"var_name": "width", "label": "Width", "ptype": "INTEGER"},
            {"var_name": "height", "label": "Height", "ptype": "INTEGER"},
            {"var_name": "codec", "label": "Codec", "ptype": "STRING"},
            {"var_name": "audio_codec", "label": "Audio Codec", "ptype": "STRING"},
            {"var_name": "fps", "label": "Frame Rate", "ptype": "DOUBLE"},
            {"var_name": "bitrate", "label": "Bitrate (kbps)", "ptype": "INTEGER"},
            {"var_name": "format", "label": "Format", "ptype": "STRING"},
        ]

    def execute(self, dao, inputs: Dict[str, Any]) -> Dict[str, Any]:
        file_key = inputs["file"]
        local_path = None

        try:
            # Download file
            local_path = self.download_file(dao, file_key)

            # Run ffprobe
            result = self.run_ffprobe([
                "-v", "quiet",
                "-print_format", "json",
                "-show_format",
                "-show_streams",
                local_path
            ])

            data = json.loads(result.stdout)

            # Extract info
            format_info = data.get("format", {})
            video_stream = None
            audio_stream = None

            for stream in data.get("streams", []):
                if stream.get("codec_type") == "video" and not video_stream:
                    video_stream = stream
                elif stream.get("codec_type") == "audio" and not audio_stream:
                    audio_stream = stream

            # Build output
            output = {
                "duration": float(format_info.get("duration", 0)),
                "format": format_info.get("format_name", ""),
                "bitrate": int(format_info.get("bit_rate", 0)) // 1000,
                "width": 0,
                "height": 0,
                "codec": "",
                "audio_codec": "",
                "fps": 0.0,
            }

            if video_stream:
                output["width"] = video_stream.get("width", 0)
                output["height"] = video_stream.get("height", 0)
                output["codec"] = video_stream.get("codec_name", "")
                # Parse fps from r_frame_rate (e.g., "30/1")
                fps_str = video_stream.get("r_frame_rate", "0/1")
                if "/" in fps_str:
                    num, den = fps_str.split("/")
                    output["fps"] = float(num) / float(den) if float(den) > 0 else 0

            if audio_stream:
                output["audio_codec"] = audio_stream.get("codec_name", "")

            return output

        finally:
            self.cleanup(local_path)
