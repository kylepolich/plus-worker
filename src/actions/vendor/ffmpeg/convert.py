"""FFMPEG Convert action - convert media between formats."""
from typing import Any, Dict, List

from src.actions.vendor.ffmpeg.base import FFMPEGAction


class Convert(FFMPEGAction):
    """Convert media file to a different format."""

    action_id = "ffmpeg.convert"
    label = "Convert Format"
    short_desc = "Convert media file to MP4, MP3, WAV, WebM, or other formats"
    icon = "refresh"

    @property
    def params(self) -> List[Dict[str, Any]]:
        return [
            {
                "var_name": "file",
                "label": "Input File",
                "ptype": "STRING",
                "hint": "S3 key of the media file to convert",
            },
            {
                "var_name": "format",
                "label": "Output Format",
                "ptype": "STRING",
                "hint": "Target format: mp4, mp3, wav, webm, avi, mkv, flac, ogg",
                "svals": [
                    {"label": "MP4", "value": "mp4"},
                    {"label": "MP3", "value": "mp3"},
                    {"label": "WAV", "value": "wav"},
                    {"label": "WebM", "value": "webm"},
                    {"label": "AVI", "value": "avi"},
                    {"label": "MKV", "value": "mkv"},
                    {"label": "FLAC", "value": "flac"},
                    {"label": "OGG", "value": "ogg"},
                ],
            },
        ]

    @property
    def outputs(self) -> List[Dict[str, Any]]:
        return [
            {"var_name": "file", "label": "Converted File", "ptype": "STRING"},
        ]

    def execute(self, dao, inputs: Dict[str, Any]) -> Dict[str, Any]:
        file_key = inputs["file"]
        target_format = inputs["format"].lower()

        local_input = None
        local_output = None

        try:
            # Download input
            local_input = self.download_file(dao, file_key)

            # Prepare output path
            import tempfile
            local_output = tempfile.mktemp(suffix=f".{target_format}")

            # Build ffmpeg command based on format
            args = ["-i", local_input]

            # Format-specific encoding options
            if target_format == "mp4":
                args += ["-c:v", "libx264", "-c:a", "aac", "-movflags", "+faststart"]
            elif target_format == "webm":
                args += ["-c:v", "libvpx-vp9", "-c:a", "libopus"]
            elif target_format == "mp3":
                args += ["-vn", "-c:a", "libmp3lame", "-q:a", "2"]
            elif target_format == "wav":
                args += ["-vn", "-c:a", "pcm_s16le"]
            elif target_format == "flac":
                args += ["-vn", "-c:a", "flac"]
            elif target_format == "ogg":
                args += ["-vn", "-c:a", "libvorbis", "-q:a", "4"]

            args.append(local_output)

            # Run conversion
            self.run_ffmpeg(args)

            # Upload result
            output_key = self.get_output_key(file_key, "converted", target_format)
            self.upload_file(dao, local_output, output_key)

            return {"file": output_key}

        finally:
            self.cleanup(local_input, local_output)
