"""FFMPEG Trim action - cut a segment from media file."""
from typing import Any, Dict, List

from src.actions.vendor.ffmpeg.base import FFMPEGAction


class Trim(FFMPEGAction):
    """Trim a media file to a specific time range."""

    action_id = "ffmpeg.trim"
    label = "Trim"
    short_desc = "Cut a segment from a media file by specifying start and end times"
    icon = "cut"

    @property
    def params(self) -> List[Dict[str, Any]]:
        return [
            {
                "var_name": "file",
                "label": "Media File",
                "ptype": "STRING",
                "hint": "S3 key of the media file to trim",
            },
            {
                "var_name": "start_time",
                "label": "Start Time",
                "ptype": "STRING",
                "hint": "Start time in seconds or HH:MM:SS format",
                "sdefault": "0",
            },
            {
                "var_name": "end_time",
                "label": "End Time",
                "ptype": "STRING",
                "hint": "End time in seconds or HH:MM:SS format (leave empty for end of file)",
                "optional": True,
            },
            {
                "var_name": "duration",
                "label": "Duration",
                "ptype": "STRING",
                "hint": "Duration in seconds (alternative to end_time)",
                "optional": True,
            },
        ]

    @property
    def outputs(self) -> List[Dict[str, Any]]:
        return [
            {"var_name": "file", "label": "Trimmed File", "ptype": "STRING"},
        ]

    def execute(self, dao, inputs: Dict[str, Any]) -> Dict[str, Any]:
        file_key = inputs["file"]
        start_time = inputs.get("start_time", "0")
        end_time = inputs.get("end_time")
        duration = inputs.get("duration")

        local_input = None
        local_output = None

        try:
            # Download input
            local_input = self.download_file(dao, file_key)

            # Prepare output path (same format as input)
            import os
            import tempfile
            ext = os.path.splitext(file_key)[1] or ".mp4"
            local_output = tempfile.mktemp(suffix=ext)

            # Build ffmpeg command
            args = ["-ss", str(start_time), "-i", local_input]

            # Add end time or duration
            if duration:
                args += ["-t", str(duration)]
            elif end_time:
                args += ["-to", str(end_time)]

            # Copy streams without re-encoding for speed
            args += ["-c", "copy", "-avoid_negative_ts", "make_zero"]
            args.append(local_output)

            # Run trim
            self.run_ffmpeg(args)

            # Upload result
            output_key = self.get_output_key(file_key, "trimmed")
            self.upload_file(dao, local_output, output_key)

            return {"file": output_key}

        finally:
            self.cleanup(local_input, local_output)
