"""FFMPEG Concat action - join multiple media files."""
import os
import tempfile
from typing import Any, Dict, List

from src.actions.vendor.ffmpeg.base import FFMPEGAction


class Concat(FFMPEGAction):
    """Concatenate multiple media files into one."""

    action_id = "ffmpeg.concat"
    label = "Concatenate"
    short_desc = "Join multiple media files into a single file"
    icon = "link"

    @property
    def params(self) -> List[Dict[str, Any]]:
        return [
            {
                "var_name": "files",
                "label": "Media Files",
                "ptype": "STRING_LIST",
                "hint": "List of S3 keys of media files to concatenate (in order)",
            },
            {
                "var_name": "output_format",
                "label": "Output Format",
                "ptype": "STRING",
                "hint": "Output format (defaults to same as first input)",
                "optional": True,
            },
        ]

    @property
    def outputs(self) -> List[Dict[str, Any]]:
        return [
            {"var_name": "file", "label": "Concatenated File", "ptype": "STRING"},
        ]

    def execute(self, dao, inputs: Dict[str, Any]) -> Dict[str, Any]:
        file_keys = inputs["files"]
        output_format = inputs.get("output_format")

        if not file_keys or len(file_keys) == 0:
            raise ValueError("No files provided for concatenation")

        local_inputs = []
        local_output = None
        concat_list_path = None

        try:
            # Download all input files
            for key in file_keys:
                local_path = self.download_file(dao, key)
                local_inputs.append(local_path)

            # Determine output format
            if not output_format:
                output_format = os.path.splitext(file_keys[0])[1].lstrip('.') or "mp4"

            # Create concat list file
            fd, concat_list_path = tempfile.mkstemp(suffix=".txt")
            with os.fdopen(fd, 'w') as f:
                for path in local_inputs:
                    # Escape single quotes in path
                    escaped = path.replace("'", "'\\''")
                    f.write(f"file '{escaped}'\n")

            # Prepare output path
            local_output = tempfile.mktemp(suffix=f".{output_format}")

            # Build ffmpeg command using concat demuxer
            args = [
                "-f", "concat",
                "-safe", "0",
                "-i", concat_list_path,
                "-c", "copy",  # Copy streams without re-encoding
                local_output
            ]

            # Run concatenation
            self.run_ffmpeg(args)

            # Upload result
            output_key = self.get_output_key(file_keys[0], "concat", output_format)
            self.upload_file(dao, local_output, output_key)

            return {"file": output_key}

        finally:
            self.cleanup(local_output, concat_list_path, *local_inputs)
