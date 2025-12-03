"""FFMPEG Resize action - scale video dimensions."""
from typing import Any, Dict, List

from src.actions.vendor.ffmpeg.base import FFMPEGAction


class Resize(FFMPEGAction):
    """Resize video to different dimensions."""

    action_id = "ffmpeg.resize"
    label = "Resize Video"
    short_desc = "Scale video to a specific resolution or preset (720p, 1080p, 4K)"
    icon = "expand"

    @property
    def params(self) -> List[Dict[str, Any]]:
        return [
            {
                "var_name": "file",
                "label": "Video File",
                "ptype": "STRING",
                "hint": "S3 key of the video file to resize",
            },
            {
                "var_name": "preset",
                "label": "Resolution Preset",
                "ptype": "STRING",
                "hint": "Common resolution preset (overrides width/height)",
                "optional": True,
                "svals": [
                    {"label": "480p (SD)", "value": "480p"},
                    {"label": "720p (HD)", "value": "720p"},
                    {"label": "1080p (Full HD)", "value": "1080p"},
                    {"label": "1440p (2K)", "value": "1440p"},
                    {"label": "2160p (4K)", "value": "2160p"},
                ],
            },
            {
                "var_name": "width",
                "label": "Width",
                "ptype": "INTEGER",
                "hint": "Target width in pixels (-1 to auto-calculate from height)",
                "optional": True,
            },
            {
                "var_name": "height",
                "label": "Height",
                "ptype": "INTEGER",
                "hint": "Target height in pixels (-1 to auto-calculate from width)",
                "optional": True,
            },
        ]

    @property
    def outputs(self) -> List[Dict[str, Any]]:
        return [
            {"var_name": "file", "label": "Resized Video", "ptype": "STRING"},
        ]

    def execute(self, dao, inputs: Dict[str, Any]) -> Dict[str, Any]:
        file_key = inputs["file"]
        preset = inputs.get("preset")
        width = inputs.get("width")
        height = inputs.get("height")

        # Preset resolutions (height, maintaining aspect ratio)
        presets = {
            "480p": (None, 480),
            "720p": (None, 720),
            "1080p": (None, 1080),
            "1440p": (None, 1440),
            "2160p": (None, 2160),
        }

        if preset and preset in presets:
            width, height = presets[preset]

        if width is None and height is None:
            raise ValueError("Must specify preset, width, height, or both")

        local_input = None
        local_output = None

        try:
            # Download input
            local_input = self.download_file(dao, file_key)

            # Prepare output path
            import os
            import tempfile
            ext = os.path.splitext(file_key)[1] or ".mp4"
            local_output = tempfile.mktemp(suffix=ext)

            # Build scale filter
            if width and height:
                scale = f"scale={width}:{height}"
            elif width:
                scale = f"scale={width}:-2"  # -2 ensures even number for codec
            else:
                scale = f"scale=-2:{height}"

            # Build ffmpeg command
            args = [
                "-i", local_input,
                "-vf", scale,
                "-c:v", "libx264",
                "-crf", "23",
                "-c:a", "copy",
                local_output
            ]

            # Run resize
            self.run_ffmpeg(args)

            # Upload result
            suffix = preset if preset else f"{width or 'auto'}x{height or 'auto'}"
            output_key = self.get_output_key(file_key, suffix)
            self.upload_file(dao, local_output, output_key)

            return {"file": output_key}

        finally:
            self.cleanup(local_input, local_output)
