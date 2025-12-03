"""FFMPEG ToGif action - convert video to animated GIF."""
from typing import Any, Dict, List

from src.actions.vendor.ffmpeg.base import FFMPEGAction


class ToGif(FFMPEGAction):
    """Convert video segment to animated GIF."""

    action_id = "ffmpeg.to_gif"
    label = "Video to GIF"
    short_desc = "Create an animated GIF from a video file"
    icon = "image"

    @property
    def params(self) -> List[Dict[str, Any]]:
        return [
            {
                "var_name": "video_file",
                "label": "Video File",
                "ptype": "STRING",
                "hint": "S3 key of the video file",
            },
            {
                "var_name": "start_time",
                "label": "Start Time",
                "ptype": "STRING",
                "hint": "Start time in seconds or HH:MM:SS format",
                "sdefault": "0",
            },
            {
                "var_name": "duration",
                "label": "Duration",
                "ptype": "DOUBLE",
                "hint": "Duration in seconds (max recommended: 10)",
                "ddefault": 5.0,
            },
            {
                "var_name": "fps",
                "label": "Frame Rate",
                "ptype": "INTEGER",
                "hint": "Frames per second (10-15 recommended for file size)",
                "idefault": 10,
            },
            {
                "var_name": "width",
                "label": "Width",
                "ptype": "INTEGER",
                "hint": "Output width in pixels (height auto-calculated)",
                "idefault": 480,
            },
        ]

    @property
    def outputs(self) -> List[Dict[str, Any]]:
        return [
            {"var_name": "gif_file", "label": "GIF File", "ptype": "STRING"},
        ]

    def execute(self, dao, inputs: Dict[str, Any]) -> Dict[str, Any]:
        video_key = inputs["video_file"]
        start_time = inputs.get("start_time", "0")
        duration = inputs.get("duration", 5.0)
        fps = inputs.get("fps", 10)
        width = inputs.get("width", 480)

        local_input = None
        local_output = None
        palette_path = None

        try:
            # Download video
            local_input = self.download_file(dao, video_key)

            import tempfile
            local_output = tempfile.mktemp(suffix=".gif")
            palette_path = tempfile.mktemp(suffix=".png")

            # Build filter string
            filters = f"fps={fps},scale={width}:-1:flags=lanczos"

            # Two-pass for better quality GIFs
            # Pass 1: Generate palette
            self.run_ffmpeg([
                "-ss", str(start_time),
                "-t", str(duration),
                "-i", local_input,
                "-vf", f"{filters},palettegen=stats_mode=diff",
                "-y", palette_path
            ])

            # Pass 2: Generate GIF using palette
            self.run_ffmpeg([
                "-ss", str(start_time),
                "-t", str(duration),
                "-i", local_input,
                "-i", palette_path,
                "-lavfi", f"{filters}[x];[x][1:v]paletteuse=dither=bayer:bayer_scale=5:diff_mode=rectangle",
                "-y", local_output
            ])

            # Upload result
            output_key = self.get_output_key(video_key, "gif", ".gif")
            self.upload_file(dao, local_output, output_key)

            return {"gif_file": output_key}

        finally:
            self.cleanup(local_input, local_output, palette_path)
