"""FFMPEG Overlay action - add watermark or image overlay to video."""
from typing import Any, Dict, List

from src.actions.vendor.ffmpeg.base import FFMPEGAction


class Overlay(FFMPEGAction):
    """Add image overlay (watermark/logo) to video."""

    action_id = "ffmpeg.overlay"
    label = "Add Overlay"
    short_desc = "Add a watermark, logo, or image overlay to a video"
    icon = "layers"

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
                "var_name": "image_file",
                "label": "Overlay Image",
                "ptype": "STRING",
                "hint": "S3 key of the image to overlay (PNG with transparency recommended)",
            },
            {
                "var_name": "position",
                "label": "Position",
                "ptype": "STRING",
                "hint": "Where to place the overlay",
                "sdefault": "bottomright",
                "svals": [
                    {"label": "Top Left", "value": "topleft"},
                    {"label": "Top Right", "value": "topright"},
                    {"label": "Bottom Left", "value": "bottomleft"},
                    {"label": "Bottom Right", "value": "bottomright"},
                    {"label": "Center", "value": "center"},
                ],
            },
            {
                "var_name": "padding",
                "label": "Padding",
                "ptype": "INTEGER",
                "hint": "Padding from edges in pixels",
                "idefault": 10,
            },
            {
                "var_name": "opacity",
                "label": "Opacity",
                "ptype": "DOUBLE",
                "hint": "Overlay opacity (0.0 - 1.0)",
                "ddefault": 1.0,
            },
            {
                "var_name": "scale",
                "label": "Scale",
                "ptype": "DOUBLE",
                "hint": "Scale factor for overlay (1.0 = original size)",
                "ddefault": 1.0,
            },
        ]

    @property
    def outputs(self) -> List[Dict[str, Any]]:
        return [
            {"var_name": "file", "label": "Video with Overlay", "ptype": "STRING"},
        ]

    def execute(self, dao, inputs: Dict[str, Any]) -> Dict[str, Any]:
        video_key = inputs["video_file"]
        image_key = inputs["image_file"]
        position = inputs.get("position", "bottomright")
        padding = inputs.get("padding", 10)
        opacity = inputs.get("opacity", 1.0)
        scale = inputs.get("scale", 1.0)

        local_video = None
        local_image = None
        local_output = None

        try:
            # Download files
            local_video = self.download_file(dao, video_key)
            local_image = self.download_file(dao, image_key)

            import os
            import tempfile
            ext = os.path.splitext(video_key)[1] or ".mp4"
            local_output = tempfile.mktemp(suffix=ext)

            # Build position expression
            # W, H = video width/height; w, h = overlay width/height
            positions = {
                "topleft": f"{padding}:{padding}",
                "topright": f"W-w-{padding}:{padding}",
                "bottomleft": f"{padding}:H-h-{padding}",
                "bottomright": f"W-w-{padding}:H-h-{padding}",
                "center": "(W-w)/2:(H-h)/2",
            }
            pos = positions.get(position, positions["bottomright"])

            # Build filter graph
            filter_parts = []

            # Scale overlay if needed
            if scale != 1.0:
                filter_parts.append(f"[1:v]scale=iw*{scale}:ih*{scale}[scaled]")
                overlay_input = "[scaled]"
            else:
                overlay_input = "[1:v]"

            # Apply opacity if needed
            if opacity < 1.0:
                if scale != 1.0:
                    filter_parts.append(f"{overlay_input}format=rgba,colorchannelmixer=aa={opacity}[img]")
                else:
                    filter_parts.append(f"[1:v]format=rgba,colorchannelmixer=aa={opacity}[img]")
                overlay_input = "[img]"

            # Overlay
            filter_parts.append(f"[0:v]{overlay_input}overlay={pos}")

            filter_complex = ";".join(filter_parts) if filter_parts else f"[0:v][1:v]overlay={pos}"

            # Build ffmpeg command
            args = [
                "-i", local_video,
                "-i", local_image,
                "-filter_complex", filter_complex,
                "-c:a", "copy",
                local_output
            ]

            # Run overlay
            self.run_ffmpeg(args)

            # Upload result
            output_key = self.get_output_key(video_key, "watermarked")
            self.upload_file(dao, local_output, output_key)

            return {"file": output_key}

        finally:
            self.cleanup(local_video, local_image, local_output)
