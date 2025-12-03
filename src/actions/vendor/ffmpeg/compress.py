"""FFMPEG Compress action - reduce file size."""
from typing import Any, Dict, List

from src.actions.vendor.ffmpeg.base import FFMPEGAction


class Compress(FFMPEGAction):
    """Compress video to reduce file size."""

    action_id = "ffmpeg.compress"
    label = "Compress Video"
    short_desc = "Reduce video file size with quality presets or target size"
    icon = "compress"

    @property
    def params(self) -> List[Dict[str, Any]]:
        return [
            {
                "var_name": "file",
                "label": "Video File",
                "ptype": "STRING",
                "hint": "S3 key of the video file to compress",
            },
            {
                "var_name": "quality",
                "label": "Quality Preset",
                "ptype": "STRING",
                "hint": "Quality level (higher = better quality, larger file)",
                "sdefault": "medium",
                "svals": [
                    {"label": "Low (smallest file)", "value": "low"},
                    {"label": "Medium (balanced)", "value": "medium"},
                    {"label": "High (best quality)", "value": "high"},
                ],
            },
            {
                "var_name": "target_size_mb",
                "label": "Target Size (MB)",
                "ptype": "DOUBLE",
                "hint": "Target file size in MB (overrides quality preset)",
                "optional": True,
            },
        ]

    @property
    def outputs(self) -> List[Dict[str, Any]]:
        return [
            {"var_name": "file", "label": "Compressed Video", "ptype": "STRING"},
            {"var_name": "size_reduction", "label": "Size Reduction %", "ptype": "DOUBLE"},
        ]

    def execute(self, dao, inputs: Dict[str, Any]) -> Dict[str, Any]:
        file_key = inputs["file"]
        quality = inputs.get("quality", "medium")
        target_size_mb = inputs.get("target_size_mb")

        # CRF values (lower = better quality, larger file)
        crf_values = {
            "low": 32,
            "medium": 26,
            "high": 20,
        }

        local_input = None
        local_output = None

        try:
            # Download input
            local_input = self.download_file(dao, file_key)

            import os
            import tempfile

            original_size = os.path.getsize(local_input)
            ext = os.path.splitext(file_key)[1] or ".mp4"
            local_output = tempfile.mktemp(suffix=ext)

            if target_size_mb:
                # Two-pass encoding for target size
                # First, get duration
                import json
                probe_result = self.run_ffprobe([
                    "-v", "quiet",
                    "-print_format", "json",
                    "-show_format",
                    local_input
                ])
                data = json.loads(probe_result.stdout)
                duration = float(data["format"].get("duration", 60))

                # Calculate target bitrate (bits per second)
                # Reserve 128kbps for audio
                target_bits = target_size_mb * 8 * 1024 * 1024
                audio_bits = 128 * 1000 * duration
                video_bitrate = int((target_bits - audio_bits) / duration)
                video_bitrate = max(video_bitrate, 100000)  # Minimum 100kbps

                # Two-pass encoding
                # Pass 1
                self.run_ffmpeg([
                    "-i", local_input,
                    "-c:v", "libx264",
                    "-b:v", str(video_bitrate),
                    "-pass", "1",
                    "-an",
                    "-f", "null",
                    "/dev/null"
                ])

                # Pass 2
                args = [
                    "-i", local_input,
                    "-c:v", "libx264",
                    "-b:v", str(video_bitrate),
                    "-pass", "2",
                    "-c:a", "aac",
                    "-b:a", "128k",
                    local_output
                ]
            else:
                # CRF-based encoding
                crf = crf_values.get(quality, 26)
                args = [
                    "-i", local_input,
                    "-c:v", "libx264",
                    "-crf", str(crf),
                    "-preset", "medium",
                    "-c:a", "aac",
                    "-b:a", "128k",
                    local_output
                ]

            # Run compression
            self.run_ffmpeg(args)

            # Calculate size reduction
            compressed_size = os.path.getsize(local_output)
            reduction = ((original_size - compressed_size) / original_size) * 100

            # Upload result
            output_key = self.get_output_key(file_key, "compressed")
            self.upload_file(dao, local_output, output_key)

            return {
                "file": output_key,
                "size_reduction": round(reduction, 1),
            }

        finally:
            self.cleanup(local_input, local_output)
            # Clean up two-pass log files
            for f in ["ffmpeg2pass-0.log", "ffmpeg2pass-0.log.mbtree"]:
                self.cleanup(f)
