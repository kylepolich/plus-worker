"""FFMPEG Normalize Audio action - normalize audio levels."""
from typing import Any, Dict, List

from src.actions.vendor.ffmpeg.base import FFMPEGAction


class NormalizeAudio(FFMPEGAction):
    """Normalize audio levels in a media file."""

    action_id = "ffmpeg.normalize_audio"
    label = "Normalize Audio"
    short_desc = "Normalize audio levels to a consistent volume"
    icon = "volume"

    @property
    def params(self) -> List[Dict[str, Any]]:
        return [
            {
                "var_name": "file",
                "label": "Media File",
                "ptype": "STRING",
                "hint": "S3 key of the audio or video file",
            },
            {
                "var_name": "target_level",
                "label": "Target Level (dB)",
                "ptype": "DOUBLE",
                "hint": "Target loudness in dB (default: -16 for podcasts, -14 for music)",
                "ddefault": -16.0,
            },
            {
                "var_name": "method",
                "label": "Normalization Method",
                "ptype": "STRING",
                "hint": "Type of normalization to apply",
                "sdefault": "loudnorm",
                "svals": [
                    {"label": "EBU R128 (loudnorm)", "value": "loudnorm"},
                    {"label": "Peak Normalization", "value": "peak"},
                    {"label": "RMS Normalization", "value": "rms"},
                ],
            },
        ]

    @property
    def outputs(self) -> List[Dict[str, Any]]:
        return [
            {"var_name": "file", "label": "Normalized File", "ptype": "STRING"},
        ]

    def execute(self, dao, inputs: Dict[str, Any]) -> Dict[str, Any]:
        file_key = inputs["file"]
        target_level = inputs.get("target_level", -16.0)
        method = inputs.get("method", "loudnorm")

        local_input = None
        local_output = None

        try:
            # Download input
            local_input = self.download_file(dao, file_key)

            import os
            import tempfile
            ext = os.path.splitext(file_key)[1] or ".mp4"
            local_output = tempfile.mktemp(suffix=ext)

            # Build audio filter based on method
            if method == "loudnorm":
                # EBU R128 loudness normalization (two-pass for best results)
                # First pass: analyze
                analyze_result = self.run_ffmpeg([
                    "-i", local_input,
                    "-af", f"loudnorm=I={target_level}:TP=-1.5:LRA=11:print_format=json",
                    "-f", "null",
                    "-"
                ], check=False)

                # Parse measured values from stderr
                import json
                import re
                stderr = analyze_result.stderr

                # Find JSON in output
                json_match = re.search(r'\{[^{}]+\}', stderr, re.DOTALL)
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
                    # Fallback to single-pass
                    audio_filter = f"loudnorm=I={target_level}:TP=-1.5:LRA=11"

            elif method == "peak":
                # Peak normalization
                audio_filter = f"volume=replaygain=peak"
            else:  # rms
                # RMS normalization - analyze first
                analyze_result = self.run_ffmpeg([
                    "-i", local_input,
                    "-af", "volumedetect",
                    "-f", "null",
                    "-"
                ], check=False)

                # Parse mean volume
                import re
                match = re.search(r'mean_volume:\s*([-\d.]+)\s*dB', analyze_result.stderr)
                if match:
                    current_mean = float(match.group(1))
                    adjustment = target_level - current_mean
                    audio_filter = f"volume={adjustment}dB"
                else:
                    audio_filter = f"volume=0dB"  # No change if can't analyze

            # Determine if input has video
            probe_result = self.run_ffprobe([
                "-v", "quiet",
                "-print_format", "json",
                "-show_streams",
                local_input
            ])
            import json
            streams = json.loads(probe_result.stdout).get("streams", [])
            has_video = any(s.get("codec_type") == "video" for s in streams)

            # Build ffmpeg command
            if has_video:
                args = [
                    "-i", local_input,
                    "-af", audio_filter,
                    "-c:v", "copy",
                    "-c:a", "aac",
                    "-b:a", "192k",
                    local_output
                ]
            else:
                args = [
                    "-i", local_input,
                    "-af", audio_filter,
                    "-c:a", "libmp3lame" if ext == ".mp3" else "aac",
                    "-b:a", "192k",
                    local_output
                ]

            # Run normalization
            self.run_ffmpeg(args)

            # Upload result
            output_key = self.get_output_key(file_key, "normalized")
            self.upload_file(dao, local_output, output_key)

            return {"file": output_key}

        finally:
            self.cleanup(local_input, local_output)
