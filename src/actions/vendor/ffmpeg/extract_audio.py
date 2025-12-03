"""FFMPEG Extract Audio action - extract audio track from video."""
from typing import Any, Dict, List

from src.actions.vendor.ffmpeg.base import FFMPEGAction


class ExtractAudio(FFMPEGAction):
    """Extract audio track from a video file."""

    action_id = "ffmpeg.extract_audio"
    label = "Extract Audio"
    short_desc = "Extract the audio track from a video file"
    icon = "music"

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
                "var_name": "format",
                "label": "Audio Format",
                "ptype": "STRING",
                "hint": "Output audio format",
                "sdefault": "mp3",
                "svals": [
                    {"label": "MP3", "value": "mp3"},
                    {"label": "WAV", "value": "wav"},
                    {"label": "AAC", "value": "aac"},
                    {"label": "FLAC", "value": "flac"},
                    {"label": "OGG", "value": "ogg"},
                ],
            },
        ]

    @property
    def outputs(self) -> List[Dict[str, Any]]:
        return [
            {"var_name": "audio_file", "label": "Audio File", "ptype": "STRING"},
        ]

    def execute(self, dao, inputs: Dict[str, Any]) -> Dict[str, Any]:
        video_key = inputs["video_file"]
        audio_format = inputs.get("format", "mp3").lower()

        local_input = None
        local_output = None

        try:
            # Download video
            local_input = self.download_file(dao, video_key)

            # Prepare output path
            import tempfile
            local_output = tempfile.mktemp(suffix=f".{audio_format}")

            # Build ffmpeg command
            args = ["-i", local_input, "-vn"]  # -vn = no video

            # Format-specific encoding
            if audio_format == "mp3":
                args += ["-c:a", "libmp3lame", "-q:a", "2"]
            elif audio_format == "wav":
                args += ["-c:a", "pcm_s16le"]
            elif audio_format == "aac":
                args += ["-c:a", "aac", "-b:a", "192k"]
            elif audio_format == "flac":
                args += ["-c:a", "flac"]
            elif audio_format == "ogg":
                args += ["-c:a", "libvorbis", "-q:a", "4"]

            args.append(local_output)

            # Run extraction
            self.run_ffmpeg(args)

            # Upload result
            output_key = self.get_output_key(video_key, "audio", audio_format)
            self.upload_file(dao, local_output, output_key)

            return {"audio_file": output_key}

        finally:
            self.cleanup(local_input, local_output)
