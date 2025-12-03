"""FFMPEG Mix Audio action - combine audio tracks."""
from typing import Any, Dict, List

from src.actions.vendor.ffmpeg.base import FFMPEGAction


class MixAudio(FFMPEGAction):
    """Mix an audio track into a video or audio file."""

    action_id = "ffmpeg.mix_audio"
    label = "Mix Audio"
    short_desc = "Add background music or mix audio tracks together"
    icon = "sliders"

    @property
    def params(self) -> List[Dict[str, Any]]:
        return [
            {
                "var_name": "file",
                "label": "Main File",
                "ptype": "STRING",
                "hint": "S3 key of the main video or audio file",
            },
            {
                "var_name": "audio_track",
                "label": "Audio Track to Mix",
                "ptype": "STRING",
                "hint": "S3 key of the audio file to mix in (e.g., background music)",
            },
            {
                "var_name": "main_volume",
                "label": "Main Volume",
                "ptype": "DOUBLE",
                "hint": "Volume of main audio (1.0 = original)",
                "ddefault": 1.0,
            },
            {
                "var_name": "mix_volume",
                "label": "Mix Volume",
                "ptype": "DOUBLE",
                "hint": "Volume of mixed audio (0.2-0.4 typical for background)",
                "ddefault": 0.3,
            },
            {
                "var_name": "loop_audio",
                "label": "Loop Audio Track",
                "ptype": "BOOLEAN",
                "hint": "Loop the audio track if shorter than main file",
                "bdefault": True,
            },
            {
                "var_name": "mix_mode",
                "label": "Mix Mode",
                "ptype": "STRING",
                "hint": "How to combine the audio tracks",
                "sdefault": "mix",
                "svals": [
                    {"label": "Mix (combine both)", "value": "mix"},
                    {"label": "Replace (use mix track only)", "value": "replace"},
                    {"label": "Ducking (lower mix when main has audio)", "value": "ducking"},
                ],
            },
        ]

    @property
    def outputs(self) -> List[Dict[str, Any]]:
        return [
            {"var_name": "file", "label": "Mixed File", "ptype": "STRING"},
        ]

    def execute(self, dao, inputs: Dict[str, Any]) -> Dict[str, Any]:
        main_key = inputs["file"]
        audio_key = inputs["audio_track"]
        main_volume = inputs.get("main_volume", 1.0)
        mix_volume = inputs.get("mix_volume", 0.3)
        loop_audio = inputs.get("loop_audio", True)
        mix_mode = inputs.get("mix_mode", "mix")

        local_main = None
        local_audio = None
        local_output = None

        try:
            # Download files
            local_main = self.download_file(dao, main_key)
            local_audio = self.download_file(dao, audio_key)

            import os
            import tempfile
            ext = os.path.splitext(main_key)[1] or ".mp4"
            local_output = tempfile.mktemp(suffix=ext)

            # Get duration of main file
            import json
            probe_result = self.run_ffprobe([
                "-v", "quiet",
                "-print_format", "json",
                "-show_format",
                "-show_streams",
                local_main
            ])
            main_data = json.loads(probe_result.stdout)
            main_duration = float(main_data["format"].get("duration", 0))
            has_video = any(s.get("codec_type") == "video" for s in main_data.get("streams", []))

            # Build input arguments
            input_args = ["-i", local_main]

            if loop_audio:
                input_args += ["-stream_loop", "-1", "-i", local_audio]
            else:
                input_args += ["-i", local_audio]

            # Build filter based on mix mode
            if mix_mode == "replace":
                # Just use the mix track, adjusted for volume
                filter_complex = f"[1:a]volume={mix_volume},atrim=0:{main_duration}[outa]"
                map_args = ["-map", "0:v?" if has_video else "-map", "-map", "[outa]"]
            elif mix_mode == "ducking":
                # Sidechain compression - lower mix when main has audio
                filter_complex = (
                    f"[0:a]volume={main_volume}[main];"
                    f"[1:a]volume={mix_volume},atrim=0:{main_duration}[bg];"
                    f"[bg][main]sidechaincompress=threshold=0.02:ratio=6:attack=200:release=1000[ducked];"
                    f"[main][ducked]amix=inputs=2:duration=first[outa]"
                )
                map_args = ["-map", "[outa]"]
                if has_video:
                    map_args = ["-map", "0:v"] + map_args
            else:  # mix
                # Simple mixing
                filter_complex = (
                    f"[0:a]volume={main_volume}[a0];"
                    f"[1:a]volume={mix_volume},atrim=0:{main_duration}[a1];"
                    f"[a0][a1]amix=inputs=2:duration=first:dropout_transition=2[outa]"
                )
                map_args = ["-map", "[outa]"]
                if has_video:
                    map_args = ["-map", "0:v"] + map_args

            # Build ffmpeg command
            args = input_args + [
                "-filter_complex", filter_complex,
            ] + map_args

            if has_video:
                args += ["-c:v", "copy"]
            args += ["-c:a", "aac", "-b:a", "192k", "-t", str(main_duration)]
            args.append(local_output)

            # Run mix
            self.run_ffmpeg(args)

            # Upload result
            output_key = self.get_output_key(main_key, "mixed")
            self.upload_file(dao, local_output, output_key)

            return {"file": output_key}

        finally:
            self.cleanup(local_main, local_audio, local_output)
