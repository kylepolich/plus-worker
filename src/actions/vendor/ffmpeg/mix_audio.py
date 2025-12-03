"""FFMPEG Mix Audio action - combine audio tracks."""
import json
import os
import tempfile

import feaas.objects as objs
from src.actions.vendor.ffmpeg.base import FFMPEGAction


class MixAudio(FFMPEGAction):
    """Mix an audio track into a video or audio file."""

    def __init__(self, dao):
        params = [
            objs.Parameter(var_name='file', label='Main File', ptype=objs.ParameterType.STRING),
            objs.Parameter(var_name='audio_track', label='Audio Track to Mix', ptype=objs.ParameterType.STRING),
            objs.Parameter(var_name='main_volume', label='Main Volume', ptype=objs.ParameterType.FLOAT),
            objs.Parameter(var_name='mix_volume', label='Mix Volume', ptype=objs.ParameterType.FLOAT),
            objs.Parameter(var_name='loop_audio', label='Loop Audio Track', ptype=objs.ParameterType.BOOLEAN),
            objs.Parameter(var_name='mix_mode', label='Mix Mode', ptype=objs.ParameterType.STRING),
        ]
        outputs = [
            objs.Parameter(var_name='file', label='Mixed File', ptype=objs.ParameterType.STRING),
        ]
        super().__init__(dao, params, outputs)

    def execute_action(self, file, audio_track, main_volume=1.0, mix_volume=0.3,
                       loop_audio=True, mix_mode='mix') -> objs.Receipt:
        local_main = None
        local_audio = None
        local_output = None

        try:
            local_main = self.download_file(file)
            local_audio = self.download_file(audio_track)

            ext = os.path.splitext(file)[1] or ".mp4"
            fd, local_output = tempfile.mkstemp(suffix=ext)
            os.close(fd)

            probe_result = self.run_ffprobe(["-v", "quiet", "-print_format", "json",
                                              "-show_format", "-show_streams", local_main])
            main_data = json.loads(probe_result.stdout)
            main_duration = float(main_data["format"].get("duration", 0))
            has_video = any(s.get("codec_type") == "video" for s in main_data.get("streams", []))

            input_args = ["-i", local_main]
            if loop_audio:
                input_args += ["-stream_loop", "-1", "-i", local_audio]
            else:
                input_args += ["-i", local_audio]

            if mix_mode == "replace":
                filter_complex = f"[1:a]volume={mix_volume},atrim=0:{main_duration}[outa]"
                map_args = ["-map", "0:v", "-map", "[outa]"] if has_video else ["-map", "[outa]"]
            elif mix_mode == "ducking":
                filter_complex = (
                    f"[0:a]volume={main_volume}[main];"
                    f"[1:a]volume={mix_volume},atrim=0:{main_duration}[bg];"
                    f"[bg][main]sidechaincompress=threshold=0.02:ratio=6:attack=200:release=1000[ducked];"
                    f"[main][ducked]amix=inputs=2:duration=first[outa]"
                )
                map_args = ["-map", "0:v", "-map", "[outa]"] if has_video else ["-map", "[outa]"]
            else:  # mix
                filter_complex = (
                    f"[0:a]volume={main_volume}[a0];"
                    f"[1:a]volume={mix_volume},atrim=0:{main_duration}[a1];"
                    f"[a0][a1]amix=inputs=2:duration=first:dropout_transition=2[outa]"
                )
                map_args = ["-map", "0:v", "-map", "[outa]"] if has_video else ["-map", "[outa]"]

            args = input_args + ["-filter_complex", filter_complex] + map_args
            if has_video:
                args += ["-c:v", "copy"]
            args += ["-c:a", "aac", "-b:a", "192k", "-t", str(main_duration), local_output]

            self.run_ffmpeg(args)

            output_key = self.get_output_key(file, "mixed")
            self.upload_file(local_output, output_key)

            return objs.Receipt(
                success=True, primary_output='file',
                outputs={'file': objs.AnyType(ptype=objs.ParameterType.STRING, sval=output_key)}
            )

        except Exception as e:
            return objs.Receipt(success=False, error_message=str(e))
        finally:
            self.cleanup(local_main, local_audio, local_output)
