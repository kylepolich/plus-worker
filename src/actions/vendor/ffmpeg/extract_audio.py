"""FFMPEG Extract Audio action - extract audio track from video."""
import feaas.objects as objs
from src.actions.vendor.ffmpeg.base import FFMPEGAction


class ExtractAudio(FFMPEGAction):
    """Extract audio track from a video file."""

    def __init__(self, dao):
        params = [
            objs.Parameter(var_name='video_file', label='Video File', ptype=objs.ParameterType.STRING),
            objs.Parameter(var_name='format', label='Audio Format', ptype=objs.ParameterType.STRING),
        ]
        outputs = [
            objs.Parameter(var_name='audio_file', label='Audio File', ptype=objs.ParameterType.STRING),
        ]
        super().__init__(dao, params, outputs)

    def execute_action(self, video_file, format='mp3') -> objs.Receipt:
        audio_format = format.lower()
        local_input = None
        local_output = None

        try:
            local_input = self.download_file(video_file)

            import tempfile
            import os
            fd, local_output = tempfile.mkstemp(suffix=f".{audio_format}")
            os.close(fd)

            args = ["-i", local_input, "-vn"]

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
            self.run_ffmpeg(args)

            output_key = self.get_output_key(video_file, "audio", audio_format)
            self.upload_file(local_output, output_key)

            return objs.Receipt(
                success=True, primary_output='audio_file',
                outputs={'audio_file': objs.AnyType(ptype=objs.ParameterType.STRING, sval=output_key)}
            )

        except Exception as e:
            return objs.Receipt(success=False, error_message=str(e))
        finally:
            self.cleanup(local_input, local_output)
