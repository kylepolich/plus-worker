"""FFMPEG Convert action - convert media between formats."""
import feaas.objects as objs
from src.actions.vendor.ffmpeg.base import FFMPEGAction


class Convert(FFMPEGAction):
    """Convert media file to a different format."""

    def __init__(self, dao):
        params = [
            objs.Parameter(var_name='file', label='Input File', ptype=objs.ParameterType.STRING),
            objs.Parameter(var_name='format', label='Output Format', ptype=objs.ParameterType.STRING),
        ]
        outputs = [
            objs.Parameter(var_name='file', label='Converted File', ptype=objs.ParameterType.STRING),
        ]
        super().__init__(dao, params, outputs)

    def execute_action(self, file, format) -> objs.Receipt:
        target_format = format.lower()
        local_input = None
        local_output = None

        try:
            local_input = self.download_file(file)

            import tempfile
            import os
            fd, local_output = tempfile.mkstemp(suffix=f".{target_format}")
            os.close(fd)

            args = ["-i", local_input]

            if target_format == "mp4":
                args += ["-c:v", "libx264", "-c:a", "aac", "-movflags", "+faststart"]
            elif target_format == "webm":
                args += ["-c:v", "libvpx-vp9", "-c:a", "libopus"]
            elif target_format == "mp3":
                args += ["-vn", "-c:a", "libmp3lame", "-q:a", "2"]
            elif target_format == "wav":
                args += ["-vn", "-c:a", "pcm_s16le"]
            elif target_format == "flac":
                args += ["-vn", "-c:a", "flac"]
            elif target_format == "ogg":
                args += ["-vn", "-c:a", "libvorbis", "-q:a", "4"]

            args.append(local_output)
            self.run_ffmpeg(args)

            output_key = self.get_output_key(file, "converted", target_format)
            self.upload_file(local_output, output_key)

            return objs.Receipt(
                success=True, primary_output='file',
                outputs={'file': objs.AnyType(ptype=objs.ParameterType.STRING, sval=output_key)}
            )

        except Exception as e:
            return objs.Receipt(success=False, error_message=str(e))
        finally:
            self.cleanup(local_input, local_output)
