"""FFMPEG AdjustVolume action - apply a flat gain (in dB) to an audio/video track."""
import feaas.objects as objs
from src.actions.vendor.ffmpeg.base import FFMPEGAction


class AdjustVolume(FFMPEGAction):
    """Apply a gain (in dB) to the audio of a media file."""

    def __init__(self, dao):
        params = [
            objs.Parameter(var_name='file', label='Media File', ptype=objs.ParameterType.STRING),
            objs.Parameter(var_name='gain_db', label='Gain (dB)', ptype=objs.ParameterType.FLOAT),
        ]
        outputs = [
            objs.Parameter(var_name='file', label='Adjusted File', ptype=objs.ParameterType.STRING),
        ]
        super().__init__(dao, params, outputs)

    def execute_action(self, file, gain_db) -> objs.Receipt:
        local_input = None
        local_output = None
        try:
            local_input = self.download_file(file)

            import os
            import tempfile
            ext = os.path.splitext(file)[1] or '.mp3'
            fd, local_output = tempfile.mkstemp(suffix=ext)
            os.close(fd)

            try:
                gain = float(gain_db)
            except (TypeError, ValueError):
                return objs.Receipt(success=False, error_message=f"gain_db must be numeric, got: {gain_db!r}")

            args = [
                "-i", local_input,
                "-filter:a", f"volume={gain:.4f}dB",
                "-c:v", "copy",
                local_output,
            ]
            self.run_ffmpeg(args)

            output_key = self.get_output_key(file, f"vol{int(round(gain))}db")
            self.upload_file(local_output, output_key)

            return objs.Receipt(
                success=True, primary_output='file',
                outputs={'file': objs.AnyType(ptype=objs.ParameterType.STRING, sval=output_key)},
            )
        except Exception as e:
            return objs.Receipt(success=False, error_message=str(e))
        finally:
            self.cleanup(local_input, local_output)
