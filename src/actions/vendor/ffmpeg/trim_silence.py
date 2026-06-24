"""FFMPEG TrimSilence action - strip leading and/or trailing silence."""
import feaas.objects as objs
from src.actions.vendor.ffmpeg.base import FFMPEGAction


class TrimSilence(FFMPEGAction):
    """Remove silent regions from the start and/or end of an audio file.

    Uses ffmpeg's silenceremove filter. Silence is anything below threshold_db
    for at least min_silence_sec. To strip end silence we reverse, strip start,
    reverse again (standard ffmpeg pattern).
    """

    def __init__(self, dao):
        params = [
            objs.Parameter(var_name='file', label='Media File', ptype=objs.ParameterType.STRING),
            objs.Parameter(var_name='threshold_db', label='Silence Threshold (dB)', ptype=objs.ParameterType.FLOAT),
            objs.Parameter(var_name='min_silence_sec', label='Min Silence (s)', ptype=objs.ParameterType.FLOAT),
            objs.Parameter(var_name='from_start', label='Trim From Start', ptype=objs.ParameterType.BOOLEAN),
            objs.Parameter(var_name='from_end', label='Trim From End', ptype=objs.ParameterType.BOOLEAN),
        ]
        outputs = [
            objs.Parameter(var_name='file', label='Trimmed File', ptype=objs.ParameterType.STRING),
        ]
        super().__init__(dao, params, outputs)

    def execute_action(self, file, threshold_db=-50.0, min_silence_sec=0.5,
                       from_start=True, from_end=True) -> objs.Receipt:
        local_input = None
        local_output = None
        try:
            import os
            import tempfile

            try:
                thr = float(threshold_db)
                dur = float(min_silence_sec)
            except (TypeError, ValueError):
                return objs.Receipt(success=False, error_message="threshold_db and min_silence_sec must be numeric")

            if not from_start and not from_end:
                return objs.Receipt(success=False, error_message="At least one of from_start/from_end must be true")

            local_input = self.download_file(file)
            ext = os.path.splitext(file)[1] or '.mp3'
            fd, local_output = tempfile.mkstemp(suffix=ext)
            os.close(fd)

            # silenceremove syntax: detect <stop_periods> periods of <stop_duration> below <stop_threshold>.
            filters = []
            if from_start:
                filters.append(
                    f"silenceremove=start_periods=1:start_duration={dur:.3f}:start_threshold={thr:.1f}dB"
                )
            if from_end:
                # Reverse → strip leading (was trailing) → reverse back.
                filters.append("areverse")
                filters.append(
                    f"silenceremove=start_periods=1:start_duration={dur:.3f}:start_threshold={thr:.1f}dB"
                )
                filters.append("areverse")

            args = ["-i", local_input, "-af", ",".join(filters), local_output]
            self.run_ffmpeg(args)

            output_key = self.get_output_key(file, "desilenced")
            self.upload_file(local_output, output_key)

            return objs.Receipt(
                success=True, primary_output='file',
                outputs={'file': objs.AnyType(ptype=objs.ParameterType.STRING, sval=output_key)},
            )
        except Exception as e:
            return objs.Receipt(success=False, error_message=str(e))
        finally:
            self.cleanup(local_input, local_output)
