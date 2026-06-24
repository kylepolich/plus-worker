"""FFMPEG AddIntroOutro action - prepend/append fixed clips with optional crossfade."""
import feaas.objects as objs
from src.actions.vendor.ffmpeg.base import FFMPEGAction


class AddIntroOutro(FFMPEGAction):
    """Concatenate optional intro and outro clips around a main body clip.

    If crossfade_sec > 0, uses the acrossfade filter (audio-only). Otherwise
    uses the concat demuxer for fast stream-copy concatenation when codecs match.
    Inputs whose key is empty/None are skipped.
    """

    def __init__(self, dao):
        params = [
            objs.Parameter(var_name='body', label='Body File', ptype=objs.ParameterType.STRING),
            objs.Parameter(var_name='intro', label='Intro File', ptype=objs.ParameterType.STRING),
            objs.Parameter(var_name='outro', label='Outro File', ptype=objs.ParameterType.STRING),
            objs.Parameter(var_name='crossfade_sec', label='Crossfade (s)', ptype=objs.ParameterType.FLOAT),
        ]
        outputs = [
            objs.Parameter(var_name='file', label='Output File', ptype=objs.ParameterType.STRING),
        ]
        super().__init__(dao, params, outputs)

    def execute_action(self, body, intro=None, outro=None, crossfade_sec=0.0) -> objs.Receipt:
        local_paths = []
        local_output = None
        concat_list_path = None
        try:
            import os
            import tempfile

            if not body:
                return objs.Receipt(success=False, error_message="body is required")

            try:
                xfade = float(crossfade_sec or 0)
            except (TypeError, ValueError):
                xfade = 0.0

            ordered_keys = [k for k in [intro, body, outro] if k]
            ordered_local = [self.download_file(k) for k in ordered_keys]
            local_paths.extend(ordered_local)

            ext = os.path.splitext(body)[1] or '.mp3'
            fd, local_output = tempfile.mkstemp(suffix=ext)
            os.close(fd)

            if xfade <= 0 or len(ordered_local) < 2:
                # Stream-copy concat via demuxer; requires matching codecs.
                fd2, concat_list_path = tempfile.mkstemp(suffix='.txt')
                os.close(fd2)
                with open(concat_list_path, 'w') as fh:
                    for p in ordered_local:
                        fh.write(f"file '{p}'\n")
                args = ["-f", "concat", "-safe", "0", "-i", concat_list_path, "-c", "copy", local_output]
            else:
                # Audio crossfade chain: acrossfade between each adjacent pair.
                args = []
                for p in ordered_local:
                    args += ["-i", p]
                # Build filter graph chaining acrossfade across all inputs.
                # 0:a + 1:a -> [a01]; [a01] + 2:a -> [a02]; ...
                chain = []
                prev = "[0:a]"
                for i in range(1, len(ordered_local)):
                    out_label = f"[a0{i}]" if i < len(ordered_local) - 1 else "[aout]"
                    chain.append(f"{prev}[{i}:a]acrossfade=d={xfade:.3f}:c1=tri:c2=tri{out_label}")
                    prev = out_label
                args += ["-filter_complex", ";".join(chain), "-map", "[aout]", local_output]

            self.run_ffmpeg(args)

            output_key = self.get_output_key(body, "with_intro_outro")
            self.upload_file(local_output, output_key)

            return objs.Receipt(
                success=True, primary_output='file',
                outputs={'file': objs.AnyType(ptype=objs.ParameterType.STRING, sval=output_key)},
            )
        except Exception as e:
            return objs.Receipt(success=False, error_message=str(e))
        finally:
            self.cleanup(*local_paths, local_output, concat_list_path)
