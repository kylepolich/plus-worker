"""FFMPEG Waveform action - render a waveform image of an audio file for player UIs."""
import feaas.objects as objs
from src.actions.vendor.ffmpeg.base import FFMPEGAction


class Waveform(FFMPEGAction):
    """Render a waveform PNG of an audio (or media) file.

    Uses ffmpeg's `showwavespic` filter. Colors accept ffmpeg color syntax —
    hex like `#3da9fc` or names like `white`/`black` work. `bg_color` of empty
    string or `transparent` makes the background transparent (PNG only).
    """

    def __init__(self, dao):
        params = [
            objs.Parameter(var_name='file', label='Audio File', ptype=objs.ParameterType.STRING),
            objs.Parameter(var_name='width_px', label='Width (px)', ptype=objs.ParameterType.INTEGER),
            objs.Parameter(var_name='height_px', label='Height (px)', ptype=objs.ParameterType.INTEGER),
            objs.Parameter(var_name='color', label='Wave Color', ptype=objs.ParameterType.STRING),
            objs.Parameter(var_name='bg_color', label='Background Color', ptype=objs.ParameterType.STRING),
        ]
        outputs = [
            objs.Parameter(var_name='file', label='Waveform PNG', ptype=objs.ParameterType.STRING),
        ]
        super().__init__(dao, params, outputs)

    def execute_action(self, file, width_px=1200, height_px=200,
                       color='#3da9fc', bg_color='#ffffff') -> objs.Receipt:
        local_input = None
        local_output = None
        try:
            import os
            import tempfile

            try:
                w = max(16, int(width_px or 1200))
                h = max(16, int(height_px or 200))
            except (TypeError, ValueError):
                return objs.Receipt(success=False, error_message="width_px and height_px must be integers")

            fg = (color or '#3da9fc').strip()
            bg = (bg_color or '').strip()
            transparent = (not bg) or bg.lower() == 'transparent'

            local_input = self.download_file(file)
            fd, local_output = tempfile.mkstemp(suffix='.png')
            os.close(fd)

            wave_filter = (
                f"aformat=channel_layouts=mono,"
                f"showwavespic=s={w}x{h}:colors={fg}"
            )
            if transparent:
                filter_complex = wave_filter
            else:
                # Draw the wave over a solid background of bg_color.
                filter_complex = (
                    f"color=c={bg}:s={w}x{h}[bg];"
                    f"[0:a]{wave_filter}[wav];"
                    f"[bg][wav]overlay=format=auto"
                )

            args = [
                "-i", local_input,
                "-filter_complex", filter_complex,
                "-frames:v", "1",
                local_output,
            ]
            self.run_ffmpeg(args)

            output_key = self.get_output_key(file, f"waveform_{w}x{h}", new_ext='png')
            self.upload_file(local_output, output_key)

            return objs.Receipt(
                success=True, primary_output='file',
                outputs={'file': objs.AnyType(ptype=objs.ParameterType.STRING, sval=output_key)},
            )
        except Exception as e:
            return objs.Receipt(success=False, error_message=str(e))
        finally:
            self.cleanup(local_input, local_output)
