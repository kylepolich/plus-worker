"""FFMPEG EditMedia action - cut a segment out of an audio file."""
import os
import tempfile
import uuid

import feaas.objects as objs
from src.actions.vendor.ffmpeg.base import FFMPEGAction


class EditMedia(FFMPEGAction):
    """Remove a segment (edit_remove_from .. edit_remove_until, in ms) from an audio file
    by extracting the parts before and after, then concatenating them."""

    def __init__(self, dao):
        vals = objs.Validation(vtype=objs.ValidationType.ENDS_WITH, svals=['.mp3', '.wav'])
        params = [
            objs.Parameter(var_name='hostname', label='Host name', ptype=objs.ParameterType.HOSTNAME),
            objs.Parameter(var_name='username', label='Username', ptype=objs.ParameterType.USERNAME),
            objs.Parameter(var_name='media_key', label='Media Key',
                           ptype=objs.ParameterType.KEY, validations=[vals]),
            objs.Parameter(var_name='edit_remove_from', label='Edit remove from (ms)',
                           ptype=objs.ParameterType.INTEGER),
            objs.Parameter(var_name='edit_remove_until', label='Edit remove until (ms)',
                           ptype=objs.ParameterType.INTEGER),
            objs.Parameter(var_name='dest_key', label='Destination Key',
                           ptype=objs.ParameterType.KEY, optional=True),
        ]
        outputs = [
            objs.Parameter(var_name='dest_key', label='Destination Key', ptype=objs.ParameterType.KEY),
        ]
        super().__init__(dao, params, outputs)

    def execute_action(self, hostname, username, media_key, edit_remove_from, edit_remove_until, dest_key) -> objs.Receipt:
        ext = os.path.splitext(media_key)[1].lstrip('.') or 'mp3'
        if not dest_key:
            dest_key = f"{hostname}/{username}/ffmpeg/{uuid.uuid4()}.{ext}"

        if not self.blobstore.exists(media_key):
            return objs.Receipt(success=False, error_message=f'No file named {media_key}')

        local_input = None
        local_part1 = None
        local_part2 = None
        concat_list = None
        local_output = None
        try:
            local_input = self.download_file(media_key)

            fd, local_part1 = tempfile.mkstemp(suffix=f'.{ext}')
            os.close(fd)
            fd, local_part2 = tempfile.mkstemp(suffix=f'.{ext}')
            os.close(fd)
            fd, local_output = tempfile.mkstemp(suffix=f'.{ext}')
            os.close(fd)

            t_from = self._ms_to_timecode(edit_remove_from)
            t_until = self._ms_to_timecode(edit_remove_until)

            self.run_ffmpeg(['-i', local_input, '-ss', '00:00:00.000', '-to', t_from, '-c', 'copy', local_part1])
            self.run_ffmpeg(['-i', local_input, '-ss', t_until, '-c', 'copy', local_part2])

            fd, concat_list = tempfile.mkstemp(suffix='.txt')
            with os.fdopen(fd, 'w') as f:
                for p in (local_part1, local_part2):
                    esc = p.replace("'", "'\\''")
                    f.write(f"file '{esc}'\n")

            self.run_ffmpeg(['-f', 'concat', '-safe', '0', '-i', concat_list, '-c', 'copy', local_output])
            self.upload_file(local_output, dest_key)

            outputs = {'dest_key': objs.AnyType(ptype=objs.ParameterType.KEY, sval=dest_key)}
            return objs.Receipt(success=True, outputs=outputs, primary_output='dest_key')
        except Exception as e:
            return objs.Receipt(success=False, error_message=str(e))
        finally:
            self.cleanup(local_input, local_part1, local_part2, concat_list, local_output)

    def _ms_to_timecode(self, milliseconds):
        seconds = (milliseconds or 0) / 1000.0
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        seconds = seconds % 60
        return f"{hours:02}:{minutes:02}:{seconds:06.3f}"
