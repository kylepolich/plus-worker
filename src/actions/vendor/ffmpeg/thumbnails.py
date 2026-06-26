"""FFMPEG Thumbnails action - extract frames at N per second and upload them."""
import os
import tempfile

import feaas.objects as objs
from src.actions.vendor.ffmpeg.base import FFMPEGAction


class Thumbnails(FFMPEGAction):
    """Extract thumbnails from a video at the requested fps, upload all to dest folder."""

    def __init__(self, dao):
        params = [
            objs.Parameter(var_name='username', label='Username', ptype=objs.ParameterType.USERNAME),
            objs.Parameter(var_name='src_key', label='Video File', ptype=objs.ParameterType.KEY),
            objs.Parameter(var_name='thumbnail_prefix', label='Save in Folder', ptype=objs.ParameterType.PREFIX),
            objs.Parameter(
                var_name='thumbnail_ext', label='Save As Type',
                ptype=objs.ParameterType.FIXED_LIST_SINGLE_SELECT,
                svals=[
                    objs.LabelledParam(label='PNG', value='.png'),
                    objs.LabelledParam(label='JPEG', value='.jpg'),
                ]),
            objs.Parameter(var_name='thumbnails_per_second', label='Thumbnails per Second',
                           idefault=1, ptype=objs.ParameterType.INTEGER),
        ]
        outputs = [
            objs.Parameter(var_name='thumbnails_created', label='# Created', ptype=objs.ParameterType.INTEGER),
            objs.Parameter(var_name='dest_prefix', label='Destination', ptype=objs.ParameterType.PREFIX),
        ]
        super().__init__(dao, params, outputs)

    def execute_action(self, username, src_key, thumbnail_prefix, thumbnail_ext, thumbnails_per_second) -> objs.Receipt:
        if not self.blobstore.exists(src_key):
            return objs.Receipt(success=False, error_message=f'File not found: {src_key}')
        if not thumbnail_prefix.endswith('/'):
            thumbnail_prefix += '/'
        if not thumbnail_ext.startswith('.'):
            thumbnail_ext = '.' + thumbnail_ext

        local_video = None
        out_dir = None
        try:
            local_video = self.download_file(src_key)
            out_dir = tempfile.mkdtemp(prefix='thumbs_')
            pattern = os.path.join(out_dir, f'thumb_%05d{thumbnail_ext}')

            fps = max(thumbnails_per_second or 1, 1)
            args = ['-i', local_video, '-vf', f'fps={fps}', pattern]
            self.run_ffmpeg(args)

            created = 0
            for fname in sorted(os.listdir(out_dir)):
                local_path = os.path.join(out_dir, fname)
                if not os.path.isfile(local_path):
                    continue
                dest_key = thumbnail_prefix + fname
                self.upload_file(local_path, dest_key)
                created += 1

            outputs = {
                'thumbnails_created': objs.AnyType(ptype=objs.ParameterType.INTEGER, ival=created),
                'dest_prefix': objs.AnyType(ptype=objs.ParameterType.PREFIX, sval=thumbnail_prefix),
            }
            return objs.Receipt(success=True, outputs=outputs, primary_output='thumbnails_created')
        except Exception as e:
            return objs.Receipt(success=False, error_message=str(e))
        finally:
            self.cleanup(local_video)
            if out_dir and os.path.isdir(out_dir):
                for fname in os.listdir(out_dir):
                    try:
                        os.remove(os.path.join(out_dir, fname))
                    except OSError:
                        pass
                try:
                    os.rmdir(out_dir)
                except OSError:
                    pass
