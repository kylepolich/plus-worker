"""FFMPEG MergeAudioVideo / MergeAudio / MergeAudioFromFolder actions."""
import os
import tempfile

import feaas.objects as objs
from src.actions.vendor.ffmpeg.base import FFMPEGAction


AUDIO_EXTS = ['.aif', '.cda', '.mid', '.mp3', '.mpa', '.ogg', '.wav', '.wma', '.wpl']
VIDEO_EXTS = ['.3gp', '.avi', '.flv', '.h264', '.m4v', '.mkv', '.mov', '.mp4', '.mpg', '.mpeg', '.rm', '.swf', '.vob', '.webm', '.wmv']


class MergeAudioVideo(FFMPEGAction):
    """Mux an audio track into a video file (replaces the video's audio)."""

    def __init__(self, dao):
        params = [
            objs.Parameter(
                var_name='audio_key',
                label='Audio File',
                ptype=objs.ParameterType.KEY,
                validations=[objs.Validation(vtype=objs.ValidationType.ENDS_WITH, svals=AUDIO_EXTS)]),
            objs.Parameter(
                var_name='video_key',
                label='Video File',
                ptype=objs.ParameterType.KEY,
                validations=[objs.Validation(vtype=objs.ValidationType.ENDS_WITH, svals=VIDEO_EXTS)]),
        ]
        outputs = [
            objs.Parameter(var_name='dest_key', label='Output', ptype=objs.ParameterType.KEY),
        ]
        super().__init__(dao, params, outputs)

    def execute_action(self, audio_key, video_key) -> objs.Receipt:
        if not self.blobstore.exists(audio_key):
            return objs.Receipt(success=False, error_message=f'Audio file not found: {audio_key}')
        if not self.blobstore.exists(video_key):
            return objs.Receipt(success=False, error_message=f'Video file not found: {video_key}')

        local_audio = None
        local_video = None
        local_output = None
        try:
            local_audio = self.download_file(audio_key)
            local_video = self.download_file(video_key)
            local_output = self.download_file(video_key) + '_merged.mp4'

            args = [
                '-i', local_video,
                '-i', local_audio,
                '-map', '0:v',
                '-map', '1:a',
                '-c:v', 'copy',
                local_output,
            ]
            self.run_ffmpeg(args)

            dest_key = self.get_output_key(video_key, 'merged', 'mp4')
            self.upload_file(local_output, dest_key)
            return objs.Receipt(
                success=True,
                primary_output='dest_key',
                outputs={'dest_key': objs.AnyType(ptype=objs.ParameterType.KEY, sval=dest_key)},
            )
        except Exception as e:
            return objs.Receipt(success=False, error_message=str(e))
        finally:
            self.cleanup(local_audio, local_video, local_output)


class MergeAudio(FFMPEGAction):
    """Mix two audio files into one using ffmpeg amix."""

    def __init__(self, dao):
        v1 = objs.Validation(vtype=objs.ValidationType.ENDS_WITH, svals=AUDIO_EXTS)
        params = [
            objs.Parameter(var_name='audio_key1', label='Audio File 1', ptype=objs.ParameterType.KEY, validations=[v1]),
            objs.Parameter(var_name='audio_key2', label='Audio File 2', ptype=objs.ParameterType.KEY, validations=[v1]),
        ]
        outputs = [
            objs.Parameter(var_name='dest_key', label='Output', ptype=objs.ParameterType.KEY),
        ]
        super().__init__(dao, params, outputs)

    def execute_action(self, audio_key1, audio_key2) -> objs.Receipt:
        if not self.blobstore.exists(audio_key1):
            return objs.Receipt(success=False, error_message=f'Source file not found: {audio_key1}')
        if not self.blobstore.exists(audio_key2):
            return objs.Receipt(success=False, error_message=f'Source file not found: {audio_key2}')

        local_a = None
        local_b = None
        local_output = None
        try:
            local_a = self.download_file(audio_key1)
            local_b = self.download_file(audio_key2)
            fd, local_output = tempfile.mkstemp(suffix='.mp3')
            os.close(fd)

            args = [
                '-i', local_a, '-i', local_b,
                '-filter_complex', '[0:a][1:a]amix=inputs=2:duration=longest[out]',
                '-map', '[out]',
                local_output,
            ]
            self.run_ffmpeg(args)

            dest_key = self.get_output_key(audio_key1, 'merged', 'mp3')
            self.upload_file(local_output, dest_key)
            return objs.Receipt(
                success=True,
                primary_output='dest_key',
                outputs={'dest_key': objs.AnyType(ptype=objs.ParameterType.KEY, sval=dest_key)},
            )
        except Exception as e:
            return objs.Receipt(success=False, error_message=str(e))
        finally:
            self.cleanup(local_a, local_b, local_output)


class MergeAudioFromFolder(FFMPEGAction):
    """Mix all audio files under a folder prefix into one output file."""

    def __init__(self, dao):
        params = [
            objs.Parameter(var_name='src_prefix', label='Folder', ptype=objs.ParameterType.PREFIX),
        ]
        outputs = [
            objs.Parameter(var_name='dest_key', label='Output', ptype=objs.ParameterType.KEY),
        ]
        super().__init__(dao, params, outputs)

    def execute_action(self, src_prefix) -> objs.Receipt:
        if not src_prefix.endswith('/'):
            src_prefix += '/'
        files = self.blobstore.ls(src_prefix, '')
        audio_files = [f for f in files if os.path.splitext(f)[1].lower() in AUDIO_EXTS]
        if not audio_files:
            return objs.Receipt(success=False, error_message='No audio files found to merge.')

        local_inputs = []
        local_output = None
        try:
            for k in audio_files:
                local_inputs.append(self.download_file(k))
            fd, local_output = tempfile.mkstemp(suffix='.mp3')
            os.close(fd)

            input_args = []
            for p in local_inputs:
                input_args += ['-i', p]
            labels = ''.join([f'[{i}:a]' for i in range(len(local_inputs))])
            filter_complex = f'{labels}amix=inputs={len(local_inputs)}:duration=longest[out]'

            args = input_args + ['-filter_complex', filter_complex, '-map', '[out]', local_output]
            self.run_ffmpeg(args)

            dest_key = f'{src_prefix}merged_output.mp3'
            self.upload_file(local_output, dest_key)
            return objs.Receipt(
                success=True,
                primary_output='dest_key',
                outputs={'dest_key': objs.AnyType(ptype=objs.ParameterType.KEY, sval=dest_key)},
            )
        except Exception as e:
            return objs.Receipt(success=False, error_message=str(e))
        finally:
            self.cleanup(local_output, *local_inputs)
