"""Base class for FFMPEG actions - inherits from AbstractAction."""
import os
import subprocess
import tempfile

import feaas.objects as objs
from feaas.abstract import AbstractAction


class FFMPEGAction(AbstractAction):
    """Base class for all FFMPEG actions.

    Provides common helper methods for file operations and ffmpeg/ffprobe execution.
    Subclasses must call super().__init__(params, outputs) after setting up self.dao.
    """

    def __init__(self, dao, params, outputs):
        """Initialize with DAO and action parameters.

        Args:
            dao: DataAccessObject for storage operations
            params: List of objs.Parameter for inputs
            outputs: List of objs.Parameter for outputs
        """
        self.dao = dao
        self.blobstore = dao.get_blobstore()
        super().__init__(params, outputs)

    def download_file(self, file_key: str) -> str:
        """Download file from blobstore to temp location. Returns local path."""
        ext = os.path.splitext(file_key)[1] or ".tmp"
        fd, local_path = tempfile.mkstemp(suffix=ext)
        os.close(fd)
        self.blobstore.download_file(file_key, local_path)
        return local_path

    def upload_file(self, local_path: str, dest_key: str) -> str:
        """Upload file to blobstore. Returns the key."""
        self.blobstore.upload_file(local_path, dest_key)
        return dest_key

    def get_output_key(self, input_key: str, suffix: str, new_ext: str = None) -> str:
        """Generate output key based on input key."""
        base, ext = os.path.splitext(input_key)
        if new_ext:
            ext = new_ext if new_ext.startswith('.') else f'.{new_ext}'
        return f"{base}_{suffix}{ext}"

    def run_ffmpeg(self, args: list, check: bool = True) -> subprocess.CompletedProcess:
        """Run ffmpeg with given arguments."""
        cmd = ["ffmpeg", "-y"] + args  # -y to overwrite without asking
        return subprocess.run(cmd, capture_output=True, text=True, check=check)

    def run_ffprobe(self, args: list) -> subprocess.CompletedProcess:
        """Run ffprobe with given arguments."""
        cmd = ["ffprobe"] + args
        return subprocess.run(cmd, capture_output=True, text=True, check=True)

    def cleanup(self, *paths):
        """Remove temporary files."""
        for path in paths:
            if path and os.path.exists(path):
                try:
                    os.remove(path)
                except OSError:
                    pass
