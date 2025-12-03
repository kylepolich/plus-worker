"""Base class for FFMPEG actions."""
import json
import os
import subprocess
import tempfile
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class FFMPEGAction(ABC):
    """Base class for all FFMPEG actions."""

    # Subclasses should override these
    action_id: str = ""
    label: str = ""
    short_desc: str = ""
    icon: str = "video"

    @property
    @abstractmethod
    def params(self) -> List[Dict[str, Any]]:
        """Define input parameters."""
        pass

    @property
    @abstractmethod
    def outputs(self) -> List[Dict[str, Any]]:
        """Define output parameters."""
        pass

    @abstractmethod
    def execute(self, dao, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Execute the action. Returns output dict."""
        pass

    def run_ffmpeg(self, args: List[str], check: bool = True) -> subprocess.CompletedProcess:
        """Run ffmpeg with given arguments."""
        cmd = ["ffmpeg", "-y"] + args  # -y to overwrite without asking
        return subprocess.run(cmd, capture_output=True, text=True, check=check)

    def run_ffprobe(self, args: List[str]) -> subprocess.CompletedProcess:
        """Run ffprobe with given arguments."""
        cmd = ["ffprobe"] + args
        return subprocess.run(cmd, capture_output=True, text=True, check=True)

    def download_file(self, dao, file_key: str) -> str:
        """Download file from blobstore to temp location. Returns local path."""
        blobstore = dao.get_blobstore()

        # Get file extension from key
        ext = os.path.splitext(file_key)[1] or ".tmp"

        # Create temp file
        fd, local_path = tempfile.mkstemp(suffix=ext)
        os.close(fd)

        # Download
        blobstore.download_file(file_key, local_path)
        return local_path

    def upload_file(self, dao, local_path: str, dest_key: str) -> str:
        """Upload file to blobstore. Returns the key."""
        blobstore = dao.get_blobstore()
        blobstore.upload_file(local_path, dest_key)
        return dest_key

    def get_output_key(self, input_key: str, suffix: str, new_ext: Optional[str] = None) -> str:
        """Generate output key based on input key."""
        base, ext = os.path.splitext(input_key)
        if new_ext:
            ext = new_ext if new_ext.startswith('.') else f'.{new_ext}'
        return f"{base}_{suffix}{ext}"

    def cleanup(self, *paths):
        """Remove temporary files."""
        for path in paths:
            if path and os.path.exists(path):
                try:
                    os.remove(path)
                except OSError:
                    pass
