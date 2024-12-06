import gzip
import os
from logging.handlers import RotatingFileHandler

class CompressedRotatingFileHandler(RotatingFileHandler):
    """
    A RotatingFileHandler that compresses old log files after rotation.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def rotate(self, source, dest):
        """
        Override the rotate method to compress the rotated log file.
        """
        super().rotate(source, dest)  # Perform the rotation
        if os.path.exists(dest):
            with open(dest, 'rb') as f_in:
                with gzip.open(f"{dest}.gz", 'wb') as f_out:
                    f_out.writelines(f_in)
            os.remove(dest)  # Remove the uncompressed rotated file
