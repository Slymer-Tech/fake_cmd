"""
File operations.
"""
import os
import shutil
from .exception import retry_deco


@retry_deco(suppress_exc=Exception)
def remove_file_with_retry(fp: str):
    try:
        os.remove(fp)
    except FileNotFoundError:
        pass


@retry_deco(suppress_exc=Exception)
def remove_dir_with_retry(namespace: str):
    try:
        shutil.rmtree(namespace)
    except FileNotFoundError:
        pass


# Kept for backward compatibility: ``detect_files()`` filters out
# stale ``.writing`` files that may linger from older versions.
SINGLE_WRITER_LOCK_FILE_EXTENSION = 'writing'
