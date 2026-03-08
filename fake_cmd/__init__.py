"""
Some naming conventions:

- ``xxx_fp``: The real file path (not equal to the absolute path)
that can be used to directly access the file.
- ``xxx_fname``: The file name (or folder name).
- ``xxx_namespace``: A directory (or a directory name) that contains files.
- ``address``: Same to ``xxx_namespace``, but it is specially used
for the main server namespace.
- ``terminate``: Denote to command-level terminate.
- ``destroy``: Denote to session-level destroy.
- About server and client naming: ``server`` means message sent to
server, so does ``client``.
"""

__version__ = "0.0.5"

# NOTE: Import ``readline`` here to improve input experience.
readline_loaded: bool = False
try:
    import readline
except Exception:
    pass
else:
    readline_loaded = True

if not readline_loaded:
    # NOTE: Add a ``NOTHING`` adapter here for possible ``readline``
    # API usage in future versions, and if ``readline`` is not installed,
    # this allows the API calls to do nothing without exception raised.
    from slime_core.utils.typing import NOTHING as readline
