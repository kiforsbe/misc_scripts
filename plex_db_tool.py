"""Windows-friendly shim for launching the Plex DB tool from the repo root.

This file exists so the tool can be run directly as .\\plex_db_tool.py
without having to type ``python`` or ``py`` first in PowerShell/cmd on a
Windows setup where .py files are associated with Python.
"""

from plex_db_tool.main import main


if __name__ == "__main__":
    # Delegate all CLI parsing and command execution to the package entrypoint.
    raise SystemExit(main())