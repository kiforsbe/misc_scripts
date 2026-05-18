from . import (
    list_accounts,
    list_libraries,
    list_playlists,
    remove_playlists,
    sync_metadata_playlists,
    transfer_playlists,
    transfer_watch_status,
)

COMMAND_MODULES = (
    transfer_watch_status,
    transfer_playlists,
    sync_metadata_playlists,
    list_playlists,
    remove_playlists,
    list_libraries,
    list_accounts,
)

__all__ = ["COMMAND_MODULES"]