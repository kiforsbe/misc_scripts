from . import (
    list_accounts,
    list_libraries,
    list_playlists,
    recover_database,
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
    recover_database,
)

__all__ = ["COMMAND_MODULES"]