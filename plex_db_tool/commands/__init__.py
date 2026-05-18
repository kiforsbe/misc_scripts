from . import list_accounts, list_libraries, list_playlists, transfer_playlists, transfer_watch_status

COMMAND_MODULES = (
    transfer_watch_status,
    transfer_playlists,
    list_playlists,
    list_libraries,
    list_accounts,
)

__all__ = ["COMMAND_MODULES"]