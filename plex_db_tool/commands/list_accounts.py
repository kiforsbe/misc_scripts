import sys
from argparse import ArgumentParser, Namespace, _SubParsersAction

from ..infrastructure import PlexDatabase, PlexDatabaseLocator
from ..models import TableColumnSpec
from ..reporting import PlexReportWriter


def register(subparsers: _SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "list-accounts",
        help="List Plex accounts in a Plex library database.",
    )
    parser.set_defaults(command="list-accounts", command_handler=run)
    parser.add_argument(
        "--path",
        required=True,
        help="Path to the Plex location or DB file to inspect.",
    )


def run(args: Namespace) -> int:
    database = PlexDatabase(PlexDatabaseLocator.resolve_db_path(args.path, "path"), readonly=True)
    report_writer = PlexReportWriter()
    try:
        rows = [
            {
                "id": item.id,
                "name": item.name,
                "default_audio_language": item.default_audio_language,
                "default_subtitle_language": item.default_subtitle_language,
                "auto_select_audio": item.auto_select_audio,
                "auto_select_subtitle": item.auto_select_subtitle,
            }
            for item in database.list_accounts()
        ]
        report_writer.write_table_rows(
            sys.stdout,
            rows,
            [
                TableColumnSpec("id"),
                TableColumnSpec("name"),
                TableColumnSpec("default_audio_language"),
                TableColumnSpec("default_subtitle_language"),
                TableColumnSpec("auto_select_audio"),
                TableColumnSpec("auto_select_subtitle"),
            ],
        )
        return 0
    finally:
        database.close()