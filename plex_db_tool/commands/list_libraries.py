import sys
from argparse import ArgumentParser, Namespace, _SubParsersAction

from ..infrastructure import PlexDatabase, PlexDatabaseLocator
from ..models import TableColumnSpec
from ..reporting import PlexReportWriter


def register(subparsers: _SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "list-libraries",
        help="List Plex library sections in a Plex library database.",
    )
    parser.set_defaults(command="list-libraries", command_handler=run)
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
                "section_type": item.section_type,
                "agent": item.agent,
                "scanner": item.scanner,
                "language": item.language,
                "public": item.public,
            }
            for item in database.list_library_sections()
        ]
        report_writer.write_table_rows(
            sys.stdout,
            rows,
            [
                TableColumnSpec("id"),
                TableColumnSpec("name"),
                TableColumnSpec("section_type"),
                TableColumnSpec("agent"),
                TableColumnSpec("scanner"),
                TableColumnSpec("language"),
                TableColumnSpec("public"),
            ],
        )
        return 0
    finally:
        database.close()