import sqlite3
from argparse import Namespace
from pathlib import Path

from plex_db_tool.commands.recover_database import run


def _write_test_db(tmp_path: Path, include_added_at: bool = True, flat_episode_shape: bool = False) -> Path:
    db_path = tmp_path / "Plex Media Server" / "Plug-in Support" / "Databases" / "com.plexapp.plugins.library.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)

    episode_file_name = "Show.S01E01.mkv" if flat_episode_shape else "Episode 01.mkv"
    episode_title = "Show.S01E01" if flat_episode_shape else "Episode 1"
    episode_parent_id = None if flat_episode_shape else 101

    episode_file = tmp_path / "media" / "Show" / "Season 01" / episode_file_name
    episode_file.parent.mkdir(parents=True, exist_ok=True)
    episode_file.write_bytes(b"test")

    added_at_column = ", added_at INTEGER" if include_added_at else ""
    added_at_value = ", 0" if include_added_at else ""

    with sqlite3.connect(db_path) as connection:
        connection.executescript(
            f"""
            CREATE TABLE metadata_items (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER,
                metadata_type INTEGER,
                title TEXT,
                created_at INTEGER,
                updated_at INTEGER
                {added_at_column}
            );
            CREATE TABLE media_items (
                id INTEGER PRIMARY KEY,
                metadata_item_id INTEGER
            );
            CREATE TABLE media_parts (
                id INTEGER PRIMARY KEY,
                media_item_id INTEGER,
                file TEXT
            );
            """
        )
        if not flat_episode_shape:
            connection.execute(
                "INSERT INTO metadata_items (id, parent_id, metadata_type, title, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                (100, None, 2, "Example Show", 0, 0),
            )
            connection.execute(
                "INSERT INTO metadata_items (id, parent_id, metadata_type, title, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                (101, 100, 3, "Season 1", 0, 0),
            )
        if include_added_at:
            connection.execute(
                "INSERT INTO metadata_items (id, parent_id, metadata_type, title, created_at, updated_at, added_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (1, episode_parent_id, 1, episode_title, 0, 0, 0),
            )
        else:
            connection.execute(
                "INSERT INTO metadata_items (id, parent_id, metadata_type, title, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                (1, episode_parent_id, 1, episode_title, 0, 0),
            )
        connection.execute("INSERT INTO media_items (id, metadata_item_id) VALUES (?, ?)", (10, 1))
        connection.execute(
            "INSERT INTO media_parts (id, media_item_id, file) VALUES (?, ?, ?)",
            (20, 10, str(episode_file)),
        )

    return db_path


def test_episode_date_added_audit_reports_mismatch_and_write_feasibility(tmp_path: Path, capsys) -> None:
    db_path = _write_test_db(tmp_path, include_added_at=True)

    result = run(
        Namespace(
            path=str(db_path),
            output=None,
            in_place=False,
            overwrite=False,
            verbose=False,
            check_episode_date_added=True,
            episode_date_added_max_drift_hours=24.0,
        )
    )

    captured = capsys.readouterr()
    assert result == 0
    assert "Date Added audit" in captured.out
    assert "Schema status: metadata_items/media_items/media_parts support the audit query." in captured.out
    assert "Update feasibility: metadata_items.added_at looks directly writable on this DB shape." in captured.out
    assert "Audit summary: 0 within tolerance, 1 outside tolerance or incomplete." in captured.out
    assert "Mismatch: id=1 type=1" in captured.out
    assert "Planned mutation: UPDATE metadata_items SET added_at=" in captured.out
    assert "WHERE id=1 current_added_at=1970-01-01" in captured.out


def test_episode_date_added_audit_detects_flat_episode_filename_shape(tmp_path: Path, capsys) -> None:
    db_path = _write_test_db(tmp_path, include_added_at=True, flat_episode_shape=True)

    result = run(
        Namespace(
            path=str(db_path),
            output=None,
            in_place=False,
            overwrite=False,
            verbose=False,
            check_episode_date_added=True,
            episode_date_added_max_drift_hours=24.0,
        )
    )

    captured = capsys.readouterr()
    assert result == 0
    assert "Date Added scan: 1 file-backed metadata item(s) with file rows found." in captured.out
    assert "Metadata shape: observed metadata types among file-backed rows: type 1=1" in captured.out
    assert "Mismatch: id=1 type=1" in captured.out
    assert "Planned mutation: UPDATE metadata_items SET added_at=" in captured.out


def test_apply_episode_date_added_fix_updates_flagged_rows(tmp_path: Path, capsys) -> None:
    db_path = _write_test_db(tmp_path, include_added_at=True, flat_episode_shape=True)
    episode_file = tmp_path / "media" / "Show" / "Season 01" / "Show.S01E01.mkv"
    stat_result = episode_file.stat()
    expected_candidates = {int(stat_result.st_ctime), int(stat_result.st_mtime)}

    result = run(
        Namespace(
            path=str(db_path),
            output=None,
            in_place=True,
            overwrite=False,
            verbose=False,
            check_episode_date_added=False,
            apply_episode_date_added_fix=True,
            episode_date_added_max_drift_hours=24.0,
        )
    )

    captured = capsys.readouterr()
    assert result == 0
    assert "Fix summary: updated 1 metadata_items.added_at row(s)." in captured.out

    with sqlite3.connect(db_path) as connection:
        added_at = connection.execute("SELECT added_at FROM metadata_items WHERE id = 1").fetchone()[0]
    assert int(added_at) in expected_candidates


def test_apply_episode_date_added_fix_requires_in_place_confirmation(tmp_path: Path, capsys) -> None:
    db_path = _write_test_db(tmp_path, include_added_at=True, flat_episode_shape=True)

    result = run(
        Namespace(
            path=str(db_path),
            output=None,
            in_place=False,
            overwrite=False,
            verbose=False,
            check_episode_date_added=False,
            apply_episode_date_added_fix=True,
            episode_date_added_max_drift_hours=24.0,
        )
    )

    captured = capsys.readouterr()
    assert result == 1
    assert "Pass --in-place to confirm." in captured.err


def test_episode_date_added_audit_rejects_incompatible_schema(tmp_path: Path, capsys) -> None:
    db_path = _write_test_db(tmp_path, include_added_at=False)

    result = run(
        Namespace(
            path=str(db_path),
            output=None,
            in_place=False,
            overwrite=False,
            verbose=False,
            check_episode_date_added=True,
            episode_date_added_max_drift_hours=24.0,
        )
    )

    captured = capsys.readouterr()
    assert result == 1
    assert "Schema status: incompatible for episode Date Added audit." in captured.err
    assert "metadata_items: missing columns added_at" in captured.err